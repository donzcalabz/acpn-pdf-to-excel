import pdfplumber
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine


DATABASE_URL = "postgresql://postgres:password@localhost:5432/pdf-converter"
engine = create_engine(DATABASE_URL)

# Use only this as the fixed header
FINAL_HEADER = [
    "PABN No.", "Series No.", "Member PIN", "Patient Name", "Confinement Period",
    "Caserate1_Code", "Caserate1_Gross",
    "Caserate2_Code", "Caserate2_Gross",
    "Others_Code", "Others_Gross",
    "Total_Gross", "Total_WTax", "Total_HCI", "Total_PF"
]

def is_header_row(row):
    header_keywords = [
        "PABN No.", "Series No.", "Member PIN", "Patient Name", "Confinement Period"
    ]
    subheader_keywords = [
        "Code", "Gross", "WTax", "HCI", "PF"
    ]

    # Check for main headers in the first 6 columns
    if any(cell and any(k in cell for k in header_keywords) for cell in row[:6]):
        return True

    # Check for repeated subheaders like: Code, Gross, etc.
    if all(cell and any(k in cell for k in subheader_keywords) for cell in row if cell):
        return True

    return False


def process_pdf(file_bytes: bytes) -> BytesIO:
    from copy import deepcopy

    data_rows = []
    with pdfplumber.open(BytesIO(file_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            table = page.extract_table()
            if not table:
                text = page.extract_text()
                print(f"⚠️ No table found on page {i + 1}. Text content:\n{text}")
                continue

            rows = table[1:] if i == 0 else table

            j = 0
            while j < len(rows):
                base_row = rows[j]

                # 🚫 Skip header rows that are repeated
                if is_header_row(base_row):
                    j += 1
                    continue
                    
                if j + 1 < len(rows) and "Health Care Professional/s:" in (rows[j + 1][0] or ""):
                    physician_line = rows[j + 1][0]
                    # Extract names from string
                    raw_text = physician_line.split("Health Care Professional/s:")[-1].strip()

                    new_row = deepcopy(base_row)
                    new_row.insert(4, raw_text)  # Insert physician name after Patient Name
                    new_row.insert(6, '')  # Insert physician name after Patient Name
                    data_rows.append(new_row)

                    j += 2  # Skip both base row and physician row
                else:
                    # No physician row — still create the row with empty physician
                    new_row = deepcopy(base_row)
                    new_row.insert(4, "")  # Insert empty physician field
                    data_rows.append(new_row)
                    j += 1

    # Expected final header with inserted Physician
    FINAL_HEADER = [
        "PABN No.", "Series No.", "Member PIN", "Patient Name", "Health Care Professional",
        "Confinement Period","Folio",
        "Caserate1_Code", "Caserate1_Gross",
        "Caserate2_Code", "Caserate2_Gross",
        "Others_Code", "Others_Gross",
        "Total_Gross", "Total_WTax", "Total_HCI", "Total_PF"
    ]

    # Remove rows with incorrect column count
    clean_data = [r for r in data_rows if len(r) == len(FINAL_HEADER)]

    if not clean_data:
        raise ValueError("No valid rows extracted.")

    df = pd.DataFrame(clean_data, columns=FINAL_HEADER)
    df = df.fillna(method="ffill", axis=0)

    try:
        df.to_sql("pdf_data", engine, if_exists="append", index=False)
    except Exception as e:
        print("❌ Failed to insert into DB:", e)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    return output
