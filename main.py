from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse
from pdf_processor import process_pdf
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Update if your frontend runs elsewhere
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    # Read file content
    file_bytes = await file.read()

    # Process PDF → Excel in-memory BytesIO
    excel_bytes = process_pdf(file_bytes)

    # Return Excel file as downloadable response
    return StreamingResponse(
        excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=output.xlsx"}
    )
