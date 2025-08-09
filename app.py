import uvicorn
from fastapi import FastAPI, File, UploadFile, Form
from typing import List
import tempfile
import os
import shutil
from utils import (
    read_questions,
    scrape_website,
    extract_data_from_files,
    generate_answer,
    save_output_file
)

app = FastAPI()

from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def home():
    return {"message": "Data Analyst Agent is running. Use POST / to send files."}

@app.post("/")
async def analyze(
    files: List[UploadFile] = File(...),
    output_format: str = Form(...),  # txt, docx, pptx, pdf, html, xlsx
    website_url: str = Form(None),    # Optional: scrape website text
    duckdb_query: str = Form(None)    # Optional: DuckDB SQL query on tabular data
):
    work_dir = tempfile.mkdtemp()

    try:
        saved_paths = []
        for file in files:
            file_path = os.path.join(work_dir, file.filename)
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            saved_paths.append(file_path)

        # Find and read questions.txt
        questions_file = next((p for p in saved_paths if p.endswith("questions.txt")), None)
        if not questions_file:
            return {"error": "questions.txt file is required."}
        questions = read_questions(questions_file)

        # Scrape website if URL provided
        scraped_text = ""
        if website_url:
            scraped_text = scrape_website(website_url)

        # Extract text data from uploaded files
        extracted_text = extract_data_from_files(saved_paths)

        # Combine all context data
        combined_context = scraped_text + "\n" + extracted_text

        # Generate AI answer
        answer_text = generate_answer(questions, combined_context)

        # Save output file and get base64 encoding
        output_path, encoded_content = save_output_file(answer_text, output_format, work_dir, duckdb_query)

        return {
            "status": "success",
            "output_file": os.path.basename(output_path),
            "output_format": output_format,
            "base64_content": encoded_content
        }

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
