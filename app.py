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

@app.post("/")
async def analyze(
    files: List[UploadFile] = File(...),
    output_format: str = Form(...),  # docx, pptx, pdf, html, xlsx
    website_url: str = Form(None)    # Optional: scrape website
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
            return {"error": "questions.txt is required"}
        questions = read_questions(questions_file)

        # Optional: scrape website
        scraped_text = ""
        if website_url:
            scraped_text = scrape_website(website_url)

        # Extract from uploaded files
        extracted_text = extract_data_from_files(saved_paths)

        # Combine all context
        combined_context = scraped_text + "\n" + extracted_text

        # Get AI-generated answers
        answer_text = generate_answer(questions, combined_context)

        # Save output in requested format
        output_file = save_output_file(answer_text, output_format, work_dir)

        return {
            "status": "success",
            "output_file": output_file
        }

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
