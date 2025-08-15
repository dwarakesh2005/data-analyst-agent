import uvicorn
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import tempfile, os, shutil, json

from utils import (
    read_questions,
    scrape_website,
    extract_data_from_files,        # -> (combined_text, tables, table_registry)
    generate_answer_flexible,       # main brain
    save_output_file
)

app = FastAPI(title="Data Analyst Agent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def home():
    return {"message": "Data Analyst Agent is running. Use POST / to send files."}

@app.post("/")
async def analyze(
    files: List[UploadFile] = File(...),
    output_format: str = Form("auto"),        # "auto" | "txt" | "docx" | "pptx" | "pdf" | "html" | "xlsx" | "json"
    website_url: str = Form(None),
    duckdb_query: str = Form(None)
):
    work_dir = tempfile.mkdtemp()
    try:
        saved_paths = []
        for f in files:
            fp = os.path.join(work_dir, f.filename)
            with open(fp, "wb") as buf:
                shutil.copyfileobj(f.file, buf)
            saved_paths.append(fp)

        # Find and read questions.txt
        questions_file = next((p for p in saved_paths if p.lower().endswith("questions.txt")), None)
        if not questions_file:
            return {"error": "questions.txt file is required."}
        questions = read_questions(questions_file)

        # Optional website scrape
        scraped_text = scrape_website(website_url) if website_url else ""

        # Extract text + tables from uploads
        combined_text, tables, table_registry = extract_data_from_files(saved_paths)

        # Build combined context for LLM fallback
        context_text = (scraped_text + "\n" + combined_text).strip()

        # Generate flexible answer (numbers/plots direct; fallback LLM otherwise)
        answer_text = generate_answer_flexible(
            questions=questions,
            context_text=context_text,
            tables=tables,
            table_registry=table_registry,
            duckdb_query=duckdb_query
        )

        # If caller asked for JSON or we detect valid JSON, return JSON directly
        wants_json = output_format.lower() == "json" or output_format.lower() == "auto"
        parsed_json: Dict[str, Any] = None
        if wants_json:
            try:
                parsed_json = json.loads(answer_text)
            except Exception:
                parsed_json = None

        if parsed_json is not None:
            return parsed_json

        # Otherwise, save to a file in requested format
        output_path, encoded_content = save_output_file(
            answer_text=answer_text,
            fmt=output_format.lower(),
            work_dir=work_dir,
            duckdb_query=duckdb_query
        )
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
