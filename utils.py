import os
import tarfile
import pandas as pd
import requests
from bs4 import BeautifulSoup
import PyPDF2
from docx import Document
from pptx import Presentation
from pptx.util import Inches
from reportlab.pdfgen import canvas
from openai import OpenAI
import base64
import duckdb
import io

# Read AI Pipe token from environment variable
AIPIPE_TOKEN = os.getenv("AIPIPE_TOKEN")
if not AIPIPE_TOKEN:
    raise ValueError("Environment variable AIPIPE_TOKEN is not set")

# AI Pipe client
client = OpenAI(
    api_key=AIPIPE_TOKEN,
    base_url="https://aipipe.org/openai/v1"
)

def read_questions(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()

def scrape_website(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    return soup.get_text(separator="\n")

def extract_data_from_files(file_paths):
    text_data = ""
    for path in file_paths:
        if path.endswith(".csv"):
            df = pd.read_csv(path)
            text_data += df.to_string() + "\n"
        elif path.endswith(".pdf"):
            reader = PyPDF2.PdfReader(open(path, "rb"))
            for page in reader.pages:
                text_data += page.extract_text() + "\n"
        elif path.endswith(".tar"):
            with tarfile.open(path, "r") as tar:
                tar.extractall(path=os.path.dirname(path))
                for member in tar.getmembers():
                    extracted_path = os.path.join(os.path.dirname(path), member.name)
                    if extracted_path.endswith(".csv"):
                        df = pd.read_csv(extracted_path)
                        text_data += df.to_string() + "\n"
        elif path.endswith(".txt"):
            with open(path, "r", encoding="utf-8") as f:
                text_data += f.read() + "\n"
    return text_data

def generate_answer(questions, context):
    prompt = f"""
You are a data analyst. Answer the following questions based on the provided data.

Questions:
{questions}

Data:
{context}
"""
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content

def run_duckdb_query(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Run a DuckDB SQL query on the given DataFrame and return the result as a DataFrame.
    """
    con = duckdb.connect()
    con.register('data', df)
    result_df = con.execute(query).df()
    con.close()
    return result_df

def save_output_file(answer_text, fmt, work_dir, duckdb_query=None):
    """
    Save the answer_text or DuckDB query results in the specified format.
    Returns a tuple: (output_path, base64_encoded_content)
    """
    output_path = os.path.join(work_dir, f"answer.{fmt}")

    # Run DuckDB query if provided (assumes answer_text is CSV format string)
    if duckdb_query:
        try:
            df = pd.read_csv(io.StringIO(answer_text))
            df = run_duckdb_query(df, duckdb_query)
            answer_text = df.to_string(index=False)
        except Exception as e:
            answer_text = f"Error running DuckDB query: {str(e)}"

    if fmt == "txt":
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(answer_text)

    elif fmt == "docx":
        doc = Document()
        doc.add_paragraph(answer_text)
        doc.save(output_path)

    elif fmt == "pptx":
        prs = Presentation()
        slide = prs.slides.add_slide(prs.slide_layouts[1])
        title = slide.shapes.title
        title.text = "Analysis Results"
        slide.shapes.add_textbox(Inches(1), Inches(2), Inches(6), Inches(4)).text = answer_text
        prs.save(output_path)

    elif fmt == "pdf":
        c = canvas.Canvas(output_path)
        lines = answer_text.split('\n')
        y = 750
        for line in lines:
            c.drawString(50, y, line)
            y -= 15
            if y < 50:
                c.showPage()
                y = 750
        c.save()

    elif fmt == "html":
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"<html><body><pre>{answer_text}</pre></body></html>")

    elif fmt == "xlsx":
        df = pd.DataFrame({"Analysis": [answer_text]})
        df.to_excel(output_path, index=False)

    else:
        raise ValueError(f"Unsupported format: {fmt}")

    with open(output_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")

    return output_path, encoded
