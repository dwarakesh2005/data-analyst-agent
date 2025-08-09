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
    result_df = con.ex_
