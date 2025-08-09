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

# AI Pipe client
client = OpenAI(
    api_key="eyJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6IjIzZjIwMDE5MTVAZHMuc3R1ZHkuaWl0bS5hYy5pbiJ9.DqxIp0WMnSQCmn5L25fVVBpIFAUqocHHgwU8pwIZMh0",
    base_url="https://aipipe.org/openai/v1"
def read_questions(filepath):
    with open(filepath, "r") as f:
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
    return response.choices[0].message.content

def save_output_file(answer_text, fmt, work_dir):
    output_path = os.path.join(work_dir, f"answer.{fmt}")

    if fmt == "docx":
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
        c.drawString(100, 750, answer_text)
        c.save()

    elif fmt == "html":
        with open(output_path, "w") as f:
            f.write(f"<html><body><pre>{answer_text}</pre></body></html>")

    elif fmt == "xlsx":
        df = pd.DataFrame({"Analysis": [answer_text]})
        df.to_excel(output_path, index=False)

    else:
        raise ValueError("Unsupported format")

    return output_path
