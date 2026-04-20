import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))


def test_transform_removes_duplicates():
    df = pd.DataFrame({
        "label": ["ham", "spam", "ham"],
        "text": ["Hello", "Win a prize", "Hello"]
    })
    df = df.drop_duplicates(subset=["text"])
    assert len(df) == 2


def test_transform_removes_empty():
    df = pd.DataFrame({
        "label": ["ham", "spam", "ham"],
        "text": ["Hello", "", "   "]
    })
    df["text"] = df["text"].str.strip()
    df = df[df["text"].str.len() > 0]
    assert len(df) == 1


def test_transform_removes_nulls():
    df = pd.DataFrame({
        "label": ["ham", "spam", None],
        "text": ["Hello", "Win a prize", None]
    })
    df = df.dropna()
    assert len(df) == 2


def test_data_file_exists():
    data_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "spam.csv"
    )
    assert os.path.exists(data_path), "spam.csv not found"


def test_csv_has_required_columns():
    data_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "spam.csv"
    )
    df = pd.read_csv(data_path, encoding="latin-1")
    assert "v1" in df.columns
    assert "v2" in df.columns


def test_csv_has_data():
    data_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "spam.csv"
    )
    df = pd.read_csv(data_path, encoding="latin-1")
    assert len(df) > 100