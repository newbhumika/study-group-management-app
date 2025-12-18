from backend.app import app  # re-export the Flask app for Vercel

# Optional: if you still want to run locally with `python app.py`
if __name__ == "__main__":
    app.run(debug=True)
