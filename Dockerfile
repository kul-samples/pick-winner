# Use the official Python image from the Docker Hub
FROM docker.io/kulbhushanmayer/luckydraw:base 

# Copy the Flask application to the container
COPY pick_winner.py /app/app.py
