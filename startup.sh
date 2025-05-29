#!/bin/bash

echo "Checking for pip3..."
if ! command -v pip3 &> /dev/null; then
    echo "pip3 not found. Installing..."
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python3 get-pip.py
fi

export PATH=$PATH:/home/.local/bin

echo "Installing dependencies..."
pip3 install --upgrade pip
pip3 install -r requirements.txt

echo "Starting Streamlit application..."

export STREAMLIT_SERVER_PORT=8000
export STREAMLIT_SERVER_ADDRESS=0.0.0.0
export STREAMLIT_SERVER_HEADLESS=true
export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Start Streamlit in the background and keep printing output
python3 -m streamlit run app.py --server.port=8000 --server.address=0.0.0.0 --server.headless=true &

# While Streamlit is starting, print a dot every 10 seconds to avoid timeout
while true; do
    echo "Streamlit starting..."
    sleep 10
done
