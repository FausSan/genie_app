import streamlit as st
import requests
import time
import json
import os
from PIL import Image

# Azure App Service Configuration
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE", "adb-3343959754742015.15.azuredatabricks.net")
SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f0380239b4135882f1046c446923d3")
AUTH_TOKEN = os.getenv("DATABRICKS_TOKEN", "Bearer dapi5c18015fe23b6de06854ab505bb61755-3")

# Ensure token has Bearer prefix
if not AUTH_TOKEN.startswith("Bearer "):
    AUTH_TOKEN = f"Bearer {AUTH_TOKEN}"

HEADERS = {
    "Authorization": AUTH_TOKEN,
    "Content-Type": "application/json"
}

BASE_URL = f"https://{DATABRICKS_INSTANCE}/api/2.0/genie/spaces/{SPACE_ID}"

# Azure App Service health check endpoint
def health_check():
    """Health check for Azure App Service"""
    return {"status": "healthy", "timestamp": time.time()}

def start_conversation(question):
    """Start a new Genie conversation."""
    url = f"{BASE_URL}/start-conversation"
    payload = {"content": question}
    
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data["conversation"]["id"], data["message"]["id"]
        else:
            st.error(f"Failed to start conversation: {response.status_code}")
            if response.status_code == 401:
                st.error("Authentication failed. Please check your Databricks token.")
            elif response.status_code == 403:
                st.error("Access denied. Please check your permissions.")
            return None, None
    except requests.exceptions.Timeout:
        st.error("Request timed out. Please try again.")
        return None, None
    except requests.exceptions.ConnectionError:
        st.error("Connection error. Please check your network connection.")
        return None, None
    except requests.exceptions.RequestException as e:
        st.error(f"Network error: {str(e)}")
        return None, None

def poll_message(conversation_id, message_id):
    """Poll for the message's status and retrieve the response when completed."""
    url = f"{BASE_URL}/conversations/{conversation_id}/messages/{message_id}"
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        for attempt in range(60):  # 2 minutes timeout
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                data = response.json()
                status_text.text(f"Processing... Status: {data['status']} ({attempt + 1}/60)")
                progress_bar.progress((attempt + 1) / 60)
                
                if data["status"] == "COMPLETED":
                    progress_bar.empty()
                    status_text.empty()
                    return data
                elif data["status"] in ["FAILED", "CANCELLED"]:
                    progress_bar.empty()
                    status_text.empty()
                    st.error(f"Message processing failed: {data.get('error', 'Unknown error')}")
                    return None
            elif response.status_code == 401:
                progress_bar.empty()
                status_text.empty()
                st.error("Authentication expired. Please refresh the page.")
                return None
            else:
                st.warning(f"Polling request failed: {response.status_code}")
            
            time.sleep(2)  # Poll every 2 seconds
        
        progress_bar.empty()
        status_text.empty()
        st.error("Polling timed out after 2 minutes.")
        return None
    except requests.exceptions.RequestException as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"Network error during polling: {str(e)}")
        return None

def retrieve_query_results(conversation_id, message_id, attachment_id):
    """Retrieve the query results."""
    url = f"{BASE_URL}/conversations/{conversation_id}/messages/{message_id}/query-result/{attachment_id}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to retrieve query results: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Network error retrieving results: {str(e)}")
        return None

def format_number(scientific_notation_str):
    """Convert scientific notation string to formatted number."""
    try:
        # Convert scientific notation to float
        number = float(scientific_notation_str)
        
        # Format based on the size of the number
        if abs(number) >= 1_000_000:
            return f"{number:,.0f}"
        elif abs(number) >= 1000:
            return f"{number:,.2f}"
        else:
            return f"{number:.2f}"
    except (ValueError, TypeError):
        return str(scientific_notation_str)

def display_results(response_data, query_results):
    """Display the question and formatted results."""
    
    # Extract question
    question = response_data.get("content", "No question found")
    
    # Display question
    st.subheader("📊 Query")
    st.info(question)
    
    # Extract and display SQL query if available
    attachments = response_data.get("attachments", [])
    if attachments:
        for attachment in attachments:
            if "query" in attachment:
                query_info = attachment["query"]
                sql_query = query_info.get("query", "")
                description = query_info.get("description", "")
                
                st.subheader("🔍 Generated SQL Query")
                st.code(sql_query, language="sql")
                
                if description:
                    st.subheader("📝 Query Description")
                    st.write(description)
    
    # Extract and display results
    if query_results and "statement_response" in query_results:
        statement_response = query_results["statement_response"]
        
        if statement_response.get("status", {}).get("state") == "SUCCEEDED":
            result_data = statement_response.get("result", {})
            data_array = result_data.get("data_array", [])
            
            # Get column information
            columns = statement_response.get("manifest", {}).get("schema", {}).get("columns", [])
            
            st.subheader("✅ Results")
            
            if data_array and len(data_array) > 0:
                # Display results in metric format for single values
                if len(data_array) == 1 and len(data_array[0]) <= 2:
                    col1, col2 = st.columns(2)
                    
                    for j, value in enumerate(data_array[0]):
                        column_name = columns[j]["name"] if j < len(columns) else f"Column_{j+1}"
                        formatted_value = format_number(value)
                        
                        with col1 if j % 2 == 0 else col2:
                            st.metric(
                                label=column_name.replace("_", " ").title(),
                                value=formatted_value
                            )
                
                # Always display as a table for better readability
                st.subheader("📋 Detailed Results")
                
                # Create a formatted table
                table_data = []
                column_names = [col["name"] for col in columns] if columns else [f"Column_{i+1}" for i in range(len(data_array[0]))]
                
                for row in data_array:
                    formatted_row = [format_number(value) for value in row]
                    table_data.append(dict(zip(column_names, formatted_row)))
                
                st.table(table_data)
                
                # Download option
                if st.button("📥 Download Results as JSON"):
                    json_data = json.dumps(table_data, indent=2)
                    st.download_button(
                        label="Download JSON",
                        data=json_data,
                        file_name=f"genie_results_{int(time.time())}.json",
                        mime="application/json"
                    )
            else:
                st.warning("No data returned from the query.")
        else:
            st.error("Query execution failed.")
    else:
        st.error("No valid query results found.")

def ask_follow_up(conversation_id, follow_up_question):
    """Ask a follow-up question in the same conversation."""
    url = f"{BASE_URL}/conversations/{conversation_id}/messages"
    payload = {"content": follow_up_question}
    
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to ask follow-up: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Network error asking follow-up: {str(e)}")
        return None

def main():
    # Configure Streamlit page
    st.set_page_config(
        page_title="Databricks Genie Query Interface",
        page_icon="🧞‍♂️",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Custom CSS for better Azure App Service display
    st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stButton > button {
        width: 100%;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header with logo
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        # Try to load logo from multiple possible paths
        logo_paths = ["logo.jpg", "./logo.jpg", "/home/site/wwwroot/logo.jpg"]
        logo_loaded = False
        
        for logo_path in logo_paths:
            try:
                if os.path.exists(logo_path):
                    logo = Image.open(logo_path)
                    st.image(logo, width=150)
                    logo_loaded = True
                    break
            except Exception:
                continue
        
        if not logo_loaded:
            st.write("🏢")  # Fallback icon
    
    with col2:
        st.title("🧞‍♂️ Databricks Genie Query Interface")
        st.markdown("Ask questions about your data and get AI-powered SQL insights!")
    
    with col3:
        # Environment indicator
        st.success("☁️ Azure App Service")
    
    # Configuration check
    with st.expander("🔧 Configuration Status"):
        st.write(f"**Databricks Instance:** {DATABRICKS_INSTANCE}")
        st.write(f"**Space ID:** {SPACE_ID}")
        st.write(f"**Token Status:** {'✅ Configured' if AUTH_TOKEN else '❌ Missing'}")
        
        # Test connection button
        if st.button("🔍 Test Connection"):
            try:
                test_url = f"https://{DATABRICKS_INSTANCE}/api/2.0/genie/spaces/{SPACE_ID}"
                response = requests.get(test_url, headers=HEADERS, timeout=10)
                if response.status_code in [200, 404]:  # 404 is ok, means auth works
                    st.success("✅ Connection successful!")
                else:
                    st.error(f"❌ Connection failed: {response.status_code}")
            except Exception as e:
                st.error(f"❌ Connection error: {str(e)}")
    
    # Input for the question
    question = st.text_input(
        "Enter your question:",
        placeholder="e.g., How many sales did make the Corporate customer segment in 2016?",
        help="Ask questions about your data in natural language"
    )
    
    # Submit button
    if st.button("🚀 Ask Genie", type="primary"):
        if question.strip():
            with st.spinner("Starting conversation with Genie..."):
                conversation_id, message_id = start_conversation(question)
            
            if conversation_id and message_id:
                st.success(f"✅ Conversation started! ID: {conversation_id}")
                
                # Poll for response
                with st.spinner("Waiting for Genie to process your question..."):
                    response_data = poll_message(conversation_id, message_id)
                
                if response_data:
                    # Get query results if available
                    attachments = response_data.get("attachments", [])
                    query_results = None
                    
                    for attachment in attachments:
                        if "attachment_id" in attachment:
                            attachment_id = attachment["attachment_id"]
                            query_results = retrieve_query_results(conversation_id, message_id, attachment_id)
                            break
                    
                    # Create layout for results
                    if st.checkbox("🔄 Show side-by-side view", value=True):
                        # Side-by-side layout
                        col1, col2 = st.columns([1, 1])
                        
                        with col1:
                            st.subheader("📊 Genie Query Results")
                            display_results(response_data, query_results)
                        
                        with col2:
                            st.subheader("📈 Power BI Dashboard")
                            powerbi_html = """
                            <iframe 
                                title="PBI Poc" 
                                width="100%" 
                                height="600" 
                                src="https://app.powerbi.com/reportEmbed?reportId=b62cf0f7-5142-4ec7-a0f6-cc3d62a4828a&autoAuth=true&ctid=1a243c58-e262-4bf3-8a97-08e2b733f880" 
                                frameborder="0" 
                                allowFullScreen="true">
                            </iframe>
                            """
                            st.components.v1.html(powerbi_html, height=620)
                    else:
                        # Stacked layout
                        display_results(response_data, query_results)
                    
                    # Store conversation ID for follow-up questions
                    st.session_state.conversation_id = conversation_id
                    
                    # Show raw response in expander for debugging
                    with st.expander("🔧 Debug Information"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.subheader("Response Data")
                            st.json(response_data)
                        with col2:
                            if query_results:
                                st.subheader("Query Results")
                                st.json(query_results)
        else:
            st.warning("⚠️ Please enter a question!")
    
    # Power BI Dashboard section (always visible)
    st.markdown("---")
    st.subheader("📈 Power BI Dashboard - Overview")
    st.markdown("Explore your data visually with the interactive Power BI dashboard below:")
    
    # Always show Power BI dashboard
    powerbi_html = """
    <iframe 
        title="PBI Poc" 
        width="100%" 
        height="600" 
        src="https://app.powerbi.com/reportEmbed?reportId=b62cf0f7-5142-4ec7-a0f6-cc3d62a4828a&autoAuth=true&ctid=1a243c58-e262-4bf3-8a97-08e2b733f880" 
        frameborder="0" 
        allowFullScreen="true">
    </iframe>
    """
    st.components.v1.html(powerbi_html, height=620)
    
    # Follow-up questions section
    if hasattr(st.session_state, 'conversation_id'):
        st.markdown("---")
        st.subheader("💬 Follow-up Questions")
        
        follow_up = st.text_input(
            "Ask a follow-up question:",
            placeholder="e.g., Which customers contributed most to these sales?",
            key="follow_up"
        )
        
        col1, col2 = st.columns([3, 1])
        with col1:
            if st.button("📤 Ask Follow-up", type="secondary"):
                if follow_up.strip():
                    conversation_id = st.session_state.conversation_id
                    
                    with st.spinner("Processing follow-up question..."):
                        follow_up_response = ask_follow_up(conversation_id, follow_up)
                    
                    if follow_up_response:
                        message_id = follow_up_response.get("id")
                        if message_id:
                            # Poll for follow-up response
                            response_data = poll_message(conversation_id, message_id)
                            if response_data:
                                # Display follow-up results
                                st.subheader("📊 Follow-up Results")
                                
                                # Get query results for follow-up
                                attachments = response_data.get("attachments", [])
                                query_results = None
                                
                                for attachment in attachments:
                                    if "attachment_id" in attachment:
                                        attachment_id = attachment["attachment_id"]
                                        query_results = retrieve_query_results(conversation_id, message_id, attachment_id)
                                        break
                                
                                display_results(response_data, query_results)
                else:
                    st.warning("⚠️ Please enter a follow-up question!")
        
        with col2:
            if st.button("🔄 New Conversation"):
                if 'conversation_id' in st.session_state:
                    del st.session_state.conversation_id
                st.experimental_rerun()

    # Footer
    st.markdown("---")
    st.markdown("*Powered by Databricks Genie AI and Azure App Services*")

if __name__ == "__main__":
    main()