import streamlit as st
import google.generativeai as genai
import os
import time
import requests
import pandas as pd
import tempfile
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Video Analysis",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for styling
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem;
            padding-bottom: 0rem;
        }
        .element-container {
            margin-bottom: 0.5rem;
        }
        .stTextInput > div > div > input {
            max-width: 600px;
        }
        iframe {
            border: none !important;
            background-color: #000000;
        }
        .stTable td {
            white-space: normal !important;
            padding: 0.5rem !important;
        }
        .stProgress > div > div > div {
            height: 5px;
        }
        .progress-bar-wrapper {
            margin-bottom: 1rem;
        }
        h1 {
            font-size: 1.5rem !important;
            margin-bottom: 1rem !important;
        }
        /* Hide index numbers in table */
        table {
            width: 100%;
        }
    </style>
""", unsafe_allow_html=True)

# Constants
API_KEY = os.getenv('GEMINI_API_KEY')
BUCKET_NAME = "linqia-social-post-data"
FOLDER_NAME = "tiktok_video_content_analysis"
S3_BASE_URL = f"https://{BUCKET_NAME}.s3.us-east-1.amazonaws.com/{FOLDER_NAME}"

class TikTokAnalyzer:
    def __init__(self):
        genai.configure(api_key=API_KEY)
        self.model = genai.GenerativeModel('gemini-1.5-flash')

    def extract_video_id(self, tiktok_url):
        try:
            video_id = tiktok_url.strip('/').split('/')[-1]
            return video_id
        except Exception as e:
            st.error(f"Error extracting video ID: {str(e)}")
            return None

    def get_s3_url(self, video_id):
        return f"{S3_BASE_URL}/{video_id}.mp4"

    def download_video_to_temp(self, s3_url):
        try:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
            response = requests.get(s3_url, stream=True)
            response.raise_for_status()
            
            with open(temp_file.name, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return temp_file.name
        except Exception as e:
            st.error(f"Error downloading video: {str(e)}")
            return None

    async def analyze_video(self, video_path):
        try:
            video_file = genai.upload_file(path=video_path)
            
            while video_file.state.name == "PROCESSING":
                time.sleep(2)
                video_file = genai.get_file(video_file.name)

            if video_file.state.name == "FAILED":
                raise ValueError(f"Video processing failed: {video_file.state.name}")

            prompt = """Analyze this TikTok video and provide very concise answers (max 10-15 words):

a) Video Summary: Brief description of what happens in the video
b) Analyze:
- Content Theme: Main topic/purpose
- Content Style: How it's filmed
- Creator Presence: Who appears
- Key Video Elements: Main objects/actions
- On-Screen Text/Graphics: Text overlays
- Spoken Words: Key dialogues
- Technical Elements: Filming features
- Auditory Elements: Sounds/music
- Language: Language used
- Sentiment/Tone/Vibe: Overall mood

Keep all responses very brief and direct."""

            response = self.model.generate_content(
                [video_file, prompt],
                request_options={"timeout": 600},
                generation_config=genai.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=2048
                )
            )
            
            return response.text
        except Exception as e:
            st.error(f"Error analyzing video: {str(e)}")
            return None
        finally:
            if video_path and os.path.exists(video_path):
                os.remove(video_path)

    def clean_text(self, text):
        """Remove asterisks and clean up text."""
        text = text.replace('*', '')
        text = re.sub(r'\[.*?\]', '', text)
        text = ' '.join(text.split())
        return text.strip()

    def create_analysis_table(self, text):
        """Convert analysis text to a structured table."""
        if not text:
            return pd.DataFrame()

        data = {
            'Category': ['Video Summary', 'Content Theme', 'Content Style', 'Creator Presence', 
                        'Key Video Elements', 'On-Screen Text/Graphics', 'Spoken Words', 
                        'Technical Elements', 'Auditory Elements', 'Language', 'Sentiment/Tone/Vibe'],
            'Analysis': [''] * 11
        }
        
        df = pd.DataFrame(data)
        lines = text.split('\n')
        current_section = None
        
        for line in lines:
            line = self.clean_text(line)
            if not line:
                continue
                
            if 'Video Summary' in line or line.startswith('a)'):
                content = line.split(':', 1)[1].strip() if ':' in line else ''
                df.loc[df['Category'] == 'Video Summary', 'Analysis'] = self.clean_text(content)
                
            for category in data['Category'][1:]:
                if category.lower() in line.lower():
                    content = line.split(':', 1)[1].strip() if ':' in line else line
                    df.loc[df['Category'] == category, 'Analysis'] = self.clean_text(content)
        
        return df

def main():
    st.title("Video Analysis")
    
    # Initialize analyzer
    analyzer = TikTokAnalyzer()

    # TikTok URL input with contained width
    col1, col2 = st.columns([2, 2])
    with col1:
        tiktok_url = st.text_input(
            "Enter TikTok embed URL", 
            placeholder="https://www.tiktok.com/embed/v2/..."
        )

    if tiktok_url:
        try:
            video_id = analyzer.extract_video_id(tiktok_url)
            if not video_id:
                return

            s3_url = analyzer.get_s3_url(video_id)
            
            # Create two columns for layout
            col1, col2 = st.columns([2, 3])
            
            with col1:
                embed_html = f'''
                    <div style="padding:0;margin:0;background:#000;">
                        <iframe src="{tiktok_url}" 
                                width="100%" 
                                height="600" 
                                frameborder="0" 
                                scrolling="no" 
                                allowfullscreen
                                style="padding:0;margin:0;background:#000;">
                        </iframe>
                    </div>
                '''
                st.components.v1.html(embed_html, height=600)

            with col2:
                # Create a progress bar
                progress_placeholder = st.empty()
                progress_bar = st.progress(0)
                
                # Analysis steps with progress updates
                progress_placeholder.text("Downloading video...")
                progress_bar.progress(20)
                
                temp_video_path = analyzer.download_video_to_temp(s3_url)
                if temp_video_path:
                    progress_placeholder.text("Processing video...")
                    progress_bar.progress(40)
                    
                    import asyncio
                    progress_placeholder.text("Analyzing content...")
                    progress_bar.progress(60)
                    
                    analysis_result = asyncio.run(analyzer.analyze_video(temp_video_path))
                    
                    if analysis_result:
                        progress_placeholder.text("Generating results...")
                        progress_bar.progress(80)
                        
                        df = analyzer.create_analysis_table(analysis_result)
                        if not df.empty:
                            progress_placeholder.text("Analysis complete!")
                            progress_bar.progress(100)
                            time.sleep(0.5)
                            progress_placeholder.empty()
                            progress_bar.empty()
                            
                            # Display table without index
                            st.table(df.set_index('Category').style.set_properties(**{
                                'white-space': 'normal',
                                'text-align': 'left',
                                'padding': '0.5rem'
                            }))
                    else:
                        progress_placeholder.error("Analysis failed.")
                        progress_bar.empty()
                else:
                    progress_placeholder.error("Failed to download video.")
                    progress_bar.empty()
                    
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()