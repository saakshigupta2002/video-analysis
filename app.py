import streamlit as st
import google.generativeai as genai
import os
import time
import requests
import pandas as pd
import tempfile
import re
import asyncio
import threading
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
        /* Target both container and input */
        .stTextInput {
            width: 550px;
        }
        .stTextInput > div {
            width: 550px;
        }
        .stTextInput > div > div > input {
            width: 550px;
            max-width: 550px;
        }
        iframe {
            border: none;
            background-color: #000000;
        }
        .stTable td {
            white-space: normal;
            padding: 0.5rem;
        }
        .stProgress > div > div > div {
            height: 5px;
        }
        .progress-bar-wrapper {
            margin-bottom: 1rem;
        }
        h1 {
            font-size: 1.5rem;
            margin-bottom: 1rem;
        }
        /* Hide index numbers in table */
        table {
            width: 100%;
        }
    </style>
""", unsafe_allow_html=True)


# Constants
API_KEY = os.getenv('GEMINI_API_KEY')
S3_BUCKETS = [
    "linqia-social-post-data.s3.us-east-1.amazonaws.com/tiktok_video_content_analysis",
    "tiktok-videos.s3.amazonaws.com/videos",
    # Add more S3 buckets here
]

def convert_to_embed_url(url):
    try:
        if 'embed' in url:
            return url
        if '/video/' in url:
            video_id = url.split('/video/')[1].split('?')[0]
            return f"https://www.tiktok.com/embed/v2/{video_id}"
        return url
    except Exception as e:
        st.error(f"Error converting URL: {str(e)}")
        return None

class ProgressManager:
    def __init__(self, placeholder, progress_bar):
        self.placeholder = placeholder
        self.progress_bar = progress_bar
        self.current_progress = 0
        self.target_progress = 0
        self.is_running = True
        self.current_step = ""
        
    def update_target(self, value, step_text):
        self.target_progress = value
        self.current_step = step_text
        
    def stop(self):
        self.is_running = False
        
    async def animate(self):
        while self.is_running:
            if self.current_progress < self.target_progress:
                self.current_progress = min(self.current_progress + 1, self.target_progress)
                self.progress_bar.progress(self.current_progress)
                self.placeholder.text(f"{self.current_step} ({self.current_progress}%)")
            await asyncio.sleep(0.05)

class TikTokAnalyzer:
    def __init__(self):
        genai.configure(api_key=API_KEY)
        self.model = genai.GenerativeModel('gemini-1.5-flash')

    def try_s3_download(self, video_id, progress_mgr):
        for bucket in S3_BUCKETS:
            try:
                url = f"https://{bucket}/{video_id}.mp4"
                progress_mgr.update_target(20, f"Trying S3 bucket: {bucket}...")
                
                response = requests.head(url)
                if response.status_code == 200:
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
                    response = requests.get(url, stream=True)
                    
                    with open(temp_file.name, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    return temp_file.name
            except Exception:
                continue
        return None

    async def analyze_video(self, tiktok_url, progress_mgr):
        try:
            progress_mgr.update_target(10, "Converting URL...")
            embed_url = convert_to_embed_url(tiktok_url)
            if not embed_url:
                return None

            video_id = embed_url.strip('/').split('/')[-1].split('?')[0]
            
            # Try S3 first
            progress_mgr.update_target(20, "Attempting S3 download...")
            video_path = self.try_s3_download(video_id, progress_mgr)
            
            if video_path:
                progress_mgr.update_target(40, "Processing video...")
                video_file = genai.upload_file(path=video_path)
                
                while video_file.state.name == "PROCESSING":
                    await asyncio.sleep(1)
                    video_file = genai.get_file(video_file.name)

                if video_file.state.name == "FAILED":
                    st.warning("Video processing failed, falling back to URL analysis...")
                else:
                    prompt = self.get_analysis_prompt(embed_url)
                    progress_mgr.update_target(60, "Analyzing video content...")
                    response = self.model.generate_content(
                        [video_file, prompt],
                        generation_config=genai.GenerationConfig(
                            temperature=0.1,
                            max_output_tokens=2048
                        )
                    )
                    progress_mgr.update_target(90, "Finalizing analysis...")
                    return response.text

            # Fallback to URL analysis
            progress_mgr.update_target(50, "Analyzing from URL...")
            prompt = self.get_analysis_prompt(embed_url)
            response = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=2048
                )
            )
            progress_mgr.update_target(90, "Finalizing analysis...")
            return response.text

        except Exception as e:
            st.error(f"Error in analysis: {str(e)}")
            return None
        finally:
            if 'video_path' in locals() and video_path and os.path.exists(video_path):
                os.remove(video_path)

    def get_analysis_prompt(self, url):
        return f"""Analyze this TikTok video from URL: {url}
        
        Please analyze all visible elements and provide a concise analysis (10-15 words per section):

        a) Video Summary: Brief description of what's shown
        b) Analysis Categories:
        - Content Theme: Main topic/purpose
        - Content Style: How it's filmed/presented
        - Creator Presence: Who appears
        - Key Video Elements: Objects/actions shown
        - On-Screen Text/Graphics: Visible text/overlays
        - Spoken Words: Key dialogue
        - Technical Elements: Filming features
        - Auditory Elements: Sound/music
        - Language: Languages used
        - Sentiment/Tone/Vibe: Overall mood

        Keep responses direct and specific to what's visible in the video."""

    def clean_text(self, text):
        """Remove formatting and clean up text."""
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

async def main_async():
    st.title("Video Analysis")
    
    analyzer = TikTokAnalyzer()

    tiktok_url = st.text_input(
        "Enter TikTok URL", 
        placeholder="https://www.tiktok.com/@username/video/... or embed URL"
    )

    if tiktok_url:
        try:
            embed_url = convert_to_embed_url(tiktok_url)
            if not embed_url:
                st.error("Invalid TikTok URL format")
                return

            col1, col2 = st.columns([2, 3])
            
            with col1:
                embed_html = f'''
                    <div style="padding:0;margin:0;background:#000;">
                        <iframe src="{embed_url}" 
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
                progress_placeholder = st.empty()
                progress_bar = st.progress(0)
                
                progress_mgr = ProgressManager(progress_placeholder, progress_bar)
                animation_task = asyncio.create_task(progress_mgr.animate())
                
                analysis_result = await analyzer.analyze_video(tiktok_url, progress_mgr)
                
                if analysis_result:
                    progress_mgr.update_target(100, "Analysis complete!")
                    await asyncio.sleep(0.5)
                    progress_mgr.stop()
                    await animation_task
                    
                    progress_placeholder.empty()
                    progress_bar.empty()
                    
                    df = analyzer.create_analysis_table(analysis_result)
                    if not df.empty:
                        st.table(df.set_index('Category').style.set_properties(**{
                            'white-space': 'normal',
                            'text-align': 'left',
                            'padding': '0.5rem'
                        }))
                    else:
                        st.error("Failed to parse analysis results.")
                else:
                    progress_mgr.stop()
                    await animation_task
                    progress_placeholder.error("Analysis failed.")
                    progress_bar.empty()
                    
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
