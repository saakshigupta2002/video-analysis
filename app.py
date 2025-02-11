import streamlit as st
import google.generativeai as genai
import boto3
import json
import os
import time
import requests
import pandas as pd
import tempfile
import re
import asyncio
import base64
import threading
from typing import Optional
import tempfile
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Video Analysis",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded"  
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
        table {
            width: 100%;
        }
        video {
            background-color: #000000;
        }
    </style>
""", unsafe_allow_html=True)

# Constants - Using environment variables
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

# Model configurations
GEMINI_MODELS = {
    "Gemini 2.0 Flash": "gemini-2.0-flash",
    "Gemini 2.0 Flash-Lite": "gemini-2.0-flash-lite-preview-02-05",
    "Gemini 1.5 Flash": "gemini-1.5-flash",
    "Gemini 1.5 Flash-8B": "gemini-1.5-flash-8b",
    "Gemini 1.5 Pro": "gemini-1.5-pro"
}

AWS_MODELS = {
    "AWS Nova": "amazon.nova-pro-v1:0"
}

S3_BUCKETS = [
    "linqia-social-post-data.s3.us-east-1.amazonaws.com/tiktok_video_content_analysis",
    "tiktok-videos.s3.amazonaws.com/videos",
    "linqia-social-post-data.s3.us-east-1.amazonaws.com/instagram_video_content_analysis"
]

def detect_url_type(url):
    """Detect the type of video URL"""
    if 'tiktok.com' in url:
        return 'tiktok'
    elif 'instagram.com' in url:
        return 'instagram'
    elif 'amazonaws.com' in url or url.endswith('.mp4'):
        return 'direct_mp4'
    else:
        return 'unknown'

def extract_tiktok_video_id(url):
    """Extract TikTok video ID from various URL formats"""
    try:
        if 'embed' in url:
            return url.strip('/').split('/')[-1].split('?')[0]
        if '/video/' in url:
            video_id = url.split('/video/')[1].split('?')[0]
            print(f"DEBUG - Extracted TikTok video ID: {video_id}")
            return video_id
        return None
    except Exception as e:
        print(f"DEBUG - Error extracting TikTok video ID: {str(e)}")
        return None

def extract_instagram_video_id(url):
    """Extract Instagram video ID from various URL formats"""
    patterns = [
        r'/p/([A-Za-z0-9_-]+)',
        r'/reel/([A-Za-z0-9_-]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def get_tiktok_video_url(video_id):
    """Get direct video URL from TikTok"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        embed_url = f"https://www.tiktok.com/embed/v2/{video_id}"
        response = requests.get(embed_url, headers=headers)
        
        if response.status_code == 200:
            return embed_url
        return None
    except Exception:
        return None

def get_instagram_video_url(video_id):
    """Get direct video URL from Instagram"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        embed_url = f"https://www.instagram.com/p/{video_id}/embed/"
        response = requests.get(embed_url, headers=headers)
        
        if response.status_code == 200:
            return embed_url
        return None
    except Exception:
        return None

def convert_to_embed_url(url):
    """Convert video URL to appropriate format"""
    try:
        url_type = detect_url_type(url)
        
        if url_type == 'tiktok':
            if 'embed' in url:
                return url, 'tiktok'
            video_id = extract_tiktok_video_id(url)
            print(f"DEBUG - TikTok video ID in convert_to_embed_url: {video_id}")
            if video_id:
                embed_url = f"https://www.tiktok.com/embed/v2/{video_id}"
                return embed_url, 'tiktok'
        
        elif url_type == 'instagram':
            if 'embed' in url:
                return url, 'instagram'
            
            video_id = extract_instagram_video_id(url)
            print(f"DEBUG - Instagram video ID: {video_id}")
            if video_id:
                embed_url = get_instagram_video_url(video_id)
                if embed_url:
                    return embed_url, 'instagram'
            # For direct Instagram URLs
            if '/p/' in url or '/reel/' in url:
                return url, 'instagram'
        
        elif url_type == 'direct_mp4':
            # Add https:// if not present
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            return url, 'direct_mp4'
            
        return url, url_type
    except Exception as e:
        st.error(f"Error converting URL: {str(e)}")
        return None, None

def display_video(url, url_type, col):
    """Display video based on type"""
    try:
        if url_type == 'tiktok':
            embed_html = f'''
                <div style="padding:0;margin:0;background:#000;">
                    <iframe src="{url}" 
                            width="100%" 
                            height="600" 
                            frameborder="0" 
                            scrolling="no" 
                            allowfullscreen
                            style="padding:0;margin:0;background:#000;">
                    </iframe>
                </div>
            '''
            col.markdown(embed_html, unsafe_allow_html=True)
        
        elif url_type == 'instagram':
            # Convert regular Instagram URLs to embed URLs if needed
            if '/embed/' not in url:
                video_id = extract_instagram_video_id(url)
                if video_id:
                    url = f"https://www.instagram.com/p/{video_id}/embed/"
            
            embed_html = f'''
                <div style="padding:0;margin:0;background:#000;">
                    <iframe src="{url}" 
                            width="100%" 
                            height="600" 
                            frameborder="0" 
                            scrolling="no" 
                            allowtransparency="true"
                            style="padding:0;margin:0;background:#000;">
                    </iframe>
                </div>
            '''
            col.markdown(embed_html, unsafe_allow_html=True)
        
        elif url_type == 'direct_mp4':
            video_html = f'''
                <div style="padding:0;margin:0;background:#000;">
                    <video width="100%" height="600" controls autoplay>
                        <source src="{url}" type="video/mp4">
                        Your browser does not support the video tag.
                    </video>
                </div>
            '''
            col.markdown(video_html, unsafe_allow_html=True)
        else:
            col.error("Unsupported video format")
    except Exception as e:
        col.error(f"Error displaying video: {str(e)}")

def format_bedrock_input(prompt):
    """Format the input for AWS Bedrock Nova model"""
    return {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ]
    }

class ModelConfig:
    def __init__(self):
        # Initialize the sidebar
        st.sidebar.title("Model Configuration")
        
        # First level: Select AI Platform
        self.platform = st.sidebar.radio(
            "Select AI Platform",
            options=["Google Gemini"],
            help="Choose the AI platform for analysis"
        )

        self.prompt_style = st.sidebar.selectbox(
            "Select Prompting Style",
            options=["With Options", "Without Options"],
            help="Choose the prompting style"
        )
        
        
        # Second level: Select specific model based on platform
        if self.platform == "Google Gemini":
            self.model_name = st.sidebar.selectbox(
                "Select Gemini Model",
                options=list(GEMINI_MODELS.keys()),
                help="Choose specific Gemini model version"
            )
            self.model_id = GEMINI_MODELS[self.model_name]
            
            # Model-specific parameters for Gemini
            self.temperature = st.sidebar.slider(
                "Temperature",
                min_value=0.0,
                max_value=1.0,
                value=0.7,
                step=0.1
            )
            
            self.max_output_tokens = st.sidebar.slider(
                "Max Output Tokens",
                min_value=512,
                max_value=8192,
                value=2048,
                step=512
            )
            
            try:
                genai.configure(api_key=GEMINI_API_KEY)
                self.model = genai.GenerativeModel(self.model_id)
                st.sidebar.success(f"{self.model_name} initialized successfully")
            except Exception as e:
                st.sidebar.error(f"Error initializing Gemini model: {str(e)}")
                
        else:  # AWS Nova
            self.model_name = st.sidebar.selectbox(
                "Select AWS Model",
                options=list(AWS_MODELS.keys()),
                help="Choose AWS Claude model for analysis"
            )
            self.model_id = AWS_MODELS[self.model_name]
            
            # Model-specific parameters for AWS
            self.temperature = st.sidebar.slider(
                "Temperature",
                min_value=0.0,
                max_value=1.0,
                value=0.7,
                step=0.1
            )
            
            self.max_tokens = st.sidebar.slider(
                "Max Tokens",
                min_value=512,
                max_value=8192,
                value=2048,
                step=512
            )
            
            try:
                self.bedrock_client = boto3.client(
                    'bedrock-runtime',
                    aws_access_key_id=AWS_ACCESS_KEY,
                    aws_secret_access_key=AWS_SECRET_KEY,
                    region_name='us-east-1'
                )
                st.sidebar.success(f"{self.model_name} initialized successfully")
            except Exception as e:
                st.sidebar.error(f"Error initializing AWS model: {str(e)}")

    async def generate_content(self, prompt, is_video_file=False):
        """Generate content using selected model"""
        try:
            if self.platform == "Google Gemini":
                if is_video_file:
                    response = self.model.generate_content(
                        [prompt[0], prompt[1]],
                        generation_config=genai.GenerationConfig(
                            temperature=self.temperature,
                            max_output_tokens=self.max_output_tokens
                        )
                    )
                else:
                    response = self.model.generate_content(
                        prompt,
                        generation_config=genai.GenerationConfig(
                            temperature=self.temperature,
                            max_output_tokens=self.max_output_tokens
                        )
                    )
                return response.text
                
            else:  # AWS Nova
                if is_video_file:
                    video_path, text_prompt = prompt
                    
                    # Read and encode video file
                    with open(video_path, 'rb') as video_file:
                        video_bytes = video_file.read()
                        video_base64 = base64.b64encode(video_bytes).decode('utf-8')
                    
                    request_body = {
                        "anthropic_version": "bedrock-2023-05-31",
                        "messages": [
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "image",
                                        "source": {
                                            "type": "base64",
                                            "media_type": "video/mp4",
                                            "data": video_base64
                                        }
                                    },
                                    {
                                        "type": "text",
                                        "text": text_prompt
                                    }
                                ]
                            }
                        ],
                        "max_tokens": self.max_tokens,
                        "temperature": self.temperature
                    }
                else:
                    request_body = format_bedrock_input(prompt)
                
                response = self.bedrock_client.invoke_model(
                    modelId=self.model_id,
                    body=json.dumps(request_body)
                )
                
                response_body = json.loads(response['body'].read())
                if is_video_file:
                    return response_body.get('content', [{}])[0].get('text', '')
                return response_body.get('content', [{}])[0].get('text', '')
                    
        except Exception as e:
            st.error(f"Model error: {str(e)}")
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
        """Update progress target and step text"""
        self.target_progress = value
        self.current_step = step_text
        
    def stop(self):
        """Stop progress animation"""
        self.is_running = False
        
    async def animate(self):
        """Animate progress bar"""
        while self.is_running and self.current_progress < 100:
            if self.current_progress < self.target_progress:
                self.current_progress += 1
                self.progress_bar.progress(self.current_progress / 100.0)
                self.placeholder.text(f"{self.current_step} ({self.current_progress}%)")
            await asyncio.sleep(0.05)

class PromptBuilder:
    def __init__(self):
        """Initialize the PromptBuilder with Excel file from GitHub."""
        self.excel_url = "https://github.com/saakshigupta2002/video-analysis/raw/main/Content%20Analysis%20Full%20Benchmark%20Labels.xlsx"
        
    def download_excel(self):
        """Download Excel file from GitHub and return as DataFrame."""
        try:
            response = requests.get(self.excel_url)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                tmp_file.write(response.content)
                return pd.ExcelFile(tmp_file.name)
        except Exception as e:
            st.error(f"Error downloading Excel file: {str(e)}")
            return None

    def get_column_values(self, excel_file, column_index):
        """Get unique values from a column in the main sheet."""
        try:
            df = pd.read_excel(excel_file, sheet_name="Benchmark List of Labels")
            column = df.iloc[:, column_index]
            values = column[column.str.startswith('-', na=False)].dropna()
            return [str(val).strip('- ') for val in values if str(val).startswith('-')]
        except Exception as e:
            st.error(f"Error reading column values: {str(e)}")
            return []

    def get_bucketed_options(self, excel_file, sheet_name):
        """Get options organized by buckets from specified sheet."""
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            text_col = df.iloc[:, 0]
            label_col = df.iloc[:, 1]
            
            buckets = {}
            current_bucket = None
            
            for idx in range(2, len(df)):
                text = str(text_col.iloc[idx]).strip()
                label = str(label_col.iloc[idx]).strip()
                
                if label and label != 'nan' and label != 'label':
                    current_bucket = label
                    buckets[current_bucket] = []
                    if text and text != 'nan' and text != 'text':
                        items = [item.strip() for item in text.split(',')]
                        buckets[current_bucket].extend(items)
                elif current_bucket and text and text != 'nan' and text != 'text':
                    items = [item.strip() for item in text.split(',')]
                    buckets[current_bucket].extend(items)
            
            return buckets
        except Exception as e:
            st.error(f"Error reading bucketed options: {str(e)}")
            return {}

    def build_prompt(self):
        """Build the analysis prompt with options from Excel."""
        excel_file = self.download_excel()
        if not excel_file:
            return None
            
        try:
            # Get category values
            content_themes = self.get_column_values(excel_file, 1)
            content_styles = self.get_column_values(excel_file, 3)
            creator_presence = self.get_column_values(excel_file, 5)
            key_elements = self.get_column_values(excel_file, 7)
            text_graphics = self.get_column_values(excel_file, 8)
            spoken_words = self.get_column_values(excel_file, 9)
            technical_elements = self.get_column_values(excel_file, 10)
            auditory_elements = self.get_column_values(excel_file, 11)
            
            # Get language and sentiment options
            language_buckets = self.get_bucketed_options(excel_file, "Bucketed Languages")
            sentiment_buckets = self.get_bucketed_options(excel_file, "Bucketed sentiments")
            
            # Using the exact prompt format provided
            prompt = """
            Analyze this TikTok video and provide:
            a) Brief video summary
            b) Answer these questions:

            i) Content Theme [Content Theme refers to the primary subject matter, topic, or field that the video primarily focuses on.
            This represents the core message or information being conveyed, regardless of presentation style or sound.
            Choose from these options: {content_themes}
            Choose the most relevant themes, maximum selection being three word selections. Do not explain any reasoning]

            ii) Content Style [Content Style describes the format, production techniques, and presentation methods/frequent templates used to deliver the content.
            Choose from these options: {content_styles}
            Choose the most relevant style, maximum selection being three word selections. Do not explain any reasoning]

            iii) Creator Presence [Creator Presence describes the way the content creator or main subject appears in the video, including their visibility, framing, disregarding WHAT the vibe or tone/presentation style the creator presents.
            Choose from these options: {creator_presence}
            Please also identify if there is group content in the output, do not include attribute if it is not a group content.
            Maximum selection being the most relevant three words. Do not explain any reasoning]

            iv) Key Video Elements [Key Video Elements are the primary visual, setting or audio components that appear in the video, including creator, product (specifiy), and activities that contribute to the content's overall composition and focus.
            Choose from these options: {key_elements}
            If it is an attribute is the main creator, categorise as 'the creator'.
            Maximum selection being the most relevant three words. Do not explain any reasoning]

            v) On-Screen Text/Graphics [Text Graphics Elements are any written or graphical overlays added to the video during production or post-production, serving informational, branding, or engagement purposes.
            Choose from these options: {text_graphics}
            Ensure to identify any watermarks, closed captioning and any screen overlays.
            Maximum selection being three words. Do not explain any reasoning]

            vi) Spoken Words [Spoken Words encompasses all auditory communication methods used in the video, including various speaking styles, whom the voice is directed to and vocal presentations.
            Choose from these options: {spoken_words}
            Ensure to identify whether there are multiple voices in such if it is a group discussion.
            Maximum selection being the most relevant three words. Do not explain any reasoning]

            vii) Technical Elements [Technical Elements encompass the cinematographic techniques, editing methods, and visual effects used in video production and post-production.
            Choose from these options: {technical_elements}
            Choose the most relevant technical elements, maximum selection being three word selections exactly from the benchmark list. Do not explain any reasoning]

            viii) Auditory Elements [Auditory Elements include all sound components used in the video, including music, effects, and their sources.
            Choose from these options: {auditory_elements}
            Maximum selection being three words. Do not explain any reasoning]

            ix) Language [choose from these options: {languages}
            Choose one category of label with exact wording]

            x) Sentiment/Tone/Vibe [Choose from these buckets: {sentiments}
            Maximum selection two bucketed sentiments]

            xi) Video Length [categorise according to the ranges but only state the word not in brackets: Ultra-short (0-14 seconds), Short (15-60 seconds), Medium (1-2 minutes), Standard (3-4 minutes), Long (5-9 minutes), Extended (10-19 minutes), Series (multiple connected videos), Carousel (image based)]

            xii) Brand Safety [Brand Safety Considerations identify potential content concerns that might affect advertiser suitability or audience appropriateness.
            Choose from the benchmark dataset. If it is generally safe, still state reason for brand safety concerns. If no brand safety concerns, output 'none'.]

            xiii) Brands Featured [list all brands present but do not include tiktok. If no brands identifed, output 'no brands featured']

            xiv) Target Audience [Target Audience identifies the primary and secondary viewer demographics, including age groups, professional backgrounds, interests, and lifestyle characteristics that the content is designed to reach and engage.
            Choose from these options and select one specific general niche of target audience.
            Maximum selection being three words. Do not explain any reasoning]

            xv) Location [Location Elements identify the physical or virtual environments where content is created or depicted, including both general settings and specific regional contexts.
            Choose one region and one set location from these options.
            Choose one region and one set location from the benchmark label list]
            """.format(
                content_themes=', '.join(content_themes),
                content_styles=', '.join(content_styles),
                creator_presence=', '.join(creator_presence),
                key_elements=', '.join(key_elements),
                text_graphics=', '.join(text_graphics),
                spoken_words=', '.join(spoken_words),
                technical_elements=', '.join(technical_elements),
                auditory_elements=', '.join(auditory_elements),
                languages=', '.join([item for bucket in language_buckets.values() for item in bucket]),
                sentiments=', '.join([f"{bucket}: {', '.join(items)}" for bucket, items in sentiment_buckets.items()])
            )
            
            return prompt
            
        except Exception as e:
            st.error(f"Error building prompt: {str(e)}")
            return None

class TikTokAnalyzer:
    def __init__(self):
        self.model_config = ModelConfig()
        self.prompt_builder = PromptBuilder()

    def get_analysis_prompt(self, url):
        """Generate analysis prompt for the video"""
        if self.model_config.prompt_style == "With Prompt Options":
            prompt = self.prompt_builder.build_prompt()
            if prompt:
                return prompt
            else:
                st.error("Failed to build prompt with options. Falling back to default prompt.")
                return self.get_default_prompt()
        else:
            return self.get_default_prompt()

    def get_default_prompt(self):
            """Return the default prompt without options"""
            return """Analyze this TikTok video and provide:
            a) Brief video summary -

            b) Answer these questions:
            i) Content Theme [Content Theme refers to the primary subject matter, topic, or field that the video primarily focuses on.
            This represents the core message or information being conveyed, regardless of presentation style or sound.
            Examples of this category that are not exhaustive include: Education, Language Learning, Science Experiments, History Lessons, Humor/comedy, Stand-up Comedy, Pranks, Sketch Comedy, Lifestyle, Minimalism, Luxury Living, Van Life, Personal Finance, Budgeting Tips, Investing Basics, Cryptocurrency, Mental Health, Meditation, Therapy Insights, Self-Care Tips.
            Choose the most relevant themes, maximum selection being three word selections. Do not explain any reasoning]

            ii) Content Style [Content Style describes the format, production techniques, and presentation methods/frequent templates used to deliver the content.
            Examples of this category that are not exhaustive include: ASMR, Skits, Transitions, Graphics-heavy, Vlogs, Day-in-the-Life, Reviews, Unboxing, UGC, Edits, Reaction Videos etc.
            Choose the most relevant styles, maximum selection being three word selections. Do not explain any reasoning]

            iii) Creator Presence [Creator Presence describes the way the content creator or main subject appears in the video, including their visibility, framing, disregarding WHAT the vibe or tone/presentation style the creator presents such as if they are lively or energetic etc.
            Examples of this category that are not exhaustive include: Occasional appearances, Group content, digital overlays/masks, Animated avatar, Behind the camera, Filter applied to creator, full body presence, upper body only, lower body only, face only, hands only, eyes only.
            Choose the most relevant presence, maximum selection being three word selections. Do not explain any reasoning]

            iv) Key Video Elements [Key Video Elements are the primary visual, setting or audio components that appear in the video, including creator, product (specifiy), and activities that contribute to the content's overall composition and focus.
            Examples of this category that are not exhaustive include: Main Creator, Co-creators/Collaborators, Family Members, Vehicles, Pets, Animals, A location, A product, An activity, Text/Graphics, Music/Sound effects, Special Effects, Beauty Products etc.
            If it is an attribute is the main creator, categorise as 'the creator'.
            Choose the most relevant elements, maximum selection being three word selections. Do not explain any reasoning]

            v) On-Screen Text/Graphics [Text Graphics Elements are any written or graphical overlays added to the video during production or post-production, serving informational, branding, or engagement purposes.
            If there is hastags (#), categorise as FTC Disclosures. Choose the most relevant on-screen graphics, maximum selection being three word selections. Do not explain any reasoning]

            vi) Spoken Words [For Spoken Words, it encompasses all auditory communication methods used in the video, including various speaking styles, languages, and vocal presentations.
            Examples of this category that are not exhaustive include: No words, Voiceover, Talking to Camera, voice with background music, Narration, Dialogue, Monologue, Singing, Whispered voice, Scripted, Improvised, Interview-style etc.
            If there is no words, consider if there is lip syncing. Choose maximum three words. Do not explain any reasoning]

            vii) Technical Elements [Technical Elements encompass the cinematographic techniques, editing methods, and visual effects used in video production and post-production.
            Choose the most relevant technical elements, maximum selection being three word selections. Do not explain any reasoning]

            viii) Auditory Elements [Auditory Elements include all sound components used in the video, including music, effects, and their sources.
            Examples of this category that are not exhaustive include: Original sounds/music, Mixes trending and original sounds, Uses popular music (licensed), Uses royalty-free music, Silent videos (no music/sounds), Uses sound effects, Repurposes old trending sounds, Uses platform-specific sounds, Collaborates on sounds with other creators (mixtape/remix), Uses other creator's original sound, Uses external sound snippets, Uses current trending/viral sounds etc.
            Choose the most relevant auditory themes, maximum selection being three word selections. Do not explain any reasoning]

            ix) Language [specify one]

            x) Sentiment/Tone/Vibe [Describe primary and secondary if applicable.
            Examples of this category that are not exhaustive include: Positive, Negative, Neutral, Celebratory, Challenging, Situational, Cultural and Trend-Based Vibes, Content-Specific Emotions, Combination, Meta/Community Emotions.
            Maximum three word choices. Do not explain reasoning]

            xi) Video Length [Categorise according to the ranges but only state the word not in brackets: Ultra-short (0-15 seconds), Short (15-60 seconds), Medium (1-3 minutes), Standard (3-5 minutes), Long (5-10 minutes), Extended (10-20 minutes), Series (multiple connected videos), Carousel (image based)]

            xii) Brand Safety [Brand Safety Considerations identify potential content concerns that might affect advertiser suitability or audience appropriateness.
            Choose the most relevant attributes, maximum selection being three word selections. Do not explain any reasoning]

            xiii) Brands Featured [List all brands present but do not include tiktok. If no brands are identifed, output 'no brands featured']

            xiv) Target Audience [Examples of this category that are not exhaustive include: Gen Alpha, Gen Z, Millennials, Gen X,Boomers, Students, Young Professionals, Parents, Entrepreneurs, Beauty Enthusiasts, Fitness Enthusiasts, Foodies, Fashion Followers etc.
            Choose the most relevant audience, maximum selection being three words. Do not explain any reasoning]

            xv) Location [Choose one region and one set location of the video.
            Examples of this category that are not exhaustive include: (Home Interior, Apartment Features, Retail environments, Food & Beverage Locations, Entertainment Venues, Office Environment, Educational Facilities, Health & Wellness, Urban Environments, Natural Settings, Recreational Areas) (APAC Region (Asia-Pacific), EMEA (Europe, Middle East, and Africa), Americas (North, central, south america and Caribbean), LATAM (Latin America), SAARC Region (South Asia))
            Choose one region and one set location from the benchmark label list]

            Format each answer with its heading and provide specific, concise responses."""

    async def analyze_video(self, video_url, progress_mgr):
        """Analyze video content"""
        try:
            progress_mgr.update_target(10, "Processing URL...")
            processed_url, url_type = convert_to_embed_url(video_url)
            if not processed_url:
                st.error("Invalid URL format")
                return None

            # Handle video download
            progress_mgr.update_target(30, "Downloading video...")
            video_path = None
            
            if url_type == 'tiktok':
                video_id = extract_tiktok_video_id(video_url)
                if video_id:
                    video_path = self.try_s3_download(video_id, progress_mgr)
            elif url_type == 'instagram':
                video_id = extract_instagram_video_id(video_url)
                if video_id:
                    video_path = self.try_s3_download(video_id, progress_mgr)
            elif url_type == 'direct_mp4':
                video_path = self.download_direct_video(processed_url, progress_mgr)
            
            if not video_path:
                st.error("Failed to analyse video. Please check the URL format and try again.")
                return None

            progress_mgr.update_target(50, "Processing video content...")
            prompt = self.get_analysis_prompt(processed_url)
            
            if self.model_config.platform == "Google Gemini":
                video_file = genai.upload_file(path=video_path)
                
                while video_file.state.name == "PROCESSING":
                    await asyncio.sleep(1)
                    video_file = genai.get_file(video_file.name)
                if video_file.state.name == "FAILED":
                    st.error("Video processing failed")
                    return None
                
                progress_mgr.update_target(70, "Analyzing video content...")
                response = await self.model_config.generate_content([video_file, prompt], True)
                
            else:
                progress_mgr.update_target(70, "Analyzing video content...")
                response = await self.model_config.generate_content([video_path, prompt], True)
            
            progress_mgr.update_target(90, "Finalizing analysis...")
            return response

        except Exception as e:
            st.error(f"Analysis error: {str(e)}")
            st.exception(e)
            return None
        finally:
            if 'video_path' in locals() and video_path and os.path.exists(video_path):
                os.remove(video_path)

    def try_s3_download(self, video_id, progress_mgr):
        """Attempt to download video from S3 buckets"""
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

    def download_direct_video(self, url, progress_mgr):
        """Download video from direct URL"""
        try:
            response = requests.head(url)
            if response.status_code == 200:
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
                response = requests.get(url, stream=True)
                
                with open(temp_file.name, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                return temp_file.name
        except Exception as e:
            st.error(f"Error downloading video: {str(e)}")
            return None
        return None

    
    def clean_text(self, text):
        """Clean and format analysis text"""
        if not text:
            return ""
        text = text.replace('*', '')
        text = re.sub(r'\[.*?\]', '', text)
        text = ' '.join(text.split())
        return text.strip()

    def create_analysis_table(self, text):
        """Create formatted analysis table from response"""
        if not text:
            return pd.DataFrame()

        categories = [
            'Video Summary',
            'Content Theme',
            'Content Style',
            'Creator Presence',
            'Key Video Elements',
            'On-Screen Text/Graphics',
            'Spoken Words',
            'Technical Elements',
            'Auditory Elements',
            'Language',
            'Sentiment/Tone/Vibe',
            'Video Length',
            'Brand Safety',
            'Brands Featured',
            'Target Audience',
            'Location'
        ]
        
        data = {
            'Category': categories,
            'Analysis': [''] * len(categories)
        }
        
        df = pd.DataFrame(data)
        
        # First, clean up markdown formatting
        text = text.replace('**', '')
        text = text.replace('*', '')
        
        # Split into lines and process each line
        lines = text.split('\n')
        current_category = None
        content_buffer = []
        
        # Category mapping including common variations
        category_mapping = {
            'brief video summary': 'Video Summary',
            'content theme': 'Content Theme',
            'content style': 'Content Style',
            'creator presence': 'Creator Presence',
            'key video elements': 'Key Video Elements',
            'on-screen text/graphics': 'On-Screen Text/Graphics',
            'spoken words': 'Spoken Words',
            'technical elements': 'Technical Elements',
            'auditory elements': 'Auditory Elements',
            'language': 'Language',
            'sentiment/tone/vibe': 'Sentiment/Tone/Vibe',
            'video length': 'Video Length',
            'brand safety': 'Brand Safety',
            'brands featured': 'Brands Featured',
            'target audience': 'Target Audience',
            'location': 'Location'
        }
        
        def find_category(line):
            # Check for roman numerals pattern
            roman_pattern = r'^(?:i[vx]|v|x|i{1,3})\)'
            if re.match(roman_pattern, line.lower()):
                line = line.split(')', 1)[1].strip()
            
            # Remove leading a) b) etc
            if line.startswith(('a)', 'b)')):
                line = line.split(')', 1)[1].strip()
                
            # Look for category in the line
            for key, category in category_mapping.items():
                if key in line.lower():
                    return category, line.split(':', 1)[1].strip() if ':' in line else ''
            return None, None

        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            category, initial_content = find_category(line)
            
            if category:
                # Save previous category's content if exists
                if current_category and content_buffer:
                    content = ', '.join(filter(None, content_buffer))
                    df.loc[df['Category'] == current_category, 'Analysis'] = content
                
                # Start new category
                current_category = category
                content_buffer = [initial_content] if initial_content else []
            elif current_category and line:
                # Clean up bullet points and other markers
                line = re.sub(r'^\s*[\*\-•]\s*', '', line)
                if line:
                    content_buffer.append(line)
        
        # Don't forget to save the last category
        if current_category and content_buffer:
            content = ', '.join(filter(None, content_buffer))
            df.loc[df['Category'] == current_category, 'Analysis'] = content
        
        # Clean up the Analysis column
        for idx, row in df.iterrows():
            if pd.notnull(row['Analysis']):
                # Remove category names from content
                analysis = row['Analysis']
                for cat in categories:
                    analysis = analysis.replace(f"{cat}:", "")
                # Clean up any remaining markers
                analysis = re.sub(r'^[ivxIVX]+\)', '', analysis)
                analysis = re.sub(r'^[a-zA-Z]\)', '', analysis)
                analysis = self.clean_text(analysis)
                df.at[idx, 'Analysis'] = analysis

        return df
    


async def main_async():
    """Main async application flow"""
    st.title("Video Analysis")
    
    analyzer = TikTokAnalyzer()

    video_url = st.text_input(
        "Enter Video URL", 
        placeholder="Enter TikTok, Instagram, or direct MP4 URL"
    )

    if video_url:
        try:
            processed_url, url_type = convert_to_embed_url(video_url)
            if not processed_url:
                st.error("Invalid URL format")
                return

            col1, col2 = st.columns([2, 3])
            
            with col1:
                display_video(processed_url, url_type, col1)

            with col2:
                progress_placeholder = st.empty()
                progress_bar = st.progress(0)
                
                progress_mgr = ProgressManager(progress_placeholder, progress_bar)
                animation_task = asyncio.create_task(progress_mgr.animate())
                
                analysis_result = await analyzer.analyze_video(video_url, progress_mgr)
                
                # Stop progress animation
                progress_mgr.stop()
                await animation_task
                progress_placeholder.empty()
                progress_bar.empty()
                
                # Only show analysis if we have a valid result
                if analysis_result:
                    df = analyzer.create_analysis_table(analysis_result)
                    if not df.empty:
                        st.table(df.set_index('Category').style.set_properties(**{
                            'white-space': 'normal',
                            'text-align': 'left',
                            'padding': '0.5rem',
                            'min-width': '200px',  # Add minimum width
                            'max-width': '800px'   # Add maximum width
                        }).set_table_styles([
                            {'selector': 'th', 'props': [('font-weight', 'bold')]},
                            {'selector': 'td', 'props': [('vertical-align', 'top')]}
                        ]))
                        
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.exception(e)

def main():
    """Main application entry point"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
