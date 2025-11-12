# # streamlit_app.py - IMPROVED VERSION
# import streamlit as st
# import uuid
# import json
# import threading
# import queue
# import time
# from kafka import KafkaProducer, KafkaConsumer
# from streamlit_webrtc import (
#     webrtc_streamer, 
#     VideoProcessorBase, 
#     WebRtcMode,
#     RTCConfiguration
# )
# import av
# import cv2
# from deepface import DeepFace
# from urllib.parse import quote_plus
# import logging

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # -----------------------
# # Configuration
# # -----------------------
# KAFKA_BOOTSTRAP = "localhost:9092"
# TOPIC_USER_ACTIONS = "user_actions"
# TOPIC_USER_PLAYLISTS = "user_playlists"

# RTC_CONFIGURATION = RTCConfiguration(
#     {"iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]}
# )

# # Mood detection settings
# ANALYSIS_INTERVAL = 10.0  # Analyze every 10 seconds
# MOOD_DEBOUNCE_COUNT = 2   # Require 2 consecutive same moods before sending

# # -----------------------
# # Helper Functions
# # -----------------------
# def youtube_search_link(query):
#     """Generate YouTube search URL"""
#     if not query:
#         return None
#     return f"https://www.youtube.com/results?search_query={quote_plus(query)}"

# def extract_youtube_id(url):
#     """Extract video ID from YouTube URL"""
#     if not url:
#         return None
#     if 'watch?v=' in url:
#         return url.split('watch?v=')[1].split('&')[0]
#     elif 'youtu.be/' in url:
#         return url.split('youtu.be/')[1].split('?')[0]
#     return None

# # -----------------------
# # Initialize Session State
# # -----------------------
# def initialize_session_state():
#     """Initialize all session state variables"""
#     defaults = {
#         'user_id': str(uuid.uuid4()),
#         'detected_mood': None,
#         'last_sent_mood': None,
#         'latest_playlist': [],
#         'playlist_queue': queue.Queue(),
#         'mood_queue': queue.Queue(),
#         'last_mood_time': None,
#         'mood_history': [],
#         'detection_count': 0,
#         'playlist_count': 0,
#         'playlist_consumer_thread_started': False,
#         'consecutive_mood_count': 0,
#         'pending_mood': None,
#         'kafka_connected': False,
#         'last_error': None
#     }
    
#     for key, value in defaults.items():
#         if key not in st.session_state:
#             st.session_state[key] = value

# initialize_session_state()

# # -----------------------
# # Kafka Producer with Error Handling
# # -----------------------
# @st.cache_resource
# def get_kafka_producer():
#     """Create Kafka producer with connection retry"""
#     try:
#         producer = KafkaProducer(
#             bootstrap_servers=KAFKA_BOOTSTRAP,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#             max_block_ms=5000,  # Timeout after 5 seconds
#             retries=3
#         )
#         logger.info("✅ Kafka producer connected")
#         return producer
#     except Exception as e:
#         logger.error(f"❌ Kafka producer failed: {e}")
#         return None

# producer = get_kafka_producer()
# st.session_state.kafka_connected = producer is not None

# # -----------------------
# # Background Kafka Consumer
# # -----------------------
# def start_playlist_consumer(user_id, playlist_queue):
#     """Background thread to consume playlist updates"""
#     logger.info(f"🎧 Starting consumer for user: {user_id}")
    
#     max_retries = 5
#     retry_delay = 5
    
#     for attempt in range(max_retries):
#         try:
#             consumer = KafkaConsumer(
#                 TOPIC_USER_PLAYLISTS,
#                 bootstrap_servers=KAFKA_BOOTSTRAP,
#                 auto_offset_reset='latest',
#                 value_deserializer=lambda v: json.loads(v.decode('utf-8')),
#                 consumer_timeout_ms=1000,
#                 session_timeout_ms=30000,
#                 heartbeat_interval_ms=10000
#             )
#             logger.info(f"✅ Consumer connected (attempt {attempt + 1})")
#             break
#         except Exception as e:
#             logger.error(f"❌ Consumer connection failed (attempt {attempt + 1}): {e}")
#             if attempt < max_retries - 1:
#                 time.sleep(retry_delay)
#             else:
#                 logger.error("❌ Consumer failed to start after max retries")
#                 return
    
#     while True:
#         try:
#             for msg in consumer:
#                 payload = msg.value
#                 logger.info(f"📨 Received message: {payload.get('user_id')}")
                
#                 if payload.get('user_id') == user_id:
#                     playlist_queue.put(payload)
#                     logger.info(f"✅ Playlist queued for user {user_id}")
            
#             time.sleep(0.5)
            
#         except Exception as e:
#             logger.error(f"❌ Consumer error: {e}")
#             time.sleep(5)

# # Start consumer thread (only once)
# if not st.session_state.playlist_consumer_thread_started:
#     consumer_thread = threading.Thread(
#         target=start_playlist_consumer,
#         args=(st.session_state.user_id, st.session_state.playlist_queue),
#         daemon=True
#     )
#     consumer_thread.start()
#     st.session_state.playlist_consumer_thread_started = True
#     logger.info("🚀 Consumer thread started")

# # -----------------------
# # Video Processor - Emotion Detection
# # -----------------------
# class DeepFaceProcessor(VideoProcessorBase):
#     """Process video frames and detect emotions"""
    
#     def __init__(self, user_id, mood_queue):
#         self.user_id = user_id
#         self.mood_queue = mood_queue
#         self.last_analysis_time = 0
#         self.analysis_interval = ANALYSIS_INTERVAL
#         self.frame_count = 0
#         self.detection_count = 0
#         self.last_mood = None
#         self.analyzing = False

#     def recv(self, frame):
#         """Process each video frame"""
#         img = frame.to_ndarray(format="bgr24")
#         current_time = time.time()
#         self.frame_count += 1
        
#         # Show live indicator
#         cv2.putText(img, "LIVE", (10, 30), 
#                    cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)
        
#         # Perform analysis at intervals
#         time_since_last = current_time - self.last_analysis_time
        
#         if time_since_last >= self.analysis_interval and not self.analyzing:
#             self.analyzing = True
            
#             try:
#                 # Show analyzing status
#                 cv2.putText(img, "Analyzing...", (10, 70), 
#                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 0), 2)
                
#                 # Resize for faster processing
#                 small = cv2.resize(img, (320, 240))
                
#                 # Analyze emotion
#                 result = DeepFace.analyze(
#                     small,
#                     actions=['emotion'],
#                     enforce_detection=False,
#                     detector_backend='opencv',
#                     silent=True
#                 )
                
#                 res = result[0] if isinstance(result, list) else result
                
#                 if res and 'dominant_emotion' in res:
#                     mood = res['dominant_emotion']
#                     confidence = res.get('emotion', {}).get(mood, 0)
                    
#                     self.detection_count += 1
#                     self.last_analysis_time = current_time
#                     self.last_mood = mood
                    
#                     # Send to queue
#                     self.mood_queue.put({
#                         'mood': mood,
#                         'confidence': confidence,
#                         'timestamp': current_time
#                     })
                    
#                     logger.info(f"✅ Detected: {mood} ({confidence:.0f}% confidence)")
                
#             except Exception as e:
#                 logger.error(f"❌ DeepFace error: {e}")
#             finally:
#                 self.analyzing = False
        
#         # Display current mood and countdown
#         if self.last_mood:
#             mood_text = f"Mood: {self.last_mood.upper()}"
#             cv2.putText(img, mood_text, (10, img.shape[0] - 50), 
#                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2)
        
#         # Show countdown to next analysis
#         next_in = max(0, self.analysis_interval - time_since_last)
#         countdown_text = f"Next: {next_in:.0f}s"
#         cv2.putText(img, countdown_text, (10, img.shape[0] - 20), 
#                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        
#         return av.VideoFrame.from_ndarray(img, format="bgr24")

# # -----------------------
# # Streamlit UI
# # -----------------------
# st.set_page_config(
#     page_title="Real-Time Mood Music", 
#     layout="wide",
#     page_icon="🎵"
# )

# st.title("🎵 Real-Time Mood-Based Music Player")
# st.caption("🎥 AI analyzes your facial expressions • 🎵 Music adapts to your mood")

# # Connection status banner
# if not st.session_state.kafka_connected:
#     st.error("⚠️ Kafka not connected. Please ensure Kafka is running on localhost:9092")

# col1, col2 = st.columns([1, 2])

# # -----------------------
# # LEFT COLUMN - Camera & Mood Detection
# # -----------------------
# with col1:
#     st.subheader("📹 Live Mood Detection")
#     st.info(f"🔄 Detection runs every {ANALYSIS_INTERVAL:.0f} seconds")
    
#     current_user_id = st.session_state.user_id
#     current_mood_queue = st.session_state.mood_queue
    
#     ctx = webrtc_streamer(
#         key=f"mood_stream_{current_user_id}",
#         mode=WebRtcMode.SENDRECV,
#         rtc_configuration=RTC_CONFIGURATION,
#         video_processor_factory=lambda: DeepFaceProcessor(
#             current_user_id,
#             current_mood_queue
#         ),
#         media_stream_constraints={"video": True, "audio": False},
#         async_processing=True,
#     )
#     # Process mood queue (non-blocking)
#     try:
#         mood_data = st.session_state.mood_queue.get_nowait()
#         mood = mood_data['mood']
#         confidence = mood_data.get('confidence', 0)
        
#         st.session_state.detected_mood = mood
#         st.session_state.last_mood_time = mood_data['timestamp']
#         st.session_state.detection_count += 1
        
#         # Add to history
#         st.session_state.mood_history.append(mood)
#         if len(st.session_state.mood_history) > 10:
#             st.session_state.mood_history.pop(0)
        
#         # Implement mood debouncing
#         if mood == st.session_state.pending_mood:
#             st.session_state.consecutive_mood_count += 1
#         else:
#             st.session_state.pending_mood = mood
#             st.session_state.consecutive_mood_count = 1
        
#         # Send to Kafka only after consistent mood detection
#         if (st.session_state.consecutive_mood_count >= MOOD_DEBOUNCE_COUNT and 
#             mood != st.session_state.last_sent_mood and
#             producer is not None):
            
#             payload = {
#                 "user_id": st.session_state.user_id,
#                 "action": "mood_update",
#                 "data": {
#                     "mood": mood,
#                     "confidence": confidence,
#                     "timestamp": mood_data['timestamp']
#                 }
#             }
            
#             try:
#                 producer.send(TOPIC_USER_ACTIONS, payload)
#                 producer.flush()
#                 st.session_state.last_sent_mood = mood
#                 st.session_state.playlist_count += 1
#                 logger.info(f"📤 Sent mood to Kafka: {mood}")
#                 st.success(f"🎵 Requesting {mood} playlist...")
#             except Exception as e:
#                 logger.error(f"❌ Failed to send mood: {e}")
#                 st.error("Failed to send mood update")
        
#     except queue.Empty:
#         pass

#     # Display current mood
#     mood_emoji = {
#         'happy': '😊', 'sad': '😢', 'angry': '😠', 
#         'neutral': '😐', 'fear': '😨', 'surprise': '😮', 'disgust': '🤢'
#     }
    
#     if st.session_state.detected_mood:
#         emoji = mood_emoji.get(st.session_state.detected_mood, '🎭')
#         col_a, col_b = st.columns(2)
        
#         with col_a:
#             st.metric(
#                 label="Current Mood",
#                 value=f"{emoji} {st.session_state.detected_mood.upper()}"
#             )
        
#         with col_b:
#             st.metric(
#                 label="Detections",
#                 value=st.session_state.detection_count
#             )
        
#         if st.session_state.last_mood_time:
#             elapsed = int(time.time() - st.session_state.last_mood_time)
#             st.caption(f"Last updated {elapsed}s ago")
#     else:
#         st.info("👀 Waiting for first detection... Start camera above!")
    
#     # Mood history visualization
#     if st.session_state.mood_history:
#         st.markdown("**Recent mood history:**")
#         history_str = " → ".join([
#             mood_emoji.get(m, '🎭') 
#             for m in st.session_state.mood_history[-5:]
#         ])
#         st.text(history_str)

# # -----------------------
# # RIGHT COLUMN - Playlist
# # -----------------------
# with col2:
#     st.subheader("🎵 Dynamic Playlist")
    
#     # Process playlist queue (non-blocking)
#     try:
#         payload = st.session_state.playlist_queue.get_nowait()
#         st.session_state.latest_playlist = payload.get('playlist', [])
#         playlist_mood = payload.get('mood', 'unknown')
#         st.success(f"✨ New playlist for **{playlist_mood}** mood!")
#         logger.info(f"📥 Received playlist: {len(st.session_state.latest_playlist)} tracks")
#     except queue.Empty:
#         pass

#     # Display playlist
#     if st.session_state.latest_playlist:
#         st.info(f"🎼 {len(st.session_state.latest_playlist)} tracks in queue")
        
#         # Create tabs for better organization
#         tab1, tab2 = st.tabs(["📃 Playlist", "▶️ Now Playing"])
        
#         with tab1:
#             for i, track in enumerate(st.session_state.latest_playlist, 1):
#                 title = track.get('title') or track.get('name', 'Unknown Track')
#                 artist = track.get('artist') or track.get('artists', 'Unknown Artist')
#                 yt_url = track.get('youtube_url') or track.get('youtube')
                
#                 if not yt_url:
#                     yt_url = youtube_search_link(f"{title} {artist}")
                
#                 with st.container():
#                     col_num, col_info, col_link = st.columns([0.5, 3, 1])
                    
#                     with col_num:
#                         st.markdown(f"**{i}**")
                    
#                     with col_info:
#                         st.markdown(f"**{title}**")
#                         st.caption(f"by {artist}")
                    
#                     with col_link:
#                         if yt_url:
#                             st.markdown(f"[▶️ Play]({yt_url})")
                    
#                     if i < len(st.session_state.latest_playlist):
#                         st.divider()
        
#         with tab2:
#             # Embed first video
#             first_track = st.session_state.latest_playlist[0]
#             title = first_track.get('title') or first_track.get('name', 'Unknown')
#             artist = first_track.get('artist') or first_track.get('artists', '')
            
#             st.markdown(f"### {title}")
#             if artist:
#                 st.caption(f"by {artist}")
            
#             yt_url = first_track.get('youtube_url') or first_track.get('youtube')
            
#             if yt_url:
#                 video_id = extract_youtube_id(yt_url)
#                 if video_id:
#                     embed_url = f"https://www.youtube.com/embed/{video_id}"
#                     st.video(embed_url)
#                 else:
#                     st.video(yt_url)
#             else:
#                 st.warning("No video available for this track")
#     else:
#         st.info("⏳ Waiting for your first playlist...")
#         st.markdown("""
#         ### 🎯 How it works:
        
#         1. **📹 Start Camera** - Click "START" above to enable webcam
#         2. **🧠 AI Analysis** - Your facial expression is analyzed every 10 seconds
#         3. **🎵 Mood Detection** - System detects emotions (happy, sad, angry, etc.)
#         4. **🎶 Playlist Generation** - Music recommendations match your mood
#         5. **▶️ Auto-Play** - Videos load automatically!
        
#         **Supported Moods:** Happy 😊 | Sad 😢 | Angry 😠 | Neutral 😐 | Fear 😨 | Surprise 😮 | Disgust 🤢
#         """)

# # -----------------------
# # SIDEBAR - Stats & Controls
# # -----------------------
# st.sidebar.header("📊 Session Statistics")

# col_s1, col_s2 = st.sidebar.columns(2)
# with col_s1:
#     st.metric("Detections", st.session_state.detection_count)
# with col_s2:
#     st.metric("Playlists", st.session_state.playlist_count)

# st.sidebar.metric("Tracks in Queue", len(st.session_state.latest_playlist))
# st.sidebar.caption(f"User: {st.session_state.user_id[:12]}...")

# st.sidebar.markdown("---")
# st.sidebar.subheader("🔧 System Status")

# # Kafka status
# if st.session_state.kafka_connected:
#     st.sidebar.success("✅ Kafka Connected")
# else:
#     st.sidebar.error("❌ Kafka Disconnected")

# # Camera status
# if ctx and ctx.state.playing:
#     st.sidebar.success("✅ Camera Active")
# else:
#     st.sidebar.warning("⚠️ Camera Inactive")

# # Consumer status
# if st.session_state.playlist_consumer_thread_started:
#     st.sidebar.success("✅ Consumer Running")
# else:
#     st.sidebar.error("❌ Consumer Not Started")

# st.sidebar.markdown("---")
# st.sidebar.subheader("🧪 Manual Controls")

# # Manual mood override
# test_mood = st.sidebar.selectbox(
#     "Test with mood:",
#     ["happy", "sad", "angry", "neutral", "fear", "surprise", "disgust"]
# )

# if st.sidebar.button("🎭 Send Test Mood", use_container_width=True):
#     if producer is not None:
#         st.session_state.detected_mood = test_mood
#         st.session_state.last_sent_mood = None
        
#         payload = {
#             "user_id": st.session_state.user_id,
#             "action": "mood_update",
#             "data": {
#                 "mood": test_mood,
#                 "confidence": 100,
#                 "timestamp": time.time()
#             }
#         }
        
#         try:
#             producer.send(TOPIC_USER_ACTIONS, payload)
#             producer.flush()
#             st.session_state.playlist_count += 1
#             st.sidebar.success(f"✅ Sent: {test_mood}")
#             logger.info(f"🧪 Manual test: {test_mood}")
#         except Exception as e:
#             st.sidebar.error(f"❌ Failed: {str(e)[:50]}")
#     else:
#         st.sidebar.error("Kafka not connected")

# if st.sidebar.button("🔄 Refresh UI", use_container_width=True):
#     st.rerun()

# if st.sidebar.button("🗑️ Clear Session", use_container_width=True):
#     st.session_state.mood_history = []
#     st.session_state.detection_count = 0
#     st.session_state.playlist_count = 0
#     st.session_state.latest_playlist = []
#     st.session_state.detected_mood = None
#     st.sidebar.success("✅ Session cleared!")
#     time.sleep(1)
#     st.rerun()

# # Footer
# st.sidebar.markdown("---")
# st.sidebar.caption("🎵 Mood Music Player v2.0")
# st.sidebar.caption("Powered by DeepFace & Kafka")











# streamlit_app.py - ENHANCED VERSION with Language & Genre Options
import streamlit as st
import uuid
import json
import threading
import queue
import time
from kafka import KafkaProducer, KafkaConsumer
from streamlit_webrtc import (
    webrtc_streamer, 
    VideoProcessorBase, 
    WebRtcMode,
    RTCConfiguration
)
import av
import cv2
from deepface import DeepFace
from urllib.parse import quote_plus
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------
# Configuration
# -----------------------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_USER_ACTIONS = "user_actions"
TOPIC_USER_PLAYLISTS = "user_playlists"

RTC_CONFIGURATION = RTCConfiguration(
    {"iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]}
)

# Mood detection settings
ANALYSIS_INTERVAL = 10.0  # Analyze every 10 seconds
MOOD_DEBOUNCE_COUNT = 2   # Require 2 consecutive same moods before sending

# Language options
LANGUAGES = {
    "English": "en",
    "Spanish": "es",
    "French": "fr",
    "German": "de",
    "Italian": "it",
    "Portuguese": "pt",
    "Hindi": "hi",
    "Tamil": "ta",
    "Telugu": "te",
    "Korean": "ko",
    "Japanese": "ja",
    "Chinese": "zh",
    "Arabic": "ar",
    "Russian": "ru"
}

# Genre options
GENRES = [
    "Pop",
    "Rock",
    "Hip Hop",
    "Jazz",
    "Classical",
    "Electronic",
    "R&B",
    "Country",
    "Indie",
    "Metal",
    "Folk",
    "Blues",
    "Reggae",
    "Latin",
    "K-Pop",
    "Bollywood",
    "Any Genre"
]

# -----------------------
# Helper Functions
# -----------------------
def youtube_search_link(query):
    """Generate YouTube search URL"""
    if not query:
        return None
    return f"https://www.youtube.com/results?search_query={quote_plus(query)}"

def extract_youtube_id(url):
    """Extract video ID from YouTube URL"""
    if not url:
        return None
    if 'watch?v=' in url:
        return url.split('watch?v=')[1].split('&')[0]
    elif 'youtu.be/' in url:
        return url.split('youtu.be/')[1].split('?')[0]
    return None

# -----------------------
# Initialize Session State
# -----------------------
def initialize_session_state():
    """Initialize all session state variables"""
    defaults = {
        'user_id': str(uuid.uuid4()),
        'detected_mood': None,
        'last_sent_mood': None,
        'latest_playlist': [],
        'playlist_queue': queue.Queue(),
        'mood_queue': queue.Queue(),
        'last_mood_time': None,
        'mood_history': [],
        'detection_count': 0,
        'playlist_count': 0,
        'playlist_consumer_thread_started': False,
        'consecutive_mood_count': 0,
        'pending_mood': None,
        'kafka_connected': False,
        'last_error': None,
        'selected_language': "English",
        'selected_genre': "Any Genre",
        'preferences_updated': False
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

initialize_session_state()

# -----------------------
# Kafka Producer with Error Handling
# -----------------------
@st.cache_resource
def get_kafka_producer():
    """Create Kafka producer with connection retry"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=5000,  # Timeout after 5 seconds
            retries=3
        )
        logger.info("✅ Kafka producer connected")
        return producer
    except Exception as e:
        logger.error(f"❌ Kafka producer failed: {e}")
        return None

producer = get_kafka_producer()
st.session_state.kafka_connected = producer is not None

# -----------------------
# Background Kafka Consumer
# -----------------------
def start_playlist_consumer(user_id, playlist_queue):
    """Background thread to consume playlist updates"""
    logger.info(f"🎧 Starting consumer for user: {user_id}")
    
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                TOPIC_USER_PLAYLISTS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset='latest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info(f"✅ Consumer connected (attempt {attempt + 1})")
            break
        except Exception as e:
            logger.error(f"❌ Consumer connection failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("❌ Consumer failed to start after max retries")
                return
    
    while True:
        try:
            for msg in consumer:
                payload = msg.value
                logger.info(f"📨 Received message: {payload.get('user_id')}")
                
                if payload.get('user_id') == user_id:
                    playlist_queue.put(payload)
                    logger.info(f"✅ Playlist queued for user {user_id}")
            
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}")
            time.sleep(5)

# Start consumer thread (only once)
if not st.session_state.playlist_consumer_thread_started:
    consumer_thread = threading.Thread(
        target=start_playlist_consumer,
        args=(st.session_state.user_id, st.session_state.playlist_queue),
        daemon=True
    )
    consumer_thread.start()
    st.session_state.playlist_consumer_thread_started = True
    logger.info("🚀 Consumer thread started")

# -----------------------
# Video Processor - Emotion Detection
# -----------------------
class DeepFaceProcessor(VideoProcessorBase):
    """Process video frames and detect emotions"""
    
    def __init__(self, user_id, mood_queue):
        self.user_id = user_id
        self.mood_queue = mood_queue
        self.last_analysis_time = 0
        self.analysis_interval = ANALYSIS_INTERVAL
        self.frame_count = 0
        self.detection_count = 0
        self.last_mood = None
        self.analyzing = False

    def recv(self, frame):
        """Process each video frame"""
        img = frame.to_ndarray(format="bgr24")
        current_time = time.time()
        self.frame_count += 1
        
        # Show live indicator
        cv2.putText(img, "LIVE", (10, 30), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)
        
        # Perform analysis at intervals
        time_since_last = current_time - self.last_analysis_time
        
        if time_since_last >= self.analysis_interval and not self.analyzing:
            self.analyzing = True
            
            try:
                # Show analyzing status
                cv2.putText(img, "Analyzing...", (10, 70), 
                           cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 0), 2)
                
                # Resize for faster processing
                small = cv2.resize(img, (320, 240))
                
                # Analyze emotion
                result = DeepFace.analyze(
                    small,
                    actions=['emotion'],
                    enforce_detection=False,
                    detector_backend='opencv',
                    silent=True
                )
                
                res = result[0] if isinstance(result, list) else result
                
                if res and 'dominant_emotion' in res:
                    mood = res['dominant_emotion']
                    confidence = res.get('emotion', {}).get(mood, 0)
                    
                    self.detection_count += 1
                    self.last_analysis_time = current_time
                    self.last_mood = mood
                    
                    # Send to queue
                    self.mood_queue.put({
                        'mood': mood,
                        'confidence': confidence,
                        'timestamp': current_time
                    })
                    
                    logger.info(f"✅ Detected: {mood} ({confidence:.0f}% confidence)")
                
            except Exception as e:
                logger.error(f"❌ DeepFace error: {e}")
            finally:
                self.analyzing = False
        
        # Display current mood and countdown
        if self.last_mood:
            mood_text = f"Mood: {self.last_mood.upper()}"
            cv2.putText(img, mood_text, (10, img.shape[0] - 50), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2)
        
        # Show countdown to next analysis
        next_in = max(0, self.analysis_interval - time_since_last)
        countdown_text = f"Next: {next_in:.0f}s"
        cv2.putText(img, countdown_text, (10, img.shape[0] - 20), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        
        return av.VideoFrame.from_ndarray(img, format="bgr24")

# -----------------------
# Helper function to send mood with preferences
# -----------------------
def send_mood_to_kafka(mood, confidence=100, manual=False):
    """Send mood update to Kafka with user preferences"""
    if producer is None:
        st.error("❌ Kafka not connected")
        return False
    
    payload = {
        "user_id": st.session_state.user_id,
        "action": "mood_update",
        "data": {
            "mood": mood,
            "confidence": confidence,
            "timestamp": time.time(),
            "language": LANGUAGES[st.session_state.selected_language],
            "language_name": st.session_state.selected_language,
            "genre": st.session_state.selected_genre,
            "manual": manual
        }
    }
    
    try:
        producer.send(TOPIC_USER_ACTIONS, payload)
        producer.flush()
        st.session_state.last_sent_mood = mood
        st.session_state.playlist_count += 1
        logger.info(f"Sent mood to Kafka: {mood} | Lang: {st.session_state.selected_language} | Genre: {st.session_state.selected_genre}")
        return True
    except Exception as e:
        logger.error(f"Failed to send mood: {e}")
        st.error(f"Failed to send mood update: {str(e)[:100]}")
        return False

# -----------------------
# Streamlit UI
# -----------------------
st.set_page_config(
    page_title="Real-Time Mood Music", 
    layout="wide",
    page_icon="🎵"
)

st.title("🎵 Real-Time Mood-Based Music Player")
st.caption("🎥 AI analyzes your facial expressions • 🎵 Music adapts to your mood • 🌍 Multi-language support")

# Connection status banner
if not st.session_state.kafka_connected:
    st.error("⚠️ Kafka not connected. Please ensure Kafka is running on localhost:9092")

# -----------------------
# PREFERENCES BAR (Top)
# -----------------------
with st.expander("⚙️ Music Preferences", expanded=False):
    pref_col1, pref_col2, pref_col3 = st.columns([2, 2, 1])
    
    with pref_col1:
        new_language = st.selectbox(
            "🌍 Music Language",
            options=list(LANGUAGES.keys()),
            index=list(LANGUAGES.keys()).index(st.session_state.selected_language),
            help="Select preferred language for music recommendations"
        )
        
        if new_language != st.session_state.selected_language:
            st.session_state.selected_language = new_language
            st.session_state.preferences_updated = True
            st.success(f"Language changed to {new_language}")
    
    with pref_col2:
        new_genre = st.selectbox(
            "🎸 Music Genre",
            options=GENRES,
            index=GENRES.index(st.session_state.selected_genre),
            help="Select preferred music genre"
        )
        
        if new_genre != st.session_state.selected_genre:
            st.session_state.selected_genre = new_genre
            st.session_state.preferences_updated = True
            st.success(f"Genre changed to {new_genre}")
    
    with pref_col3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Apply & Refresh Playlist", use_container_width=True):
            if st.session_state.detected_mood:
                if send_mood_to_kafka(st.session_state.detected_mood, manual=True):
                    st.success("✅ Preferences applied! Generating new playlist...")
            else:
                st.warning("No mood detected yet. Start camera first!")

st.markdown("---")

col1, col2 = st.columns([1, 2])

# -----------------------
# LEFT COLUMN - Camera & Mood Detection
# -----------------------
with col1:
    st.subheader("📹 Live Mood Detection")
    
    # Current preferences display
    pref_display = st.container()
    with pref_display:
        pcol1, pcol2 = st.columns(2)
        with pcol1:
            st.info(f"🌍 **{st.session_state.selected_language}**")
        with pcol2:
            st.info(f"🎸 **{st.session_state.selected_genre}**")
    
    st.caption(f"🔄 Detection runs every {ANALYSIS_INTERVAL:.0f} seconds")
    
    # WebRTC Video Stream
    current_user_id = st.session_state.user_id
    current_mood_queue = st.session_state.mood_queue
    
    ctx = webrtc_streamer(
        key=f"mood_stream_{current_user_id}",
        mode=WebRtcMode.SENDRECV,
        rtc_configuration=RTC_CONFIGURATION,
        video_processor_factory=lambda: DeepFaceProcessor(
            current_user_id,
            current_mood_queue
        ),
        media_stream_constraints={"video": True, "audio": False},
        async_processing=True,
    )

    # Process mood queue (non-blocking)
    try:
        mood_data = st.session_state.mood_queue.get_nowait()
        mood = mood_data['mood']
        confidence = mood_data.get('confidence', 0)
        
        st.session_state.detected_mood = mood
        st.session_state.last_mood_time = mood_data['timestamp']
        st.session_state.detection_count += 1
        
        # Add to history
        st.session_state.mood_history.append(mood)
        if len(st.session_state.mood_history) > 10:
            st.session_state.mood_history.pop(0)
        
        # Implement mood debouncing
        if mood == st.session_state.pending_mood:
            st.session_state.consecutive_mood_count += 1
        else:
            st.session_state.pending_mood = mood
            st.session_state.consecutive_mood_count = 1
        
        # Send to Kafka only after consistent mood detection
        if (st.session_state.consecutive_mood_count >= MOOD_DEBOUNCE_COUNT and 
            mood != st.session_state.last_sent_mood):
            
            if send_mood_to_kafka(mood, confidence):
                st.success(f"🎵 Generating {st.session_state.selected_language} {mood} playlist...")
        
    except queue.Empty:
        pass

    # Display current mood
    mood_emoji = {
        'happy': '😊', 'sad': '😢', 'angry': '😠', 
        'neutral': '😐', 'fear': '😨', 'surprise': '😮', 'disgust': '🤢'
    }
    
    if st.session_state.detected_mood:
        emoji = mood_emoji.get(st.session_state.detected_mood, '🎭')
        col_a, col_b = st.columns(2)
        
        with col_a:
            st.metric(
                label="Current Mood",
                value=f"{emoji} {st.session_state.detected_mood.upper()}"
            )
        
        with col_b:
            st.metric(
                label="Detections",
                value=st.session_state.detection_count
            )
        
        if st.session_state.last_mood_time:
            elapsed = int(time.time() - st.session_state.last_mood_time)
            st.caption(f"Last updated {elapsed}s ago")
    else:
        st.info("👀 Waiting for first detection... Start camera above!")
    
    # Mood history visualization
    if st.session_state.mood_history:
        st.markdown("**Recent mood history:**")
        history_str = " → ".join([
            mood_emoji.get(m, '🎭') 
            for m in st.session_state.mood_history[-5:]
        ])
        st.text(history_str)

# -----------------------
# RIGHT COLUMN - Playlist
# -----------------------
with col2:
    st.subheader("🎵 Dynamic Playlist")
    
    # Process playlist queue (non-blocking)
    try:
        payload = st.session_state.playlist_queue.get_nowait()
        st.session_state.latest_playlist = payload.get('playlist', [])
        playlist_mood = payload.get('mood', 'unknown')
        playlist_language = payload.get('language_name', st.session_state.selected_language)
        playlist_genre = payload.get('genre', st.session_state.selected_genre)
        
        st.success(f"✨ New **{playlist_language}** {playlist_genre} playlist for **{playlist_mood}** mood!")
        logger.info(f"📥 Received playlist: {len(st.session_state.latest_playlist)} tracks")
    except queue.Empty:
        pass

    # Display playlist
    if st.session_state.latest_playlist:
        # Playlist header with info
        info_col1, info_col2, info_col3 = st.columns(3)
        with info_col1:
            st.info(f"🎼 {len(st.session_state.latest_playlist)} tracks")
        with info_col2:
            st.info(f"🌍 {st.session_state.selected_language}")
        with info_col3:
            st.info(f"🎸 {st.session_state.selected_genre}")
        
        # Create tabs for better organization
        tab1, tab2 = st.tabs(["📃 Full Playlist", "▶️ Now Playing"])
        
        with tab1:
            for i, track in enumerate(st.session_state.latest_playlist, 1):
                title = track.get('title') or track.get('name', 'Unknown Track')
                artist = track.get('artist') or track.get('artists', 'Unknown Artist')
                yt_url = track.get('youtube_url') or track.get('youtube')
                
                if not yt_url:
                    yt_url = youtube_search_link(f"{title} {artist}")
                
                with st.container():
                    col_num, col_info, col_link = st.columns([0.5, 3, 1])
                    
                    with col_num:
                        st.markdown(f"**{i}**")
                    
                    with col_info:
                        st.markdown(f"**{title}**")
                        st.caption(f"by {artist}")
                    
                    with col_link:
                        if yt_url:
                            st.markdown(f"[▶️ Play]({yt_url})")
                    
                    if i < len(st.session_state.latest_playlist):
                        st.divider()
        
        with tab2:
            # Embed first video
            first_track = st.session_state.latest_playlist[0]
            title = first_track.get('title') or first_track.get('name', 'Unknown')
            artist = first_track.get('artist') or first_track.get('artists', '')
            
            st.markdown(f"### {title}")
            if artist:
                st.caption(f"by {artist}")
            
            yt_url = first_track.get('youtube_url') or first_track.get('youtube')
            
            if yt_url:
                video_id = extract_youtube_id(yt_url)
                if video_id:
                    embed_url = f"https://www.youtube.com/embed/{video_id}"
                    st.video(embed_url)
                else:
                    st.video(yt_url)
            else:
                st.warning("No video available for this track")
    else:
        st.info("⏳ Waiting for your first playlist...")
        st.markdown("""
        ### 🎯 How it works:
        
        1. **⚙️ Set Preferences** - Choose your language and genre above
        2. **📹 Start Camera** - Click "START" to enable webcam
        3. **🧠 AI Analysis** - Your facial expression is analyzed every 10 seconds
        4. **🎵 Mood Detection** - System detects emotions (happy, sad, angry, etc.)
        5. **🎶 Playlist Generation** - Music matches your mood, language & genre
        6. **▶️ Auto-Play** - Videos load automatically!
        
        **Supported Moods:** Happy 😊 | Sad 😢 | Angry 😠 | Neutral 😐 | Fear 😨 | Surprise 😮 | Disgust 🤢
        
        **Languages:** English, Spanish, French, German, Italian, Portuguese, Hindi, Tamil, Telugu, Korean, Japanese, Chinese, Arabic, Russian
        
        **Genres:** Pop, Rock, Hip Hop, Jazz, Classical, Electronic, R&B, Country, Indie, Metal, Folk, Blues, Reggae, Latin, K-Pop, Bollywood
        """)

# -----------------------
# SIDEBAR - Stats & Controls
# -----------------------
st.sidebar.header("📊 Session Statistics")

col_s1, col_s2 = st.sidebar.columns(2)
with col_s1:
    st.metric("Detections", st.session_state.detection_count)
with col_s2:
    st.metric("Playlists", st.session_state.playlist_count)

st.sidebar.metric("Tracks in Queue", len(st.session_state.latest_playlist))
st.sidebar.caption(f"User: {st.session_state.user_id[:12]}...")

st.sidebar.markdown("---")
st.sidebar.subheader("🔧 System Status")

# Kafka status
if st.session_state.kafka_connected:
    st.sidebar.success("✅ Kafka Connected")
else:
    st.sidebar.error("❌ Kafka Disconnected")

# Camera status
if ctx and ctx.state.playing:
    st.sidebar.success("✅ Camera Active")
else:
    st.sidebar.warning("⚠️ Camera Inactive")

# Consumer status
if st.session_state.playlist_consumer_thread_started:
    st.sidebar.success("✅ Consumer Running")
else:
    st.sidebar.error("❌ Consumer Not Started")

st.sidebar.markdown("---")
st.sidebar.subheader("🧪 Manual Testing")

# Manual mood override with language and genre
test_mood = st.sidebar.selectbox(
    "Test Mood:",
    ["happy", "sad", "angry", "neutral", "fear", "surprise", "disgust"]
)

test_language = st.sidebar.selectbox(
    "Test Language:",
    list(LANGUAGES.keys()),
    index=list(LANGUAGES.keys()).index(st.session_state.selected_language)
)

test_genre = st.sidebar.selectbox(
    "Test Genre:",
    GENRES,
    index=GENRES.index(st.session_state.selected_genre)
)

if st.sidebar.button("🎭 Send Test Request", use_container_width=True):
    # Temporarily update preferences for test
    original_lang = st.session_state.selected_language
    original_genre = st.session_state.selected_genre
    
    st.session_state.selected_language = test_language
    st.session_state.selected_genre = test_genre
    st.session_state.detected_mood = test_mood
    
    if send_mood_to_kafka(test_mood, confidence=100, manual=True):
        st.sidebar.success(f"✅ Test sent!")
        st.sidebar.info(f"Mood: {test_mood}")
        st.sidebar.info(f"Lang: {test_language}")
        st.sidebar.info(f"Genre: {test_genre}")
        logger.info(f"🧪 Manual test: {test_mood} | {test_language} | {test_genre}")
    
    # Restore original preferences if they were different
    if test_language != original_lang or test_genre != original_genre:
        time.sleep(0.5)
        st.session_state.selected_language = original_lang
        st.session_state.selected_genre = original_genre

st.sidebar.markdown("---")

if st.sidebar.button("🔄 Refresh UI", use_container_width=True):
    st.rerun()

if st.sidebar.button("🗑️ Clear Session", use_container_width=True):
    st.session_state.mood_history = []
    st.session_state.detection_count = 0
    st.session_state.playlist_count = 0
    st.session_state.latest_playlist = []
    st.session_state.detected_mood = None
    st.sidebar.success("✅ Session cleared!")
    time.sleep(1)
    st.rerun()

# Footer
st.sidebar.markdown("---")
st.sidebar.caption("🎵 Mood Music Player v2.1")
st.sidebar.caption("Multi-language • Multi-genre")
st.sidebar.caption("Powered by DeepFace & Kafka")