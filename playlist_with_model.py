# spark_playlist_aggregate_learning.py
# ML Model learns GLOBAL mood-genre patterns (no user tracking needed)

import json
import os
import random
import traceback
import requests
from datetime import datetime
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
import sys
sys.stdout.reconfigure(encoding='utf-8')

# -----------------------
# Configuration
# -----------------------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_USER_ACTIONS = "user_actions"
TOPIC_USER_PLAYLISTS = "user_playlists"

YOUTUBE_API_KEY = "AIzaSyAJXRPoH7hXGeLLn-zxuetbjrzBCjChDaU"
PLAYLIST_LENGTH = 8
ML_MODEL_PATH = r"C:\tmp\spark\mood_genre_model"
TRAINING_DATA_PATH = r"C:\tmp\spark\global_patterns"

LANGUAGE_REGIONS = {
    "en": "US", "es": "ES", "fr": "FR", "de": "DE", "it": "IT", "pt": "BR",
    "hi": "IN", "ta": "IN", "te": "IN", "ko": "KR", "ja": "JP", "zh": "CN",
    "ar": "AE", "ru": "RU"
}

MOOD_KEYWORDS = {
    "happy": ["upbeat", "cheerful", "joyful", "uplifting", "feel good"],
    "sad": ["melancholic", "emotional", "heartbreak", "slow", "ballad"],
    "angry": ["intense", "aggressive", "powerful", "hard", "energetic"],
    "neutral": ["calm", "ambient", "relaxing", "chill", "smooth"],
    "fear": ["dark", "mysterious", "suspenseful", "dramatic", "intense"],
    "surprise": ["unexpected", "exciting", "dynamic", "surprising", "energetic"],
    "disgust": ["aggressive", "intense", "raw", "powerful", "emotional"]
}

# Initial genre preferences by mood (starting point)
DEFAULT_PREFERENCES = {
    "happy": ["Pop", "Electronic", "Latin", "K-Pop", "Bollywood"],
    "sad": ["Ballad", "Classical", "Blues", "Jazz", "Folk"],
    "angry": ["Metal", "Rock", "Hip Hop"],
    "neutral": ["Jazz", "Classical", "Indie"],
    "fear": ["Classical", "Electronic"],
    "surprise": ["Electronic", "Pop", "Rock"],
    "disgust": ["Metal", "Rock"]
}

# -----------------------
# Kafka producer
# -----------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# -----------------------
# Spark setup
# -----------------------
spark = SparkSession.builder \
    .appName("SparkMoodPlaylistAggregateLearning") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

os.makedirs(TRAINING_DATA_PATH, exist_ok=True)
os.makedirs(ML_MODEL_PATH, exist_ok=True)

# -----------------------
# Aggregate Mood-Genre Learning System
# -----------------------
class GlobalMoodGenreRecommender:
    """
    Learns GLOBAL patterns: Which genres work best for each mood?
    No user tracking needed - learns from ALL interactions
    """
    def __init__(self, spark):
        self.spark = spark
        self.interaction_data = []
        self.genre_scores = {}  # mood -> {genre: score}
        self._load_scores()
        
    def log_interaction(self, mood, genre, confidence, language):
        """Log interaction (no user_id needed!)"""
        self.interaction_data.append({
            "mood": mood,
            "genre": genre,
            "confidence": confidence,
            "language": language,
            "timestamp": datetime.now().isoformat()
        })
        
        # Save periodically
        if len(self.interaction_data) >= 5:
            self._save_interactions()
        self._update_scores()
    
    def _save_interactions(self):
        """Save to parquet"""
        if not self.interaction_data:
            return
        
        df = self.spark.createDataFrame(self.interaction_data)
        output_path = f"{TRAINING_DATA_PATH}/interactions"
        df.write.mode("append").parquet(output_path)
        print(f"Saved {len(self.interaction_data)} interactions")
        self.interaction_data = []
    
    def _update_scores(self):
        """Calculate genre popularity for each mood"""
        try:
            interactions_path = f"{TRAINING_DATA_PATH}/interactions"
            if not os.path.exists(interactions_path):
                return
            
            df = self.spark.read.parquet(interactions_path)
            
            # Aggregate: For each mood-genre pair, calculate average confidence
            agg_df = df.groupBy("mood", "genre").agg(
                avg("confidence").alias("avg_confidence"),
                count("*").alias("interaction_count")
            )
            
            # Calculate weighted score (confidence × frequency)
            agg_df = agg_df.withColumn(
                "score", 
                col("avg_confidence") * col("interaction_count")
            )
            
            # Convert to dictionary
            results = agg_df.collect()
            self.genre_scores = {}
            
            for row in results:
                mood = row["mood"]
                genre = row["genre"]
                score = float(row["score"])
                
                if mood not in self.genre_scores:
                    self.genre_scores[mood] = {}
                self.genre_scores[mood][genre] = score
            
            # Save scores
            scores_path = f"{ML_MODEL_PATH}/genre_scores.json"
            with open(scores_path, 'w') as f:
                json.dump(self.genre_scores, f, indent=2)
            
            print(f"Updated genre scores for {len(self.genre_scores)} moods")
            print(f"   Example: {list(self.genre_scores.keys())[:3]}")
            
        except Exception as e:
            print(f" Score update error: {e}")
    
    def _load_scores(self):
        """Load previously calculated scores"""
        scores_path = f"{ML_MODEL_PATH}/genre_scores.json"
        if os.path.exists(scores_path):
            with open(scores_path, 'r') as f:
                self.genre_scores = json.load(f)
            print(f"Loaded genre scores: {len(self.genre_scores)} moods")
    
    def get_recommendations(self, mood, language, top_n=5):
        """
        Get top genres for a mood based on GLOBAL learning
        No user_id needed!
        """
        # If we have learned data for this mood, use it
        if mood in self.genre_scores:
            mood_genres = self.genre_scores[mood]
            sorted_genres = sorted(
                mood_genres.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            top_genres = [g[0] for g in sorted_genres[:top_n]]
            if not top_genres:
                top_genres = DEFAULT_PREFERENCES.get(mood, ["Pop", "Rock"])
            print(f"ML learned: {mood} → {top_genres[:3]}")
            return top_genres
        
        # Fallback to defaults
        defaults = DEFAULT_PREFERENCES.get(mood, ["Pop", "Rock"])
        print(f"Using defaults: {mood} → {defaults[:3]}")
        return defaults[:top_n]
    
    def get_statistics(self):
        """Get learning statistics"""
        try:
            interactions_path = f"{TRAINING_DATA_PATH}/interactions"
            if not os.path.exists(interactions_path):
                return {"total_interactions": 0}
            
            df = self.spark.read.parquet(interactions_path)
            total = df.count()
            
            mood_counts = df.groupBy("mood").count().collect()
            genre_counts = df.groupBy("genre").count().collect()
            
            return {
                "total_interactions": total,
                "moods_learned": len(mood_counts),
                "genres_seen": len(genre_counts),
                "top_mood": max(mood_counts, key=lambda x: x[1])["mood"] if mood_counts else None,
                "top_genre": max(genre_counts, key=lambda x: x[1])["genre"] if genre_counts else None
            }
        except:
            return {"total_interactions": 0}

# Initialize recommender
recommender = GlobalMoodGenreRecommender(spark)

# -----------------------
# YouTube search helper
# -----------------------
def youtube_search_tracks(mood, language="en", language_name="English", genre="Any Genre", limit=20):
    """Search YouTube videos"""
    if not mood:
        return []
    
    query_parts = []
    mood_keywords = MOOD_KEYWORDS.get(mood, [mood])
    primary_mood = random.choice(mood_keywords) if mood_keywords else mood
    query_parts.append(primary_mood)
    
    if genre and genre != "Any Genre":
        query_parts.append(genre)
    
    if language != "en":
        query_parts.append(language_name)
    
    query_parts.append("songs music")
    search_query = " ".join(query_parts)
    region_code = LANGUAGE_REGIONS.get(language, "US")
    
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": search_query,
        "type": "video",
        "videoCategoryId": "10",
        "maxResults": limit,
        "key": YOUTUBE_API_KEY,
        "regionCode": region_code,
        "relevanceLanguage": language,
        "order": "relevance"
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        tracks = []

        for item in data.get("items", []):
            snippet = item.get('snippet', {})
            title = snippet.get('title', 'Unknown Title')
            channel = snippet.get('channelTitle', 'Unknown Artist')
            video_id = item.get('id', {}).get('videoId')
            
            if video_id:
                yt_url = f"https://www.youtube.com/watch?v={video_id}"
                tracks.append({
                    "title": title,
                    "artist": channel,
                    "youtube_url": yt_url
                })

        return tracks

    except Exception as e:
        print(f"YouTube error: {e}")
        return []

# -----------------------
# Kafka stream setup
# -----------------------
kafka_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_USER_ACTIONS) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

data_schema = StructType([
    StructField("song", StringType(), True),
    StructField("mood", StringType(), True),
    StructField("confidence", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("language", StringType(), True),
    StructField("language_name", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("manual", BooleanType(), True)
])

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("data", data_schema)
])

actions_df = kafka_stream_df.select(
    from_json(col("value").cast("string"), schema).alias("payload")
).select(
    col("payload.user_id").alias("user_id"),
    col("payload.action").alias("action"),
    col("payload.data.song").alias("song"),
    col("payload.data.mood").alias("mood"),
    col("payload.data.confidence").alias("confidence"),
    col("payload.data.language").alias("language"),
    col("payload.data.language_name").alias("language_name"),
    col("payload.data.genre").alias("genre"),
    col("payload.data.manual").alias("manual")
)

# -----------------------
# Batch handler
# -----------------------
batch_counter = 0

def handle_batch(batch_df, batch_id):
    global batch_counter
    batch_counter += 1
    
    if batch_df.rdd.isEmpty():
        return

    rows = batch_df.collect()
    print(f"\n{'='*70}")
    print(f"[BATCH {batch_id}] Processing {len(rows)} action(s)")
    print(f"{'='*70}")

    for r in rows:
        try:
            user_id = r['user_id']
            action = r['action']
            mood = r['mood']
            language = r['language'] or "en"
            language_name = r['language_name'] or "English"
            genre = r['genre'] or "Any Genre"
            confidence = r['confidence'] or 0
            
            print(f"\nRequest: {mood} | {language_name} | {genre}")

            playlist_items = []

            if action == "mood_update" and mood:
                # Get globally learned genre recommendations
                recommended_genres = recommender.get_recommendations(
                    mood, language, top_n=5
                )
                if not recommended_genres:
                    recommended_genres = DEFAULT_PREFERENCES.get(mood, ["Pop", "Rock"])
                
                # Use primary genre
                primary_genre = genre if genre != "Any Genre" else recommended_genres[0]
                
                print(f"Using: {primary_genre}")
                print(f"Global learning suggests: {', '.join(recommended_genres[:3])}")
                
                # Search YouTube
                tracks = youtube_search_tracks(
                    mood=mood,
                    language=language,
                    language_name=language_name,
                    genre=primary_genre,
                    limit=25
                )
                
                # Try alternatives if needed
                if len(tracks) < PLAYLIST_LENGTH:
                    for alt_genre in recommended_genres[1:3]:
                        alt_tracks = youtube_search_tracks(
                            mood=mood, language=language,
                            language_name=language_name,
                            genre=alt_genre, limit=15
                        )
                        tracks.extend(alt_tracks)
                        if len(tracks) >= PLAYLIST_LENGTH:
                            break
                
                if tracks:
                    random.shuffle(tracks)
                    playlist_items = tracks[:PLAYLIST_LENGTH]
                    
                    # Log for GLOBAL learning (no user_id!)
                    recommender.log_interaction(
                        mood=mood,
                        genre=primary_genre,
                        confidence=confidence,
                        language=language
                    )
                    print(f"Playlist: {len(playlist_items)} tracks")

            # Publish
            if playlist_items:
                out_payload = {
                    "user_id": user_id,
                    "source": action,
                    "mood": mood,
                    "language": language,
                    "language_name": language_name,
                    "genre": genre,
                    "confidence": confidence,
                    "playlist": playlist_items,
                    "ml_recommendations": recommended_genres if action == "mood_update" else []
                }
                
                producer.send(TOPIC_USER_PLAYLISTS, out_payload)
                producer.flush()
                print(f"Published")

        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()
    
    # Show statistics every 10 batches
    if batch_counter % 10 == 0:
        stats = recommender.get_statistics()
        print(f"\n LEARNING STATS:")
        print(f"   Total interactions: {stats.get('total_interactions', 0)}")
        print(f"   Moods learned: {stats.get('moods_learned', 0)}")
        print(f"   Most popular mood: {stats.get('top_mood', 'N/A')}")
        print(f"   Most popular genre: {stats.get('top_genre', 'N/A')}")
    
    print(f"\n{'='*70}\n")

# -----------------------
# Start streaming
# -----------------------
CHECKPOINT_DIR = r"C:\tmp\spark\mood_aggregate_ckpt"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

query = actions_df.writeStream \
    .foreachBatch(handle_batch) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime="2 seconds") \
    .start()

print("\n" + "="*70)
print("Spark Playlist Generator - GLOBAL Aggregate Learning")
print("="*70)
print("ML Model: Learns GLOBAL mood-genre patterns")
print("No user tracking needed (privacy-friendly)")
print("Learns from ALL interactions collectively")
print("Example: 'Happy mood -> 80% prefer Pop, 60% like Bollywood'")
print("="*70 + "\n")

query.awaitTermination()