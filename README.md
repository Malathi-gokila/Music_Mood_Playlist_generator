# 🎵 Music Mood Playlist Generator

> **An AI-powered real-time music recommendation system that adapts to your emotions.**  
> Detects your facial expressions, identifies your mood, and instantly generates a personalized playlist using the **YouTube API**, powered by **DeepFace**, **Kafka**, and **Spark Streaming**.

---

## ✨ Overview

The **Music Mood Playlist Generator** uses **facial emotion recognition** and **real-time data streaming** to recommend music that fits your current mood.  
By combining **computer vision**, **distributed stream processing**, and **machine learning**, this project demonstrates how emotional intelligence can enhance digital experiences.

It integrates:
- 🎥 **DeepFace** for real-time emotion detection  
- ⚡ **Apache Kafka** for asynchronous message communication  
- 🔥 **Apache Spark Structured Streaming** for real-time mood analytics  
- 🎶 **YouTube Data API** for fetching culturally and linguistically relevant songs  

---

## 🎯 Features

- 🧠 Detects seven core facial emotions (happy, sad, angry, neutral, fear, surprise, disgust)  
- 🎧 Generates personalized playlists from **YouTube** in real time  
- 🌍 Supports 14 languages and multiple genres  
- 🔄 Uses **Kafka + Spark** for real-time distributed processing  
- 🔒 Privacy-preserving — no personal data stored or logged  
- 💡 Interactive **Streamlit** interface for live mood visualization  

---

## 🧰 Tech Stack

| Category | Technologies |
|-----------|--------------|
| **Language** | Python |
| **Frontend** | Streamlit |
| **Computer Vision** | DeepFace, OpenCV |
| **Data Streaming** | Apache Kafka |
| **Processing** | Apache Spark (PySpark) |
| **APIs** | YouTube Data API v3 |
| **Others** | Requests, JSON, Pandas |

---

## ⚙️ Installation and Running the Project

To set up and run the project on your system, follow these steps carefully:

```bash
# 1️⃣ Clone the Repository
git clone https://github.com/yourusername/music-mood-playlist-generator.git
cd music-mood-playlist-generator

# 2️⃣ (Optional) Create a Virtual Environment
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate

# 3️⃣ Install Required Libraries
pip install deepface opencv-python streamlit kafka-python pyspark requests

# 4️⃣ Start Apache Kafka (in two terminals)
# Terminal 1 - Start ZooKeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Terminal 2 - Start Kafka Broker
.\bin\windows\kafka-server-start.bat .\config\server.properties
# 💡 For Linux/Mac users, use the .sh scripts inside the bin folder.

# 5️⃣ Launch the Streamlit Application
streamlit run app.py

# 6️⃣ Interact with the App
# - Allow webcam access when prompted
# - Choose preferred language and genre
# - The app detects your facial emotion in real-time
# - A mood-based YouTube playlist will appear instantly 🎶
