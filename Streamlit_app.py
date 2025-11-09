import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# ----------------------------------------------------
# CONNECT TO AZURE COSMOS DB
# ----------------------------------------------------
COSMOS_URI = "mongodb+srv://mongo:Just1234@bigdatatestmongo.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
DB_NAME = "mflixdb"

@st.cache_resource
def get_client():
    return MongoClient(COSMOS_URI)

client = get_client()
db = client[DB_NAME]

# ----------------------------------------------------
# DASHBOARD HEADER
# ----------------------------------------------------
st.title("🎬 Movie Analytics Dashboard (Azure Cosmos DB)")

st.write("""
This dashboard visualizes insights from the **sample_mflix** dataset stored in 
**Azure Cosmos DB for MongoDB vCore**.
""")

# ----------------------------------------------------
# QUICK METRICS
# ----------------------------------------------------
col1, col2, col3 = st.columns(3)

col1.metric("Movies", db.movies.count_documents({}))
col2.metric("Comments", db.comments.count_documents({}))
col3.metric("Users", db.users.count_documents({}))

st.divider()

# ----------------------------------------------------
# GENRE DISTRIBUTION
# ----------------------------------------------------
st.subheader("Genre Distribution")

pipeline = [
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]

df_genre = pd.DataFrame(list(db.movies.aggregate(pipeline)))
df_genre.rename(columns={"_id": "Genre"}, inplace=True)

plt.figure(figsize=(10,4))
plt.bar(df_genre["Genre"], df_genre["count"])
plt.xticks(rotation=90)
plt.title("Movies Per Genre")
st.pyplot(plt)

st.divider()

# ----------------------------------------------------
# AVERAGE RATING BY YEAR (CLEANED)
# ----------------------------------------------------
st.subheader("Average Rating by Year")

pipeline = [
    {"$match": {"imdb.rating": {"$ne": None}, "year": {"$ne": None}}},
    {"$group": {"_id": "$year", "avgRating": {"$avg": "$imdb.rating"}}},
    {"$sort": {"_id": 1}}
]

df_year = pd.DataFrame(list(db.movies.aggregate(pipeline)))
df_year.rename(columns={"_id": "Year"}, inplace=True)

# CLEAN YEAR VALUES
import re

def clean_year(y):
    y = str(y)
    y = re.sub(r"[^0-9]", "", y)        # keep only digits
    if y == "":
        return None
    y = int(y)
    if 1880 <= y <= 2025:              # valid movie year range
        return y
    return None

df_year["Year"] = df_year["Year"].apply(clean_year)
df_year = df_year.dropna(subset=["Year"])

# SORT CLEANED VALUES
df_year = df_year.sort_values("Year")

# NOW SAFE TO PLOT
plt.figure(figsize=(10,4))
plt.plot(df_year["Year"], df_year["avgRating"])
plt.xlabel("Year")
plt.ylabel("Average Rating")
plt.title("IMDb Rating Trend Over Time (Cleaned)")
plt.grid(True)
st.pyplot(plt)

# ----------------------------------------------------
# TOP-RATED MOVIES
# ----------------------------------------------------
st.subheader("Top Rated Movies (IMDb Rating)")

pipeline = [
    {"$project": {"title": 1, "rating": "$imdb.rating"}},
    {"$sort": {"rating": -1}},
    {"$limit": 20}
]

df_top = pd.DataFrame(list(db.movies.aggregate(pipeline)))
st.dataframe(df_top)

st.divider()

# ----------------------------------------------------
# MOST COMMENTED MOVIES
# ----------------------------------------------------
st.subheader("Most Commented Movies")

pipeline = [
    {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 20},
    {"$lookup": {
        "from": "movies",
        "localField": "_id",
        "foreignField": "_id",
        "as": "movie"
    }},
    {"$unwind": "$movie"},
    {"$project": {"title": "$movie.title", "count": 1}}
]

df_comments = pd.DataFrame(list(db.comments.aggregate(pipeline)))
st.dataframe(df_comments)

st.success("Dashboard connected to Azure Cosmos DB successfully!")