import requests
import re
from comment.const import CommentConstant
import time
import concurrent.futures
from stqdm import stqdm
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN
from gensim.models import Phrases
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import KMeans
from sklearn.pipeline import make_pipeline
from emoji import UNICODE_EMOJI
from gensim.models.phrases import Phraser
import string
class CommentServiceTikTok():
    def get_post_info(post_id):
        url = "https://tiktok-video-no-watermark2.p.rapidapi.com/"

        querystring = {"url": f"https://www.tiktok.com/@tiktok/video/{post_id}", "hd": "1"}

        headers = CommentConstant.HEADER_TT

        response = requests.get(url, headers=headers, params=querystring)

        data = response.json()["data"]["title"]
        return data


    def get_comment_list(post_id):
        cursor = 0
        comment_list = []
        while True:
            url = "https://tiktok-video-no-watermark2.p.rapidapi.com/comment/list"

            querystring = {"url": f"https://www.tiktok.com/@tiktok/video/{post_id}", "count": "50", "cursor": cursor}

            headers = CommentConstant.HEADER_TT

            response = requests.get(url, headers=headers, params=querystring)
            data = response.json()["data"]
            # print(data)
            cursor = data["cursor"]
            comments = data["comments"]

            comment_list.append(comments)
            # if len(comment_list) > 4:
            #     break
            if data["hasMore"] != True:
                break
        return comment_list


    def extract_post_id(link):
        post_id_pattern = r"/video/(\d+)"
        match = re.search(post_id_pattern, link)
        if match:
            post_id = match.group(1)
            print(post_id)
            return post_id, link
        else:
            return None
class CommentServiceInstagram():
    def extract_post_id(instagram_link):
        pattern = r"\/p\/([a-zA-Z0-9_]+)"
        match = re.search(pattern, instagram_link)
        
        if match:
            return match.group(1)
        else:
            return None
    def get_post_detail(shortcode):
        url = f"https://instagram243.p.rapidapi.com/postdetail/{shortcode}"

        headers = CommentConstant.HEADER_IG

        response = requests.get(url, headers=headers)

        data = response.json()
        return data


    def get_comment(shortcode):
        cursor = "%7Bend_cursor%7D"
        scraperid = "%7Bscraperid%7D"
        data_full = []
        while True:
            url = f"https://instagram243.p.rapidapi.com/postcomments/{shortcode}/{cursor}/{scraperid}"
            headers = CommentConstant.HEADER_IG

            response = requests.get(url, headers=headers)

            data = response.json()
            try:
                cursor = data["data"]['end_cursor']
                # scraperid = data["scraperid"]
                data_full.append(data)
                # print(cursor)
            except:
                return data_full
            if data["data"]["next_page"] != True:
                break
        return data_full


    def run_get_comment(post_link):
        shortcode = CommentServiceInstagram.extract_post_id(post_link)
        caption = CommentServiceInstagram.get_post_detail(shortcode)["data"]["caption"]["text"]
        post_id = CommentServiceInstagram.get_post_detail(shortcode)["data"]["code"]
        comments = CommentServiceInstagram.get_comment(shortcode)
        data_full = [
            {
                "shortcode": post_id,
                "post_link":f"www.instagram.com/p/{post_id}",
                "caption": caption,
                "cmt_id": i.get("pk"),
                "message": i.get("text")
            }
            for comment in comments
            for i in comment["data"]["comments"]
        ]

        return data_full


    def run_get_comment_(df, max_count, display_steps=1000):
        tt = time.time()
        comment_list = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_url = dict()
            cnt = 0
            for _, row in df.iterrows():
                shortcode = row.get("Post_link")
                if cnt > max_count:
                    break
                future = executor.submit(CommentServiceInstagram.run_get_comment, shortcode)
                future_to_url[future] = (shortcode)
                cnt += 1
            future_iter = concurrent.futures.as_completed(future_to_url)
            total = len(future_to_url)
            for cnt, future in stqdm(enumerate(future_iter), total=total):
                if (cnt + 1) % display_steps == 0:
                    tt1 = time.time()
                    # print(f"{cnt + 1} requests in {tt1 - tt:.3f} seconds")
                    tt = tt1
                shortcode = future_to_url[future]
                try:
                    data = future.result()
                    if data is None:
                        print(shortcode, data, "unknown")
                    # elif type(data) == str:
                    #     if "user_id is invalid" in data:
                    #         USERS[itemid] = True
                    #     else:
                    #         print(itemid, data)
                    else:
                        comment_list.extend(data)
                except Exception as exc:
                    print(f"{shortcode} generated an exception: {exc}")
        return comment_list

class CommentServiceYoutube():
    def extract_video_id(youtube_link):
        pattern = r"watch\?v=([a-zA-Z0-9_-]+)"
        match = re.search(pattern, youtube_link)
        
        if match:
            return match.group(1)
        else:
            return None
    def get_comment_list(post_link):
        pageToken=""
        data_full=[]
        video_id = CommentServiceYoutube.extract_video_id(post_link)
        while True:

            url = f"https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&maxResults=100&order=time&pageToken={pageToken}&videoId={video_id}&key={CommentConstant.api_key_yt}"

            payload = {}
            headers = {
            'Authorization': '406107703174-313l2eakv9um8bgip8a0vut21ql4f3bb.apps.googleusercontent.com',
            'Accept': 'application/json'
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            data=response.json()
            
            data_full.append(data)
            try:
                pageToken=data["nextPageToken"]
            except:
                break
            if 'error' in data.keys():
                break
            if 'items' not in data.keys():
                break
        return data_full



    def run_get_comment_list(df, max_count, display_steps=1000):
        tt = time.time()
        collect_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_url = dict()
            cnt = 0
            for _, row in df.iterrows():
                video_id = row.get("Post_link")
                if cnt > max_count:
                    break
                future = executor.submit(CommentServiceYoutube.get_comment_list, video_id)
                future_to_url[future] = (video_id)
                cnt += 1
            future_iter = concurrent.futures.as_completed(future_to_url)
            total = len(future_to_url)
            for cnt, future in stqdm(enumerate(future_iter), total=total):
                if (cnt + 1) % display_steps == 0:
                    tt1 = time.time()
                    # print(f"{cnt + 1} requests in {tt1 - tt:.3f} seconds")
                    tt = tt1
                video_id = future_to_url[future]
                try:
                    data = future.result()
                    if data is None:
                        print(video_id, data, "unknown")
                    # elif type(data) == str:
                    #     if "user_id is invalid" in data:
                    #         USERS[itemid] = True
                    #     else:
                    #         print(itemid, data)
                    else:
                        collect_data.extend(data)
                except Exception as exc:
                    print(f"{video_id} + {exc}")
        return collect_data

class CommentServiceAnalyze():
    def cluster(df):
        string_list = np.array(df.message.dropna().astype(str).apply(lambda x: str(x).lower())).tolist()

        # Sample strings
        strings = list(string_list)

        # Step 1: Preprocess the data
        # You may want to implement a more sophisticated preprocessing based on your specific requirements.

        # Step 2: Calculate TF-IDF
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(strings)

        # Step 3: Apply DBSCAN clustering
        eps = 0.5  # @param {type:"raw"}
        min_samples = 3  # @param
        dbscan = DBSCAN(metric='cosine', eps=0.5, min_samples=3)
        cluster_labels = dbscan.fit_predict(tfidf_matrix)

        # Step 4: Organize strings into clusters
        clustered_strings = {}
        for i, cluster_id in enumerate(cluster_labels):
            if cluster_id not in clustered_strings:
                clustered_strings[cluster_id] = [strings[i]]
            else:
                clustered_strings[cluster_id].append(strings[i])

        # Step 5: Print the clustered strings
        dict_df = {'group': [], 'string': [], 'num_comment': []}
        print("Clustered Strings:")
        for cluster_id, strings_in_cluster in clustered_strings.items():
            if cluster_id == -1:
                print(f"Noise Cluster:")
            else:
                print(f"Cluster {cluster_id + 1}:")
            for string in strings_in_cluster:
                print(f"  - {string}")
                dict_df['group'].append(f"Noise Cluster:" if cluster_id == -1 else f"Cluster {cluster_id + 1}")
                dict_df['string'].append(string)
                dict_df['num_comment'].append(len(strings_in_cluster))
        df_cluster = pd.DataFrame(dict_df)
        return df_cluster
    def common_keyword(df,num_cluster):
        texts = np.array(df.message.apply(lambda x: str(x).lower())).tolist()

        # Load sample data
        # newsgroups = fetch_20newsgroups(subset='all', remove=('headers', 'footers', 'quotes'))

        # Create a pipeline with TfidfVectorizer, TruncatedSVD, and KMeans
        num_clusters = num_cluster  # @param {type:"integer"}
        # vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
        vectorizer = TfidfVectorizer()
        lsa_model = TruncatedSVD(n_components=int(float(num_clusters)), random_state=42)
        kmeans_model = KMeans(n_clusters=num_clusters, random_state=42)

        pipeline = make_pipeline(vectorizer, lsa_model, kmeans_model)

        # Fit the pipeline to the data
        pipeline.fit(texts)

        # Assign cluster labels to documents
        labels = pipeline.predict(texts)

        # Print the cluster labels for each document
        # for i, label in enumerate(labels):
        #     print(f"Document {i+1}: Cluster {label + 1}")

        group2num_comment = dict()
        for label_ in set(labels):
            group2num_comment[label_] = (np.array(labels) == label_).sum()

        def replace_emoji(s):
            count = 0
            for emoji in UNICODE_EMOJI['en']:
                s = s.replace(emoji, "")
            exclude = set(string.punctuation)
            s = ''.join(ch for ch in s if ch not in exclude)
            return s

        dict_df = {'group': [], 'string': [], 'num_comment': []}
        for text, label in zip(texts, labels):
            print(f"  - {string}")
            dict_df['group'].append(f"Cluster {label + 1}")
            dict_df['string'].append(text)
            dict_df['num_comment'].append(group2num_comment[label])
        df_cluster = pd.DataFrame(dict_df)
        df_comment = df_cluster.copy(True)  # pd.read_excel(cluster_output_file)

        df_comment['string'] = df_comment.string.apply(lambda x: replace_emoji(
            str(x).lower().replace(",", " ").replace(".", " ").replace("?", " ").replace("!", " ").replace("\\",
                                                                                                           " ").replace(
                "/",
                " ")) if not pd.isna(
            x) else x)

        df_word_all = None

        for group in set(np.array(df_comment.group).tolist()):

            df_ = df_comment[df_comment.group == group]

            string_list = np.array(df_.string.dropna().astype(str).apply(lambda x: str(x).lower())).tolist()

            print(group, len(df_), len(string_list))

            documents = string_list

            sentence_stream = [str(doc).lower().split() for doc in documents]

            bigram = Phrases(sentence_stream, min_count=1, threshold=10, delimiter='_')

            bigram_phraser = Phraser(bigram)

            trigram_phraser = Phrases(bigram_phraser[sentence_stream], min_count=1, threshold=50, delimiter='_')

            vocab = dict()

            for sent in sentence_stream:
                tokens_ = bigram_phraser[sent]
                tokens__ = trigram_phraser[tokens_]

                # print(sent, tokens_, tokens__)
                for token in sent + tokens_ + tokens__:
                    vocab[token] = 0

            for word in vocab.keys():
                count = df_comment.string.apply(
                    lambda x: word in str(x).lower() or word.replace("_", " ") in str(x).replace("_",
                                                                                                 " ").lower()).sum()
                vocab[word] = count

            sorted_keys = sorted(vocab, key=lambda x: vocab[x])
            data = {'word': [], 'count': [], 'num_word': [], 'num_charater': []}

            for key in sorted_keys:
                # if vocab[key] > 10 and len(key) > 2:
                if vocab[key] > 0:
                    data['word'].append(key)
                    data['count'].append(vocab[key])
                    data['num_charater'].append(len(key))
                    data['num_word'].append(np.array([x == '_' for x in key]).sum() + 1)

            df_word = pd.DataFrame(data)
            df_word['group'] = group

            df_word_all = df_word if df_word_all is None else pd.concat((df_word_all, df_word))
        return df_cluster, df_word_all
