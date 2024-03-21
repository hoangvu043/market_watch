import requests
import time
import concurrent.futures
from stqdm import stqdm
import json

class InfluencerTopicService:
    def get_topic(username, social, country_code):
        if country_code=="vn":
            country_code = "vi"
        url = "https://1n3e30kq96.execute-api.ap-southeast-1.amazonaws.com/prod/quick-analytics-service/v1"
        if social == "tiktok":
            social_url = f"https://www.tiktok.com/{username}"
        if social == "instagram":
            social_url = f"https://www.instagram.com/{username}"
        if social == "youtube":
            social_url = f"https://www.youtube.com/@{username}"
        payload = json.dumps({
            "social_username": username,
            "country_code": country_code,
            "social_url": social_url,
            "source": "brand_research",
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        data_user = data["data"]["user"]
        main_category = data_user["main_category"]
        topic = {
            username: main_category
        }
        return topic
    def run_get_topic(df, max_count,social=None,country_code=None, display_steps=1000):
        tt = time.time()
        user_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_url = dict()
            cnt = 0
            for _, row in df.iterrows():
                username = row.get("Username")
                if cnt > max_count:
                    break
                future = executor.submit(InfluencerTopicService.get_topic, username,social,country_code)
                future_to_url[future] = (username)
                cnt += 1
            future_iter = concurrent.futures.as_completed(future_to_url)
            total = len(future_to_url)
            for cnt, future in stqdm(enumerate(future_iter), total=total):
                if (cnt + 1) % display_steps == 0:
                    tt1 = time.time()
                    # print(f"{cnt + 1} requests in {tt1 - tt:.3f} seconds")
                    tt = tt1
                username = future_to_url[future]
                try:
                    data = future.result()
                    if data is None:
                        print(username, data, "unknown")
                    else:
                        user_data.append(data)
                except Exception as exc:
                    print(f"{username} + {exc}")
        return user_data

