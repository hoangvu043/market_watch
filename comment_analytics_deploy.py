import pandas as pd
import streamlit as st
from streamlit_gsheets import GSheetsConnection
import toml
from datetime import datetime
import warnings
from comment.service import CommentServiceTikTok, CommentServiceInstagram, CommentServiceYoutube, CommentServiceAnalyze
warnings.filterwarnings('ignore')
st.set_page_config(page_title="Comment-Analyze",
                   page_icon="https://media.licdn.com/dms/image/C560BAQFZlsleSQBzDw/company-logo_200_200/0"
                             "/1593489358194/hiip_asia_logo?e=2147483647&v=beta&t"
                             "=65VpTsit6KpuKtnzYLTOQr3heBQeIUS_OabMhTXt7ag")

st.sidebar.image(
    "https://itviec.com/rails/active_storage/representations/proxy"
    "/eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaHBBMlNQRVE9PSIsImV4cCI6bnVsbCwicHVyIjoiYmxvYl9pZCJ9fQ"
    "==--e5d4bc424d0befa16fef14ed5031047220bba3e6"
    "/eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaDdCem9MWm05eWJXRjBTU0lJY0c1bkJqb0dSVlE2RkhKbGMybDZaVjkwYjE5c2FXMXBkRnNIYVFJc0FXa0NMQUU9IiwiZXhwIjpudWxsLCJwdXIiOiJ2YXJpYXRpb24ifX0=--15c3f2f3e11927673ae52b71712c1f66a7a1b7bd/Logo%20Hiip%20biru%20(2).png")

st.sidebar.title("Collecting comments")
SOCIAL = st.sidebar.selectbox(
    "Social", ["tt", "ig", "yt"])
link_ggsheet = st.sidebar.text_input("Paste link here")
button_apply = st.sidebar.button("Apply")

st.sidebar.title("Analyzing comments")
button_analyze = st.sidebar.button("Analyze")
num_cluster = int(st.sidebar.text_input("Num Cluster") or 0)

SHEET_COMMENT = "Comment List"
SHEET_POST_LINK = "Post Link"
api_key_yt = "AIzaSyDnNWDRHkUJOKJdoLplP-nfiXoLVZSUX-A"
if not link_ggsheet:
    st.write("# Please input post link")
elif button_apply and SOCIAL == "tt":
    conn = st.connection("gsheets", type=GSheetsConnection)
    conn.set_default(spreadsheet=link_ggsheet)
    post_list = conn.read(worksheet=SHEET_POST_LINK)
    post_list = post_list.dropna(axis=1, how='all')
    post_list = post_list.dropna(axis=0, how='all')
    post_list = pd.DataFrame(post_list)
    post_list = post_list.Post_link.to_list()
    st.write(post_list)
    data_full = []
    for post in post_list:
        try:
            comments = CommentServiceTikTok.get_comment_list(CommentServiceTikTok.extract_post_id(post))
            # comments = get_comment_list(post)
            caption = CommentServiceTikTok.get_post_info(CommentServiceTikTok.extract_post_id(post))
        except:
            continue
        for comment in comments:
            for cmt in comment:
                info = {
                    "post_link": post,
                    "caption": caption,
                    "cmt_id": cmt.get("id"),
                    "message": cmt.get("text"),
                    "create_time": datetime.utcfromtimestamp(cmt.get("create_time")),
                    # "style":"HCP",
                    "num_like_cmt": cmt.get("digg_count"),
                }
                data_full.append(info)
    df = pd.DataFrame(data_full)
    df_drop = df.drop_duplicates(subset="cmt_id", keep="first")
    conn.create(worksheet=SHEET_COMMENT, data=df_drop)  
    st.write("Comment has been collected.")
    st.cache_data(ttl=0)
elif button_apply and SOCIAL == "ig":

    conn = st.connection("gsheets", type=GSheetsConnection)
    conn.set_default(spreadsheet=link_ggsheet)
    post_list = conn.read(worksheet=SHEET_POST_LINK)
    post_list = post_list.dropna(axis=1, how='all')
    post_list = post_list.dropna(axis=0, how='all')
    post_list = pd.DataFrame(post_list)
    st.write(post_list)
    post_list_ = post_list.Post_link.to_list()
    st.write(post_list_)
    comment_list = CommentServiceInstagram.run_get_comment(post_list)
    df = pd.DataFrame(comment_list)
    df_drop = df.drop_duplicates(subset="cmt_id", keep="first")
    conn.create(worksheet=SHEET_COMMENT, data=df_drop)
    st.write("Comment has been collected.")
elif button_apply and SOCIAL == "yt":

    conn = st.connection("gsheets", type=GSheetsConnection)
    conn.set_default(spreadsheet=link_ggsheet)
    post_list = conn.read(worksheet=SHEET_POST_LINK)
    post_list = post_list.dropna(axis=1, how='all')
    post_list = post_list.dropna(axis=0, how='all')
    post_list_ = post_list.Post_link.to_list()
    st.write(post_list_)
    post_list = pd.DataFrame(post_list)
    comment_list = CommentServiceYoutube.get_comment_list(post_list)
    comment_full = []
    for comment in comment_list:
        items = comment["items"]
        for i in items:
            video_id=i["snippet"]["topLevelComment"]["snippet"]["videoId"],
            comment_info = {
                "video_id": video_id,
                "post_link":f"https://www.youtube.com/watch?v={video_id}",
                "message": i["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                "username": i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
            }
            comment_full.append(comment_info)
    df_comment = pd.DataFrame(comment_full)
    # df_drop = df_comment.drop_duplicates(subset="cmt_id", keep="first")
    conn.create(worksheet=SHEET_COMMENT, data=df_comment)
    st.write("Comment has been collected.")
elif button_analyze and num_cluster == 0:
    conn = st.connection("gsheets", type=GSheetsConnection)
    conn.set_default(spreadsheet=link_ggsheet)
    comment_list = conn.read(worksheet=SHEET_COMMENT)
    comment_list = comment_list.dropna(axis=1, how='all')
    comment_list = comment_list.dropna(axis=0, how='all')
    st.write(comment_list)
    df = pd.DataFrame(comment_list)
    df_cluster = CommentServiceAnalyze.cluster(df)
    st.write(df_cluster)
elif button_analyze and num_cluster != 0:
    conn = st.connection("gsheets", type=GSheetsConnection)
    conn.set_default(spreadsheet=link_ggsheet)
    comment_list = conn.read(worksheet=SHEET_COMMENT)
    comment_list = comment_list.dropna(axis=1, how='all')
    comment_list = comment_list.dropna(axis=0, how='all')
    df = pd.DataFrame(comment_list)
    df_cluster, df_word_all = CommentServiceAnalyze.common_keyword(df,num_cluster)
    conn.create(worksheet="cluster", data=df_cluster)
    conn.create(worksheet="common_keyword", data=df_word_all)
    st.write("Comment has been collected")

