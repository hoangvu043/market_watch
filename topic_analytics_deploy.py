import pandas as pd

import streamlit as st

from streamlit_gsheets import GSheetsConnection
import toml
from influencer_topic.service import InfluencerTopicService
from influencer_topic.utils import InfluencerTopicUltils
from influencer_topic.const import InfluencerTopicConst
import warnings

warnings.filterwarnings('ignore')
st.set_page_config(page_title="Influencer-Topic",
                   page_icon="https://media.licdn.com/dms/image/C560BAQFZlsleSQBzDw/company-logo_200_200/0"
                             "/1593489358194/hiip_asia_logo?e=2147483647&v=beta&t"
                             "=65VpTsit6KpuKtnzYLTOQr3heBQeIUS_OabMhTXt7ag")

st.sidebar.image(
    "https://itviec.com/rails/active_storage/representations/proxy"
    "/eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaHBBMlNQRVE9PSIsImV4cCI6bnVsbCwicHVyIjoiYmxvYl9pZCJ9fQ"
    "==--e5d4bc424d0befa16fef14ed5031047220bba3e6"
    "/eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaDdCem9MWm05eWJXRjBTU0lJY0c1bkJqb0dSVlE2RkhKbGMybDZaVjkwYjE5c2FXMXBkRnNIYVFJc0FXa0NMQUU9IiwiZXhwIjpudWxsLCJwdXIiOiJ2YXJpYXRpb24ifX0=--15c3f2f3e11927673ae52b71712c1f66a7a1b7bd/Logo%20Hiip%20biru%20(2).png")

st.sidebar.title("Influencer Topic")
SOCIAL = st.sidebar.selectbox(
    "Social", ["tt", "ig", "yt"])
COUNTRY_CODE = st.sidebar.selectbox(
    "Country", ["sg", "my", "id", "vn", "th", "ph"])
link_ggsheet = st.sidebar.text_input("Paste link here")
button_apply = st.sidebar.button("Apply")
USER_REQUEST_TOPIC = InfluencerTopicUltils._check_user_request('topic')
st.write('User request hashtag: ' + str(USER_REQUEST_TOPIC))
st.cache_data.clear()
if not link_ggsheet:
    st.write("# Please input post link")
elif button_apply and SOCIAL == "tt":
    try:
        if USER_REQUEST_TOPIC >= InfluencerTopicConst.MAX_REQUEST:
            st.write('Over Quota! please request after 5 minutes!')
        else:
            conn = st.connection("gsheets", type=GSheetsConnection)
            conn.set_default(spreadsheet=link_ggsheet)
            users_info = conn.read(worksheet=f"Content List > Creator")
            users = users_info.dropna(axis=0, how='all')
            # users = users.dropna(axis=0, how='all')
            users = pd.DataFrame(users)
            users_ = users.drop_duplicates(subset="Username", keep="first")
            topics = InfluencerTopicService.run_get_topic(users_, max_count=5000, social="tiktok", country_code=COUNTRY_CODE)
            for d in topics:
                for username, topic in d.items():
                    users.loc[users['Username'] == username, 'Influencer Topics'] = topic

            st.write(users)
            conn.update(worksheet="Content List > Creator", data=users)
            InfluencerTopicUltils._update_user_end_request('topic', USER_REQUEST_TOPIC)
    except Exception as error:
        InfluencerTopicUltils._update_user_end_request('topic', USER_REQUEST_TOPIC)
        st.write(error)

elif button_apply and SOCIAL == "ig":
    try:
        if USER_REQUEST_TOPIC >= InfluencerTopicConst.MAX_REQUEST:
            st.write('Over Quota! please request after 5 minutes!')
        else:
            conn = st.connection("gsheets", type=GSheetsConnection)
            conn.set_default(spreadsheet=link_ggsheet)
            users_info = conn.read(worksheet="Content List > Creator")
            users = users_info.dropna(axis=0, how='all')
            # users = users.dropna(axis=0, how='all')
            users = pd.DataFrame(users)
            users_ = users.drop_duplicates(subset="Username", keep="first")
            st.write(users_)
            topics = InfluencerTopicService.run_get_topic(users_, max_count=5000, social="instagram",country_code=COUNTRY_CODE)
            for d in topics:
                for username, topic in d.items():
                    users.loc[users['Username'] == username, 'Influencer Topics'] = topic

            st.write(users)
            conn.update(worksheet="Content List > Creator", data=users)
            InfluencerTopicUltils._update_user_end_request('topic', USER_REQUEST_TOPIC)
    except Exception as error:
        InfluencerTopicUltils._update_user_end_request('topic', USER_REQUEST_TOPIC)
        st.write(error)

elif button_apply and SOCIAL == "yt":
    try:
        if USER_REQUEST_TOPIC >= InfluencerTopicConst.MAX_REQUEST:
            st.write('Over Quota! please request after 5 minutes!')
        else:
            conn = st.connection("gsheets", type=GSheetsConnection)
            conn.set_default(spreadsheet=link_ggsheet)
            users_info = conn.read(worksheet="Content List > Creator")
            users = users_info.dropna(axis=0, how='all')
            # users = users.dropna(axis=0, how='all')
            users = pd.DataFrame(users)
            users_ = users.drop_duplicates(subset="Username", keep="first")
            st.write(users_)
            topics = InfluencerTopicService.run_get_topic(users_, max_count=5000, social="youtube",country_code=COUNTRY_CODE)
            for d in topics:
                for username, topic in d.items():
                    users.loc[users['Username'] == username, 'Influencer Topics'] = topic

            st.write(users)
            conn.update(worksheet="Content List > Creator", data=users)
            InfluencerTopicUltils._update_user_end_request('topic', USER_REQUEST_TOPIC)
    except Exception as error:
        InfluencerTopicUltils._update_user_end_request('topic', USER_REQUEST_TOPIC)
        st.write(error)

