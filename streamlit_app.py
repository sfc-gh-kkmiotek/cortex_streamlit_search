import streamlit as st
from snowflake.core import Root
from snowflake.snowpark import Session

from itertools import chain, zip_longest

CONNECTION_PARAMETERS = st.secrets['connection']
st.set_page_config(layout="wide")


@st.cache_resource()
def get_root():
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    return Root(session)

root = get_root()

@st.cache_resource()
def get_service(name):
    return root\
    .databases["search_index"]\
    .schemas["public"]\
    .cortex_search_services[name]

version = st.secrets['search']['version']
search_service_popular = get_service(f"search_service_popular_{version}")
search_service_unpopular = get_service(f"search_service_un_popular_{version}")

def serialize_batch(batch):
    for col, result in zip(st.columns(len(batch)), batch):
        with col:
            serialize(result)


def serialize(result):
    app_id = result['app_id']
    image_url = f"https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/{app_id}/Raw_App_Screenshot.png?nf_resize=smartcrop&w=480&h=260"
    with st.container():
        st.subheader(result['title'])
        st.image(image_url)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.text(f"views 👀: {result['unique_views']}")
        with col2: 
            st.text(f"app_id 🆔: {result['app_id']}")
        with col3:
            st.text(f"owner 👑: {result['owner']}")

def search(query, filters=None, order_by="default"):
    resp_popular = search_service_popular.search(
        query=query,
        filter=filters,
        columns=["app_id", "title", "unique_views", "owner"],
        limit=20
    )

    resp_unpopular = search_service_unpopular.search(
        query=query,
        filter=filters,
        columns=["app_id", "title", "unique_views", "owner"],
        limit=10
    )
    
    if order_by == 'relevancy':
        print(resp_popular.results)
        return [x for x in chain.from_iterable(zip_longest(resp_popular.results, resp_unpopular.results)) if x is not None]
    if order_by == 'default':
        return resp_popular.results + resp_unpopular.results
    if order_by == 'unique_views':
        return sorted(resp_popular.results + resp_unpopular.results, key=lambda x: -int(x['unique_views']))
        
def deduplicate(results):
    app_ids = set()
    return [x for x in results if x['app_id'] not in app_ids and not app_ids.add(x['app_id'])]


def batch(iterable, batch_size=3):
    l = len(iterable)
    for ndx in range(0, l, batch_size):
        yield iterable[ndx:min(ndx + batch_size, l)]

    
st.title("Cortex based search engine")
query = st.text_input("Search", "st.chat")
with st.expander("Advanced options", expanded=True):
    col1, col2, col3, col4 = st.columns(4)
    with col1: 
        components = st.multiselect("Streamlit components", ["table", 'chat_message'], None)
    with col2:
        dependencies = st.multiselect("Python dependencies", ['sklearn', 'openai', 'pandas'], None)
    with col3:
        owner = st.text_input("Owner", None)
    with col4:
        order_by = st.selectbox("Order by", ["default", "unique_views", 'relevancy'], 1)
        
    filters = None
    and_filters = []
    if dependencies:
        and_filters.append({"@and": [{"@contains": {"DEPENDENCIES": c}} for c in dependencies]})
    if components:
        and_filters.append({"@and": [{"@contains": {"COMPONENTS": c}} for c in components]})
    if owner:
        and_filters.append({"@eq": {"OWNER": owner}})
    if and_filters:
        filters = {"@and":and_filters}
        

results = search(query, filters, order_by)
results = deduplicate(results)

for b in batch(results):
    serialize_batch(b)
