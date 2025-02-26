import streamlit as st
from snowflake.core import Root
from snowflake.snowpark import Session

from itertools import chain, zip_longest

CONNECTION_PARAMETERS = st.secrets['connection']
st.set_page_config(layout="wide")

def load_options(file_name):
    options = []
    with open(f"{file_name}.tsv") as f:
        for line in f.read().splitlines():
            name, usage = line.split()
            options.append([name.replace('"', ''), usage])
    return options


component_options = load_options('components')
deps_options = load_options('dependencies')

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
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.text(f"views 👀: {result['unique_views']}")
        with col2: 
            st.text(f"app_id 🆔: {result['app_id']}")
        with col3:
            st.text(f"owner 👑: {result['owner']}")
        with col4: 
            st.text(f"relevancy: {result['relevancy_score']}")
    
def search(query, filters=None, order_by="relevancy+views"):
    resp_popular = search_service_popular.search(
        query=query,
        filter=filters,
        columns=["app_id", "title", "unique_views", "owner"],
        limit=number_of_results
    )

    resp_unpopular = search_service_unpopular.search(
        query=query,
        filter=filters,
        columns=["app_id", "title", "unique_views", "owner"],
        limit=number_of_results//2
    )
    
    relevancy = [x for x in chain.from_iterable(zip_longest(resp_popular.results, resp_unpopular.results)) if x is not None]
    relevancy = [{**result, "relevancy_score": (len(relevancy) - i) / len(relevancy)} for i, result in enumerate(relevancy)]
    
    if order_by == 'relevancy':
        return relevancy
    if order_by == 'relevancy+views':
        relevancy = [{**r, "relevancy_score": (int(r['unique_views'])) * (r['relevancy_score']) **(1/(boost_views/15 + 1))} for r in relevancy]
        return sorted(relevancy, key=lambda x: -int(x['relevancy_score']))
    if order_by == 'unique_views':
        return sorted(relevancy, key=lambda x: -int(x['unique_views']))
        
def deduplicate(results):
    app_ids = set()
    return [x for x in results if x['app_id'] not in app_ids and not app_ids.add(x['app_id'])]


def post_filter(results, minimum_views):
    return [x for x in results if int(x['unique_views']) >= minimum_views]
    

def batch(iterable, batch_size=3):
    l = len(iterable)
    for ndx in range(0, l, batch_size):
        yield iterable[ndx:min(ndx + batch_size, l)]

    
st.title("Cortex based search engine")
query = st.text_input("Search", "st.chat")
with st.expander("Advanced options", expanded=True):
    col1, col2, col3, col_last = st.columns(4)
    with col1: 
        components = st.multiselect("Streamlit components", component_options, None, format_func=lambda x: f"{x[0]}: {x[1]}")
        minimum_views = st.slider("Minimum views", 0, 30, 3)
    with col2:
        dependencies = st.multiselect("Python dependencies", deps_options, None, format_func=lambda x: f"{x[0]}: {x[1]}")
        boost_views = st.slider("Boost views", 0, 10, 1)
    with col3:
        owner = st.text_input("Owner", None)
        number_of_results = st.slider("Number of results", 0, 100, 20)
    with col_last:
        order_by = st.selectbox("Order by", ["relevancy+views", 'relevancy', "unique_views", ], 0)
        
    filters = None
    and_filters = []
    
    if dependencies:
        and_filters.append({"@or": [{"@contains": {"DEPENDENCIES": d[0]}} for d in dependencies]})
    if components:
        and_filters.append({"@or": [{"@contains": {"COMPONENTS": c[0]}} for c in components]})
    if owner:
        and_filters.append({"@eq": {"OWNER": owner}})
    if and_filters:
        filters = {"@and":and_filters}
        

results = search(query, filters, order_by)
results = deduplicate(results)
results = post_filter(results, minimum_views)

for b in batch(results):
    serialize_batch(b)
