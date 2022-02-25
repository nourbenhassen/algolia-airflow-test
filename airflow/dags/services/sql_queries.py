sql_create_table = """
DROP TABLE IF EXISTS shopify_config;
CREATE TABLE IF NOT EXISTS shopify_config (
            id VARCHAR NOT NULL PRIMARY KEY,
            shop_domain VARCHAR NOT NULL,
            application_id VARCHAR NOT NULL,
            autocomplete_enabled BOOLEAN,
            user_created_at_least_one_qr BOOLEAN,
            nbr_merchandised_queries INTEGER,
            nbrs_pinned_items VARCHAR NOT NULL,
            showing_logo BOOLEAN,
            has_changed_sort_orders BOOLEAN,
            analytics_enabled BOOLEAN,
            use_metafields BOOLEAN,
            nbr_metafields NUMERIC(5),
            use_default_colors BOOLEAN,
            show_products BOOLEAN,
            instant_search_enabled BOOLEAN,
            instant_search_enabled_on_collection BOOLEAN,
            only_using_faceting_on_collection BOOLEAN,
            use_merchandising_for_collection BOOLEAN,
            index_prefix VARCHAR NOT NULL,
            indexing_paused BOOLEAN,
            install_channel VARCHAR NOT NULL,
            export_date DATE,
            has_specific_prefix BOOLEAN
);
"""

sql_insert_values = "INSERT INTO {}({}) VALUES %s"
