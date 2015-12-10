package org.elasticsearch.plugin.indexbysearch;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class ReindexInPlaceRequestBuilder extends
        AbstractBulkByScrollRequestBuilder<ReindexInPlaceRequest, BulkIndexByScrollResponse, ReindexInPlaceRequestBuilder> {

    public ReindexInPlaceRequestBuilder(ElasticsearchClient client,
            Action<ReindexInPlaceRequest, BulkIndexByScrollResponse, ReindexInPlaceRequestBuilder> action) {
        this(client, action, new SearchRequestBuilder(client, SearchAction.INSTANCE));
    }

    private ReindexInPlaceRequestBuilder(ElasticsearchClient client,
            Action<ReindexInPlaceRequest, BulkIndexByScrollResponse, ReindexInPlaceRequestBuilder> action,
            SearchRequestBuilder search) {
        super(client, action, search, new ReindexInPlaceRequest(search.request()));
    }

    @Override
    protected ReindexInPlaceRequestBuilder self() {
        return this;
    }

    public ReindexInPlaceRequestBuilder useReindexVersionType(boolean useReindexVersionType) {
        request.useReindexVersionType(useReindexVersionType);
        return this;
    }
}
