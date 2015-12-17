package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.plugin.reindex.ReindexInPlaceRequest.ReindexVersionType;

public class ReindexInPlaceRequestBuilder extends
        AbstractBulkIndexByScrollRequestBuilder<ReindexInPlaceRequest, BulkIndexByScrollResponse, ReindexInPlaceRequestBuilder> {

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

    public ReindexInPlaceRequestBuilder versionType(ReindexVersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    @Override
    public ReindexInPlaceRequestBuilder abortOnVersionConflict(boolean abortOnVersionConflict) {
        request.abortOnVersionConflict(abortOnVersionConflict);
        return this;
    }
}
