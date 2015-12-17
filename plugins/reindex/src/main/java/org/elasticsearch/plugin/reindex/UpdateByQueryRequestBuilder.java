package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class UpdateByQueryRequestBuilder extends
        AbstractBulkIndexByScrollRequestBuilder<UpdateByQueryRequest, BulkIndexByScrollResponse, UpdateByQueryRequestBuilder> {

    public UpdateByQueryRequestBuilder(ElasticsearchClient client,
            Action<UpdateByQueryRequest, BulkIndexByScrollResponse, UpdateByQueryRequestBuilder> action) {
        this(client, action, new SearchRequestBuilder(client, SearchAction.INSTANCE));
    }

    private UpdateByQueryRequestBuilder(ElasticsearchClient client,
            Action<UpdateByQueryRequest, BulkIndexByScrollResponse, UpdateByQueryRequestBuilder> action,
            SearchRequestBuilder search) {
        super(client, action, search, new UpdateByQueryRequest(search.request()));
    }

    @Override
    protected UpdateByQueryRequestBuilder self() {
        return this;
    }

    @Override
    public UpdateByQueryRequestBuilder abortOnVersionConflict(boolean abortOnVersionConflict) {
        request.abortOnVersionConflict(abortOnVersionConflict);
        return this;
    }
}
