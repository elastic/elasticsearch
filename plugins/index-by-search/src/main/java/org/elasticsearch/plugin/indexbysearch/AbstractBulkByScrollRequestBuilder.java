package org.elasticsearch.plugin.indexbysearch;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public abstract class AbstractBulkByScrollRequestBuilder<Request extends AbstractBulkByScrollRequest<Request>, Response extends ActionResponse, Self extends AbstractBulkByScrollRequestBuilder<Request, Response, Self>>
        extends ActionRequestBuilder<Request, Response, Self> {
    private final SearchRequestBuilder search;

    protected AbstractBulkByScrollRequestBuilder(ElasticsearchClient client,
            Action<Request, Response, Self> action, SearchRequestBuilder search, Request request) {
        super(client, action, request);
        this.search = search;
    }

    protected abstract Self self();

    // NOCOMMIT rename to source to match REST
    public SearchRequestBuilder search() {
        return search;
    }

    /**
     * The maximum number of documents to attempt.
     */
    public Self size(int size) {
        request.size(size);
        return self();
    }

    /**
     * Should we version conflicts cause the action to abort?
     */
    public Self abortOnVersionConflict(boolean abortOnVersionConflict) {
        request.abortOnVersionConflict(abortOnVersionConflict);
        return self();
    }

}
