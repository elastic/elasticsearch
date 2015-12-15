package org.elasticsearch.plugin.indexbysearch;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;

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
     * Set the source indices.
     */
    public Self source(String... indices) {
        search.setIndices(indices);
        return self();
    }

    /**
     * Set the query that will filter the source. Just a convenience method for
     * easy chaining.
     */
    public Self filter(QueryBuilder<?> filter) {
        search.setQuery(filter);
        return self();
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
