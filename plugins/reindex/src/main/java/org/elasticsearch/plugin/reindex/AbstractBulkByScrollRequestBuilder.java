package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

public abstract class AbstractBulkByScrollRequestBuilder<Request extends AbstractBulkByScrollRequest<Request>, Response extends ActionResponse, Self extends AbstractBulkByScrollRequestBuilder<Request, Response, Self>>
        extends ActionRequestBuilder<Request, Response, Self> {
    private final SearchRequestBuilder source;

    protected AbstractBulkByScrollRequestBuilder(ElasticsearchClient client,
            Action<Request, Response, Self> action, SearchRequestBuilder source, Request request) {
        super(client, action, request);
        this.source = source;
    }

    protected abstract Self self();

    /**
     * The search used to find documents to process.
     */
    public SearchRequestBuilder source() {
        return source;
    }

    /**
     * Set the source indices.
     */
    public Self source(String... indices) {
        source.setIndices(indices);
        return self();
    }

    /**
     * Set the query that will filter the source. Just a convenience method for
     * easy chaining.
     */
    public Self filter(QueryBuilder<?> filter) {
        source.setQuery(filter);
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

    /**
     * Call refresh on the indexes we've written to after the request ends?
     */
    public Self refresh(boolean refresh) {
        request.refresh(refresh);
        return self();
    }

    /**
     * Timeout to wait for the shards on to be available for each bulk request?
     */
    public Self timeout(TimeValue timeout) {
        request.timeout(timeout);
        return self();
    }

    /**
     * Timeout to wait for the shards on to be available for each bulk request?
     */
    public Self consistency(WriteConsistencyLevel consistency) {
        request.consistency(consistency);
        return self();
    }
}
