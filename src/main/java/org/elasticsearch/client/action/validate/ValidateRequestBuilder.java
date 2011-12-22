package org.elasticsearch.client.action.validate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.action.validate.ValidateRequest;
import org.elasticsearch.action.validate.ValidateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.support.BaseRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 *
 */
public class ValidateRequestBuilder extends BaseRequestBuilder<ValidateRequest, ValidateResponse> {
    public ValidateRequestBuilder(Client client) {
        super(client, new ValidateRequest());
    }

    /**
     * Sets the indices the query validation will run against.
     */
    public ValidateRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public ValidateRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * The query source to validate.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public ValidateRequestBuilder setQuery(QueryBuilder queryBuilder) {
        request.query(queryBuilder);
        return this;
    }

    /**
     * The query source to validate.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public ValidateRequestBuilder setQuery(byte[] querySource) {
        request.query(querySource);
        return this;
    }

    /**
     * Controls the operation threading model.
     */
    public ValidateRequestBuilder setOperationThreading(BroadcastOperationThreading operationThreading) {
        request.operationThreading(operationThreading);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public ValidateRequestBuilder setListenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<ValidateResponse> listener) {
        client.validate(request, listener);
    }
}
