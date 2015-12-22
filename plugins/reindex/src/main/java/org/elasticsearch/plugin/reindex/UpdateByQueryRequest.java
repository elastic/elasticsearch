package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.search.SearchRequest;

/**
 * Request to reindex a set of documents where they are without changing their
 * locations or IDs.
 */
public class UpdateByQueryRequest extends AbstractBulkIndexByScrollRequest<UpdateByQueryRequest> {
    public UpdateByQueryRequest() {
    }

    public UpdateByQueryRequest(SearchRequest search) {
        super(search);
    }

    @Override
    protected UpdateByQueryRequest self() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("update-by-query ");
        searchToString(b);
        return b.toString();
    }
}
