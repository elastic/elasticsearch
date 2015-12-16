package org.elasticsearch.plugin.indexbysearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

/**
 * Abstract base for scrolling across a search and executing bulk indexes on all
 * results.
 */
public abstract class AbstractAsyncBulkIndexByScrollAction<Request extends AbstractBulkByScrollRequest<Request>, Response extends BulkIndexByScrollResponse>
        extends AbstractAsyncBulkByScrollAction<Request, Response> {

    public AbstractAsyncBulkIndexByScrollAction(ESLogger logger, Client client, Request mainRequest, SearchRequest firstSearchRequest,
            ActionListener<Response> listener) {
        super(logger, client, mainRequest, firstSearchRequest, listener);
    }

    /**
     * Utility to help with the complex parts of building an IndexRequest from a
     * SearchHit. Shared by extensions.
     */
    protected void copyMetadata(IndexRequest index, SearchHit doc) {
        SearchHitField parent = doc.field("_parent");
        if (parent != null) {
            index.parent(parent.value());
        }
        copyRouting(index, doc);
        SearchHitField timestamp = doc.field("_timestamp");
        if (timestamp != null) {
            // Comes back as a Long but needs to be a string
            index.timestamp(timestamp.value().toString());
        }
        SearchHitField ttl = doc.field("_ttl");
        if (ttl != null) {
            index.ttl(ttl.value());
        }
    }

    /**
     * Part of copyMetadata.
     */
    protected void copyRouting(IndexRequest index, SearchHit doc) {
        SearchHitField routing = doc.field("_routing");
        if (routing != null) {
            index.routing(routing.value());
        }
    }
}
