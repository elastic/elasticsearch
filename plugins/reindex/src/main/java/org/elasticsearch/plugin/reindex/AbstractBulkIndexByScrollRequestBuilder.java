package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.script.Script;

public abstract class AbstractBulkIndexByScrollRequestBuilder<Request extends AbstractBulkIndexByScrollRequest<Request>, Response extends ActionResponse, Self extends AbstractBulkIndexByScrollRequestBuilder<Request, Response, Self>>
        extends AbstractBulkByScrollRequestBuilder<Request, Response, Self> {

    protected AbstractBulkIndexByScrollRequestBuilder(ElasticsearchClient client,
            Action<Request, Response, Self> action, SearchRequestBuilder search, Request request) {
        super(client, action, search, request);
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Self script(Script script) {
        request.script(script);
        return self();
    }
}
