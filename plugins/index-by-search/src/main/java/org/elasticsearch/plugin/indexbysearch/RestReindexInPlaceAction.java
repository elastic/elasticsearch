package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.rest.RestRequest.Method.POST;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class RestReindexInPlaceAction  extends BaseRestHandler {
    private IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestReindexInPlaceAction(Settings settings, RestController controller, Client client,
            IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings, controller, client);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        controller.registerHandler(POST, "/{index}/_reindex", this);
        controller.registerHandler(POST, "/{index}/{type}/_reindex", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {

    }

}
