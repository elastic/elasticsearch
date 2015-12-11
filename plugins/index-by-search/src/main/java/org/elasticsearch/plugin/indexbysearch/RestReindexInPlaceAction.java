package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.plugin.indexbysearch.ReindexInPlaceAction.INSTANCE;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestToXContentListener;

public class RestReindexInPlaceAction extends BaseRestHandler {
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
        /*
         * Passing the search request through ReindexInPlaceRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parse can override them.
         */
        ReindexInPlaceRequest internalRequest = new ReindexInPlaceRequest(new SearchRequest());
        int batchSize = internalRequest.search().source().size();
        internalRequest.search().source().size(-1);
        RestSearchAction.parseSearchRequest(internalRequest.search(), indicesQueriesRegistry, request,
                parseFieldMatcher);
        // TODO allow the user to modify the batch size? Or pick something better than just a default.
        internalRequest.size(internalRequest.search().source().size());
        internalRequest.search().source().size(batchSize);

        String conflicts = request.param("conflicts");
        if (conflicts != null) {
            switch (conflicts) {
            case "proceed":
                internalRequest.abortOnVersionConflict(false);
                break;
            case "abort":
                internalRequest.abortOnVersionConflict(true);
                break;
            default:
                badRequest(channel, "conflicts may only be \"proceed\" or \"abort\" but was [" + conflicts + "]");
                return;
            }
        }

        client.execute(INSTANCE, internalRequest, new RestToXContentListener<>(channel));
    }

    private void badRequest(RestChannel channel, String message) {
        try {
            XContentBuilder builder = channel.newErrorBuilder();
            channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder.startObject().field("error", message).endObject()));
        } catch (IOException e) {
            logger.warn("Failed to send response", e);
        }
    }
}
