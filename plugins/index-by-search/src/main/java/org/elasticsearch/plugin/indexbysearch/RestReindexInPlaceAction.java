package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.plugin.indexbysearch.ReindexInPlaceAction.INSTANCE;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestActions;
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
        /*
         * We can't send parseSearchRequest REST content that it doesn't support
         * so we will have to remove the content that is valid in addition to
         * what it supports from the content first. This is a temporary hack and
         * should get better when SearchRequest has full ObjectParser support
         * then we can delegate and stuff.
         */
        BytesReference bodyContent = null;
        if (RestActions.hasBodyContent(request)) {
            bodyContent = RestActions.getRestContent(request);
            Tuple<XContentType, Map<String, Object>> body = XContentHelper.convertToMap(bodyContent, false);
            boolean modified = false;
            String conflicts = (String) body.v2().remove("conflicts");
            if (conflicts != null) {
                internalRequest.conflicts(conflicts);
                modified = true;
            }
            String versionType = (String) body.v2().remove("version_type");
            if (versionType != null) {
                internalRequest.versionType(versionType);
                modified = true;
            }
            if (modified) {
                XContentBuilder builder = XContentFactory.contentBuilder(body.v1());
                builder.map(body.v2());
                bodyContent = builder.bytes();
            }
        }
        RestSearchAction.parseSearchRequest(internalRequest.search(), indicesQueriesRegistry, request,
                parseFieldMatcher, bodyContent);

        String conflicts = request.param("conflicts");
        if (conflicts != null) {
            internalRequest.conflicts(conflicts);
        }
        String versionType = request.param("version_type");
        if (versionType != null) {
            internalRequest.versionType(versionType);
        }

        // TODO allow the user to modify the batch size? Or pick something better than just a default.
        internalRequest.size(internalRequest.search().source().size());
        internalRequest.search().source().size(batchSize);


        client.execute(INSTANCE, internalRequest, new RestToXContentListener<>(channel));
    }
}
