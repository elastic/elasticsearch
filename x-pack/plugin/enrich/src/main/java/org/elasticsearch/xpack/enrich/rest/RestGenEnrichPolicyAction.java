package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.enrich.GetEnrichPolicyAction;

import java.io.IOException;

public class RestGenEnrichPolicyAction extends BaseRestHandler {

    public RestGenEnrichPolicyAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_enrich", this);
    }

    @Override
    public String getName() {
        return "get_enrich_policy";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        // need to parse out the index param as it may refer to > 1 index.
        final GetEnrichPolicyAction.Request request = new GetEnrichPolicyAction.Request(restRequest.param("index"));
        return channel -> client.execute(GetEnrichPolicyAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}


