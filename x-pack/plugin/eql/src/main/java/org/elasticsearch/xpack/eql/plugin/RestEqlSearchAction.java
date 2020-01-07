/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestEqlSearchAction extends BaseRestHandler {
    private static final String SEARCH_PATH = "/{index}/_eql/search";

    public RestEqlSearchAction(RestController controller) {
        controller.registerHandler(GET, SEARCH_PATH, this);
        controller.registerHandler(POST, SEARCH_PATH, this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
        throws IOException {

        EqlSearchRequest eqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            eqlRequest = EqlSearchRequest.fromXContent(parser);
            eqlRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            eqlRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", true));
        }

        return channel -> client.execute(EqlSearchAction.INSTANCE, eqlRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "eql_search";
    }
}
