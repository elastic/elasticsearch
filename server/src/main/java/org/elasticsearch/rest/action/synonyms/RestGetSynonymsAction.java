/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.synonyms;

import org.elasticsearch.action.synonyms.GetSynonymsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetSynonymsAction extends BaseRestHandler {

    private static final Integer DEFAULT_FROM_PARAM = 0;
    private static final Integer DEFAULT_SIZE_PARAM = 10;

    @Override
    public String getName() {
        return "synonyms_get_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_synonyms/{synonymsSet}"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return SynonymCapabilities.GET_SYNONYMS_CAPABILITIES;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String pitId = restRequest.param("pit_id");
        String searchAfter = restRequest.param("search_after");

        // Use PIT-based cursor pagination when the caller has not explicitly requested offset-based
        // pagination via "from". Cursor mode is the default for new clients; "from" is preserved for
        // backwards compatibility with existing integrations.
        final GetSynonymsAction.Request request;
        if (pitId != null || searchAfter != null || restRequest.hasParam("from") == false) {
            request = new GetSynonymsAction.Request(
                restRequest.param("synonymsSet"),
                restRequest.paramAsInt("size", DEFAULT_SIZE_PARAM),
                pitId,
                searchAfter
            );
        } else {
            request = new GetSynonymsAction.Request(
                restRequest.param("synonymsSet"),
                restRequest.paramAsInt("from", DEFAULT_FROM_PARAM),
                restRequest.paramAsInt("size", DEFAULT_SIZE_PARAM)
            );
        }
        return channel -> client.execute(GetSynonymsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
