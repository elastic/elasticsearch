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
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetSynonymsAction extends BaseRestHandler {

    private static final int DEFAULT_FROM_PARAM = 0;
    private static final int DEFAULT_SIZE_PARAM = 10;

    @Override
    public String getName() {
        return "synonyms_get_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_synonyms/{synonymsSet}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        if (restRequest.hasParam("from") && restRequest.hasParam("search_after")) {
            throw new IllegalArgumentException("[from] and [search_after] cannot be used together");
        }
        String synonymsSet = restRequest.param("synonymsSet");
        int size = restRequest.paramAsInt("size", DEFAULT_SIZE_PARAM);
        int from = restRequest.paramAsInt("from", DEFAULT_FROM_PARAM);
        String searchAfterRaw = restRequest.param("search_after");
        String searchAfter = Strings.isNullOrBlank(searchAfterRaw) ? null : searchAfterRaw;
        GetSynonymsAction.Request request;
        if (from != DEFAULT_FROM_PARAM) {
            // Legacy offset-based pagination (also catches negative from, which validation will reject)
            request = new GetSynonymsAction.Request(synonymsSet, from, size);
        } else {
            // Cursor-based pagination: searchAfter is null on the first page
            request = new GetSynonymsAction.Request(synonymsSet, searchAfter, size);
        }
        return channel -> client.execute(GetSynonymsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
