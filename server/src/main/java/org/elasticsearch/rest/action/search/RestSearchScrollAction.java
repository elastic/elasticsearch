/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSearchScrollAction extends BaseRestHandler {
    private static final Set<String> RESPONSE_PARAMS = Collections.singleton(RestSearchAction.TOTAL_HITS_AS_INT_PARAM);

    @Override
    public String getName() {
        return "search_scroll_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_search/scroll"),
            new Route(POST, "/_search/scroll"),
            new Route(GET, "/_search/scroll/{scroll_id}"),
            new Route(POST, "/_search/scroll/{scroll_id}")
        );
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String scrollId = request.param("scroll_id");
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        searchScrollRequest.scrollId(scrollId);
        String scroll = request.param("scroll");
        if (scroll != null) {
            searchScrollRequest.scroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        request.withContentOrSourceParamParserOrNull(xContentParser -> {
            if (xContentParser != null) {
                // NOTE: if rest request with xcontent body has request parameters, values parsed from request body have the precedence
                try {
                    searchScrollRequest.fromXContent(xContentParser);
                } catch (IOException | XContentParseException e) {
                    throw new IllegalArgumentException("Failed to parse request body", e);
                }
            }
        });
        return channel -> client.searchScroll(searchScrollRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
