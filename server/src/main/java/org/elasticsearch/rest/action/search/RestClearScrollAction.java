/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestClearScrollAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_search/scroll"), new Route(DELETE, "/_search/scroll/{scroll_id}"));
    }

    @Override
    public String getName() {
        return "clear_scroll_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String scrollIds = request.param("scroll_id");
        ClearScrollRequest clearRequest = new ClearScrollRequest();
        clearRequest.setScrollIds(asList(Strings.splitStringByCommaToArray(scrollIds)));
        request.withContentOrSourceParamParserOrNull((xContentParser -> {
            if (xContentParser != null) {
                // NOTE: if rest request with xcontent body has request parameters, values parsed from request body have the precedence
                try {
                    clearRequest.fromXContent(xContentParser);
                } catch (IOException | XContentParseException e) {
                    throw new IllegalArgumentException("Failed to parse request body", e);
                }
            }
        }));

        return channel -> client.clearScroll(clearRequest, new RestStatusToXContentListener<>(channel));
    }

}
