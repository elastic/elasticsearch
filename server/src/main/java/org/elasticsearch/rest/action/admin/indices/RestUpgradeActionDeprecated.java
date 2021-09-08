/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestUpgradeActionDeprecated extends BaseRestHandler {
    public static final String UPGRADE_API_DEPRECATION_MESSAGE =
        "The _upgrade API is no longer useful and will be removed. Instead, see _reindex API.";

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_upgrade")
                .deprecated(UPGRADE_API_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, "/{index}/_upgrade")
                .deprecated(UPGRADE_API_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build(),
            Route.builder(GET, "/_upgrade")
                .deprecated(UPGRADE_API_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build(),
            Route.builder(GET, "/{index}/_upgrade")
                .deprecated(UPGRADE_API_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build());
    }

    @Override
    public String getName() {
        return "upgrade_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        request.param("index");
        final UpgradeActionDeprecatedException exception = new UpgradeActionDeprecatedException(request);
        return channel -> channel.sendResponse(new BytesRestResponse(channel, exception));
    }

    public static class UpgradeActionDeprecatedException extends IllegalArgumentException {
        private final String path;
        private final RestRequest.Method method;

        public UpgradeActionDeprecatedException(RestRequest restRequest) {
            this.path = restRequest.path();
            this.method = restRequest.method();
        }

        @Override
        public final String getMessage() {
            return String.format(Locale.ROOT, "Upgrade action %s %s was removed, use _reindex API instead", method, path);
        }
    }
}
