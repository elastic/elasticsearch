/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.elasticsearch.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getTimeout;

public final class RestReloadSecureSettingsAction extends BaseRestHandler implements RestRequestFilter {

    static final class ParsedRequestBody {
        @Nullable
        SecureString secureSettingsPassword;
    }

    static final ObjectParser<ParsedRequestBody, String> PARSER = new ObjectParser<>("reload_secure_settings", ParsedRequestBody::new);

    static {
        PARSER.declareString(
            (parsedRequestBody, value) -> parsedRequestBody.secureSettingsPassword = new SecureString(value.toCharArray()),
            new ParseField("secure_settings_password")
        );
    }

    @Override
    public String getName() {
        return "nodes_reload_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_nodes/reload_secure_settings"), new Route(POST, "/_nodes/{nodeId}/reload_secure_settings"));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final NodesReloadSecureSettingsRequest reloadSecureSettingsRequest = new NodesReloadSecureSettingsRequest(
            Strings.splitStringByCommaToArray(request.param("nodeId"))
        );
        reloadSecureSettingsRequest.setTimeout(getTimeout(request));
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser != null) {
                final ParsedRequestBody parsedRequestBody = PARSER.parse(parser, null);
                reloadSecureSettingsRequest.setSecureStorePassword(parsedRequestBody.secureSettingsPassword);
            }
        });

        return new RestChannelConsumer() {
            @Override
            public void accept(RestChannel channel) {
                client.execute(
                    TransportNodesReloadSecureSettingsAction.TYPE,
                    reloadSecureSettingsRequest,
                    new RestBuilderListener<>(channel) {
                        @Override
                        public RestResponse buildResponse(NodesReloadSecureSettingsResponse response, XContentBuilder builder)
                            throws Exception {
                            builder.startObject();
                            RestActions.buildNodesHeader(builder, channel.request(), response);
                            builder.field("cluster_name", response.getClusterName().value());
                            response.toXContent(builder, channel.request());
                            builder.endObject();
                            return new RestResponse(RestStatus.OK, builder);
                        }
                    }
                );
            }

            @Override
            public void close() {
                reloadSecureSettingsRequest.decRef();
            }
        };
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("secure_settings_password");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
