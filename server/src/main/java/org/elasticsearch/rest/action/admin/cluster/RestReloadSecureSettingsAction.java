/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public final class RestReloadSecureSettingsAction extends BaseRestHandler implements RestRequestFilter {

    static final ObjectParser<NodesReloadSecureSettingsRequest, String> PARSER =
        new ObjectParser<>("reload_secure_settings", NodesReloadSecureSettingsRequest::new);

    static {
        PARSER.declareString((request, value) -> request.setSecureStorePassword(new SecureString(value.toCharArray())),
            new ParseField("secure_settings_password"));
    }

    @Override
    public String getName() {
        return "nodes_reload_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_nodes/reload_secure_settings"),
            new Route(POST, "/_nodes/{nodeId}/reload_secure_settings"));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final NodesReloadSecureSettingsRequestBuilder nodesRequestBuilder = client.admin()
            .cluster()
            .prepareReloadSecureSettings()
            .setTimeout(request.param("timeout"))
            .setNodesIds(nodesIds);
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser != null) {
                final NodesReloadSecureSettingsRequest nodesRequest = PARSER.parse(parser, null);
                nodesRequestBuilder.setSecureStorePassword(nodesRequest.getSecureSettingsPassword());
            }
        });

        return channel -> nodesRequestBuilder
                .execute(new RestBuilderListener<NodesReloadSecureSettingsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(NodesReloadSecureSettingsResponse response, XContentBuilder builder)
                        throws Exception {
                        builder.startObject();
                        RestActions.buildNodesHeader(builder, channel.request(), response);
                        builder.field("cluster_name", response.getClusterName().value());
                        response.toXContent(builder, channel.request());
                        builder.endObject();
                        nodesRequestBuilder.request().closePassword();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
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
