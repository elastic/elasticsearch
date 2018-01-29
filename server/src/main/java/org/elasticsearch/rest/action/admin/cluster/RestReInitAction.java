/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 *  [2014] Elasticsearch Incorporated. All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Elasticsearch Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Elasticsearch Incorporated.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.reinit.NodesReInitAction;
import org.elasticsearch.action.admin.cluster.reinit.NodesReInitRequest;
import org.elasticsearch.action.admin.cluster.reinit.NodesReInitResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public final class RestReInitAction extends BaseRestHandler {

    public RestReInitAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_nodes/reinit", this);
        controller.registerHandler(POST, "/_nodes/{nodeId}/reinit", this);
    }

    @Override
    public String getName() {
        return "nodes_reinit_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final NodesReInitRequest nodesReInitRequest = new NodesReInitRequest(nodesIds);
        nodesReInitRequest.timeout(request.param("timeout"));
        nodesReInitRequest.secureStorePassword(request.param("secureStorePassword", ""));

        return channel -> client.admin().cluster().execute(NodesReInitAction.INSTANCE, nodesReInitRequest,
                new RestBuilderListener<NodesReInitResponse>(channel) {

                    @Override
                    public RestResponse buildResponse(NodesReInitResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        RestActions.buildNodesHeader(builder, channel.request(), response);
                        builder.field("cluster_name", response.getClusterName().value());
                        response.toXContent(builder, channel.request());
                        builder.endObject();

                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });

    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
