/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.metering;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.metering.action.GetBlobStoreStatsNodesRequest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest endpoint for getting blob store usage stats
 */
@ServerlessScope(Scope.INTERNAL)
public class GetBlobStoreStatsRestHandler extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_blob_store_metering_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_internal/blob_store/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        return restChannel -> client.execute(
            Stateless.GET_BLOB_STORE_STATS_ACTION,
            new GetBlobStoreStatsNodesRequest(),
            new RestActions.NodesResponseRestListener<>(restChannel)
        );
    }
}
