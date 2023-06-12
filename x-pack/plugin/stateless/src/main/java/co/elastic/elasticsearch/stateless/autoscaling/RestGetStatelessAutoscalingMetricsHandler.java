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

package co.elastic.elasticsearch.stateless.autoscaling;

import co.elastic.elasticsearch.stateless.autoscaling.action.GetStatelessAutoscalingMetricsAction;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest endpoint for fetching autoscaling metrics. Invoked by CP
 */
@ServerlessScope(Scope.INTERNAL)
public class RestGetStatelessAutoscalingMetricsHandler extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_stateless_autoscaling_metrics";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_internal/stateless/autoscaling"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        return channel -> client.execute(
            GetStatelessAutoscalingMetricsAction.INSTANCE,
            new GetStatelessAutoscalingMetricsAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }
}
