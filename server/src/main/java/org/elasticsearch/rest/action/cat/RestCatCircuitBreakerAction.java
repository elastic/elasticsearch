/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.util.set.Sets.addToCopy;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestCatCircuitBreakerAction extends AbstractCatAction {

    private static final Set<String> RESPONSE_PARAMS = addToCopy(AbstractCatAction.RESPONSE_PARAMS, "circuit_breaker_patterns");

    @Override
    public String getName() {
        return "cat_circuitbreaker_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/circuit_breaker"), new Route(GET, "/_cat/circuit_breaker/{circuit_breaker_patterns}"));
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/circuit_breaker\n");
        sb.append("/_cat/circuit_breaker/{circuit_breaker_patterns}\n");
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest(getMasterNodeTimeout(request));
        clusterStateRequest.clear().nodes(true);

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(
                    clusterStateResponse.getState().nodes().stream().map(DiscoveryNode::getId).toArray(String[]::new)
                );
                nodesStatsRequest.clear().addMetric(NodesStatsRequestParameters.Metric.BREAKER);
                client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(final NodesStatsResponse nodesStatsResponse) throws Exception {
                        return RestTable.buildResponse(buildTable(request, nodesStatsResponse), channel);
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("node_id", "default:true;alias:id;desc:persistent node id");
        table.addCell("node_name", "default:false;alias:nn;desc:node name");
        table.addCell("breaker", "default:true;alias:br;desc:breaker name");
        table.addCell("limit", "default:true;alias:l;desc:limit size");
        table.addCell("limit_bytes", "default:false;alias:lb;desc:limit size in bytes");
        table.addCell("estimated", "default:true;alias:e;desc:estimated size");
        table.addCell("estimated_bytes", "default:false;alias:eb;desc:estimated size in bytes");
        table.addCell("tripped", "default:true;alias:t;desc:tripped count");
        table.addCell("overhead", "default:false;alias:o;desc:overhead");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, NodesStatsResponse nodesStatsResponse) {
        final Table table = getTableWithHeader(request);
        final String[] circuitBreakers = request.paramAsStringArray("circuit_breaker_patterns", new String[] { "*" });

        for (final NodeStats nodeStats : nodesStatsResponse.getNodes()) {
            if (nodeStats.getBreaker() == null) {
                continue;
            }
            for (final CircuitBreakerStats circuitBreakerStats : nodeStats.getBreaker().getAllStats()) {
                if (Regex.simpleMatch(circuitBreakers, circuitBreakerStats.getName()) == false) {
                    continue;
                }
                table.startRow();
                table.addCell(nodeStats.getNode().getId());
                table.addCell(nodeStats.getNode().getName());
                table.addCell(circuitBreakerStats.getName());
                table.addCell(ByteSizeValue.ofBytes(circuitBreakerStats.getLimit()));
                table.addCell(circuitBreakerStats.getLimit());
                table.addCell(ByteSizeValue.ofBytes(circuitBreakerStats.getEstimated()));
                table.addCell(circuitBreakerStats.getEstimated());
                table.addCell(circuitBreakerStats.getTrippedCount());
                table.addCell(circuitBreakerStats.getOverhead());
                table.endRow();
            }
        }
        return table;
    }
}
