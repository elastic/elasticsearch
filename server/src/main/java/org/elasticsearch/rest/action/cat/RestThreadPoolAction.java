/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.common.util.set.Sets.addToCopy;
import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestThreadPoolAction extends AbstractCatAction {

    private static final Set<String> RESPONSE_PARAMS = addToCopy(AbstractCatAction.RESPONSE_PARAMS, "thread_pool_patterns");

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/thread_pool"), new Route(GET, "/_cat/thread_pool/{thread_pool_patterns}"));
    }

    @Override
    public String getName() {
        return "cat_threadpool_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/thread_pool\n");
        sb.append("/_cat/thread_pool/{thread_pools}\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.clear()
                    .addMetrics(NodesInfoRequest.Metric.PROCESS.metricName(), NodesInfoRequest.Metric.THREAD_POOL.metricName());
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestActionListener<NodesInfoResponse>(channel) {
                    @Override
                    public void processResponse(final NodesInfoResponse nodesInfoResponse) {
                        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
                        nodesStatsRequest.clear().addMetric(NodesStatsRequest.Metric.THREAD_POOL.metricName());
                        client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(NodesStatsResponse nodesStatsResponse) throws Exception {
                                return RestTable.buildResponse(
                                    buildTable(request, clusterStateResponse, nodesInfoResponse, nodesStatsResponse),
                                    channel
                                );
                            }
                        });
                    }
                });
            }
        });
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("node_name", "default:true;alias:nn;desc:node name");
        table.addCell("node_id", "default:false;alias:id;desc:persistent node id");
        table.addCell("ephemeral_node_id", "default:false;alias:eid;desc:ephemeral node id");
        table.addCell("pid", "default:false;alias:p;desc:process id");
        table.addCell("host", "default:false;alias:h;desc:host name");
        table.addCell("ip", "default:false;alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");
        table.addCell("name", "default:true;alias:n;desc:thread pool name");
        table.addCell("type", "alias:t;default:false;desc:thread pool type");
        table.addCell("active", "alias:a;default:true;text-align:right;desc:number of active threads");
        table.addCell("pool_size", "alias:psz;default:false;text-align:right;desc:number of threads");
        table.addCell("queue", "alias:q;default:true;text-align:right;desc:number of tasks currently in queue");
        table.addCell("queue_size", "alias:qs;default:false;text-align:right;desc:maximum number of tasks permitted in queue");
        table.addCell("rejected", "alias:r;default:true;text-align:right;desc:number of rejected tasks");
        table.addCell("largest", "alias:l;default:false;text-align:right;desc:highest number of seen active threads");
        table.addCell("completed", "alias:c;default:false;text-align:right;desc:number of completed tasks");
        table.addCell("core", "alias:cr;default:false;text-align:right;desc:core number of threads in a scaling thread pool");
        table.addCell("max", "alias:mx;default:false;text-align:right;desc:maximum number of threads in a scaling thread pool");
        table.addCell("size", "alias:sz;default:false;text-align:right;desc:number of threads in a fixed thread pool");
        table.addCell("keep_alive", "alias:ka;default:false;text-align:right;desc:thread keep alive time");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
        final String[] threadPools = req.paramAsStringArray("thread_pool_patterns", new String[] { "*" });
        final DiscoveryNodes nodes = state.getState().nodes();
        final Table table = getTableWithHeader(req);

        // collect all thread pool names that we see across the nodes
        final Set<String> candidates = new HashSet<>();
        for (final NodeStats nodeStats : nodesStats.getNodes()) {
            for (final ThreadPoolStats.Stats threadPoolStats : nodeStats.getThreadPool()) {
                candidates.add(threadPoolStats.name());
            }
        }

        // collect all thread pool names that match the specified thread pool patterns
        final Set<String> included = new HashSet<>();
        for (final String candidate : candidates) {
            if (Regex.simpleMatch(threadPools, candidate)) {
                included.add(candidate);
            }
        }

        for (final DiscoveryNode node : nodes) {
            final NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
            final NodeStats stats = nodesStats.getNodesMap().get(node.getId());

            final Map<String, ThreadPoolStats.Stats> poolThreadStats;
            final Map<String, ThreadPool.Info> poolThreadInfo;

            if (stats == null) {
                poolThreadStats = Collections.emptyMap();
                poolThreadInfo = Collections.emptyMap();
            } else {
                // we use a sorted map to ensure that thread pools are sorted by name
                poolThreadStats = new TreeMap<>();
                poolThreadInfo = new HashMap<>();

                ThreadPoolStats threadPoolStats = stats.getThreadPool();
                for (ThreadPoolStats.Stats threadPoolStat : threadPoolStats) {
                    poolThreadStats.put(threadPoolStat.name(), threadPoolStat);
                }
                if (info != null) {
                    for (ThreadPool.Info threadPoolInfo : info.getInfo(ThreadPoolInfo.class)) {
                        poolThreadInfo.put(threadPoolInfo.getName(), threadPoolInfo);
                    }
                }
            }
            for (Map.Entry<String, ThreadPoolStats.Stats> entry : poolThreadStats.entrySet()) {

                if (included.contains(entry.getKey()) == false) {
                    continue;
                }

                table.startRow();

                table.addCell(node.getName());
                table.addCell(node.getId());
                table.addCell(node.getEphemeralId());
                table.addCell(info == null ? null : info.getInfo(ProcessInfo.class).getId());
                table.addCell(node.getHostName());
                table.addCell(node.getHostAddress());
                table.addCell(node.getAddress().address().getPort());
                final ThreadPoolStats.Stats poolStats = entry.getValue();
                final ThreadPool.Info poolInfo = poolThreadInfo.get(entry.getKey());

                Long maxQueueSize = null;
                TimeValue keepAlive = null;
                Integer core = null;
                Integer max = null;
                Integer size = null;

                if (poolInfo != null) {
                    if (poolInfo.getQueueSize() != null) {
                        maxQueueSize = poolInfo.getQueueSize().singles();
                    }
                    if (poolInfo.getKeepAlive() != null) {
                        keepAlive = poolInfo.getKeepAlive();
                    }

                    if (poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.SCALING) {
                        assert poolInfo.getMin() >= 0;
                        core = poolInfo.getMin();
                        assert poolInfo.getMax() > 0;
                        max = poolInfo.getMax();
                    } else {
                        assert poolInfo.getMin() == poolInfo.getMax() && poolInfo.getMax() > 0;
                        size = poolInfo.getMax();
                    }
                }

                table.addCell(entry.getKey());
                table.addCell(poolInfo == null ? null : poolInfo.getThreadPoolType().getType());
                table.addCell(poolStats == null ? null : poolStats.active());
                table.addCell(poolStats == null ? null : poolStats.threads());
                table.addCell(poolStats == null ? null : poolStats.queue());
                table.addCell(maxQueueSize == null ? -1 : maxQueueSize);
                table.addCell(poolStats == null ? null : poolStats.rejected());
                table.addCell(poolStats == null ? null : poolStats.largest());
                table.addCell(poolStats == null ? null : poolStats.completed());
                table.addCell(core);
                table.addCell(max);
                table.addCell(size);
                table.addCell(keepAlive);

                table.endRow();
            }
        }

        return table;
    }
}
