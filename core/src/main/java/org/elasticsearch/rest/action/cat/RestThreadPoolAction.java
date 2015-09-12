/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestActionListener;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestThreadPoolAction extends AbstractCatAction {

    private final static String[] SUPPORTED_NAMES = new String[]{
            ThreadPool.Names.BULK,
            ThreadPool.Names.FLUSH,
            ThreadPool.Names.GENERIC,
            ThreadPool.Names.GET,
            ThreadPool.Names.INDEX,
            ThreadPool.Names.MANAGEMENT,
            ThreadPool.Names.OPTIMIZE,
            ThreadPool.Names.PERCOLATE,
            ThreadPool.Names.REFRESH,
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SNAPSHOT,
            ThreadPool.Names.SUGGEST,
            ThreadPool.Names.WARMER
    };

    private final static String[] SUPPORTED_ALIASES = new String[]{
            "b",
            "f",
            "ge",
            "g",
            "i",
            "ma",
            "o",
            "p",
            "r",
            "s",
            "sn",
            "su",
            "w"
    };

    static {
        assert SUPPORTED_ALIASES.length == SUPPORTED_NAMES.length: "SUPPORTED_NAMES/ALIASES mismatch";
    }

    private final static String[] DEFAULT_THREAD_POOLS = new String[]{
            ThreadPool.Names.BULK,
            ThreadPool.Names.INDEX,
            ThreadPool.Names.SEARCH,
    };

    private final static Map<String, String> ALIAS_TO_THREAD_POOL;
    private final static Map<String, String> THREAD_POOL_TO_ALIAS;

    static {
        ALIAS_TO_THREAD_POOL = new HashMap<>(SUPPORTED_NAMES.length);
        for (String supportedThreadPool : SUPPORTED_NAMES) {
            ALIAS_TO_THREAD_POOL.put(supportedThreadPool.substring(0, 3), supportedThreadPool);
        }
        THREAD_POOL_TO_ALIAS = new HashMap<>(SUPPORTED_NAMES.length);
        for (int i = 0; i < SUPPORTED_NAMES.length; i++) {
            THREAD_POOL_TO_ALIAS.put(SUPPORTED_NAMES[i], SUPPORTED_ALIASES[i]);
        }
    }

    @Inject
    public RestThreadPoolAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cat/thread_pool", this);
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/thread_pool\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.clear().process(true).threadPool(true);
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestActionListener<NodesInfoResponse>(channel) {
                    @Override
                    public void processResponse(final NodesInfoResponse nodesInfoResponse) {
                        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
                        nodesStatsRequest.clear().threadPool(true);
                        client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(NodesStatsResponse nodesStatsResponse) throws Exception {
                                return RestTable.buildResponse(buildTable(request, clusterStateResponse, nodesInfoResponse, nodesStatsResponse), channel);
                            }
                        });
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("id", "default:false;alias:nodeId;desc:unique node id");
        table.addCell("pid", "default:false;alias:p;desc:process id");
        table.addCell("host", "alias:h;desc:host name");
        table.addCell("ip", "alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");

        final String[] requestedPools = fetchSortedPools(request, DEFAULT_THREAD_POOLS);
        for (String pool : SUPPORTED_NAMES) {
            String poolAlias = THREAD_POOL_TO_ALIAS.get(pool);
            boolean display = false;
            for (String requestedPool : requestedPools) {
                if (pool.equals(requestedPool)) {
                    display = true;
                    break;
                }
            }

            String defaultDisplayVal = Boolean.toString(display);
            table.addCell(
                    pool + ".type",
                    "alias:" + poolAlias + "t;default:false;desc:" + pool + " thread pool type"
            );
            table.addCell(
                    pool + ".active",
                    "alias:" + poolAlias + "a;default:" + defaultDisplayVal + ";text-align:right;desc:number of active " + pool + " threads"
            );
            table.addCell(
                    pool + ".size",
                    "alias:" + poolAlias + "s;default:false;text-align:right;desc:number of " + pool + " threads"
            );
            table.addCell(
                    pool + ".queue",
                    "alias:" + poolAlias + "q;default:" + defaultDisplayVal + ";text-align:right;desc:number of " + pool + " threads in queue"
            );
            table.addCell(
                    pool + ".queueSize",
                    "alias:" + poolAlias + "qs;default:false;text-align:right;desc:maximum number of " + pool + " threads in queue"
            );
            table.addCell(
                    pool + ".rejected",
                    "alias:" + poolAlias + "r;default:" + defaultDisplayVal + ";text-align:right;desc:number of rejected " + pool + " threads"
            );
            table.addCell(
                    pool + ".largest",
                    "alias:" + poolAlias + "l;default:false;text-align:right;desc:highest number of seen active " + pool + " threads"
            );
            table.addCell(
                    pool + ".completed",
                    "alias:" + poolAlias + "c;default:false;text-align:right;desc:number of completed " + pool + " threads"
            );
            table.addCell(
                    pool + ".min",
                    "alias:" + poolAlias + "mi;default:false;text-align:right;desc:minimum number of " + pool + " threads"
            );
            table.addCell(
                    pool + ".max",
                    "alias:" + poolAlias + "ma;default:false;text-align:right;desc:maximum number of " + pool + " threads"
            );
            table.addCell(
                    pool + ".keepAlive",
                    "alias:" + poolAlias + "k;default:false;text-align:right;desc:" + pool + " thread keep alive time"
            );
        }

        table.endHeaders();
        return table;
    }


    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
        boolean fullId = req.paramAsBoolean("full_id", false);
        DiscoveryNodes nodes = state.getState().nodes();
        Table table = getTableWithHeader(req);

        for (DiscoveryNode node : nodes) {
            NodeInfo info = nodesInfo.getNodesMap().get(node.id());
            NodeStats stats = nodesStats.getNodesMap().get(node.id());
            table.startRow();

            table.addCell(fullId ? node.id() : Strings.substring(node.getId(), 0, 4));
            table.addCell(info == null ? null : info.getProcess().getId());
            table.addCell(node.getHostName());
            table.addCell(node.getHostAddress());
            if (node.address() instanceof InetSocketTransportAddress) {
                table.addCell(((InetSocketTransportAddress) node.address()).address().getPort());
            } else {
                table.addCell("-");
            }

            final Map<String, ThreadPoolStats.Stats> poolThreadStats;
            final Map<String, ThreadPool.Info> poolThreadInfo;

            if (stats == null) {
                poolThreadStats = Collections.emptyMap();
                poolThreadInfo = Collections.emptyMap();
            } else {
                poolThreadStats = new HashMap<>(14);
                poolThreadInfo = new HashMap<>(14);

                ThreadPoolStats threadPoolStats = stats.getThreadPool();
                for (ThreadPoolStats.Stats threadPoolStat : threadPoolStats) {
                    poolThreadStats.put(threadPoolStat.getName(), threadPoolStat);
                }
                if (info != null) {
                    for (ThreadPool.Info threadPoolInfo : info.getThreadPool()) {
                        poolThreadInfo.put(threadPoolInfo.getName(), threadPoolInfo);
                    }
                }
            }
            for (String pool : SUPPORTED_NAMES) {
                ThreadPoolStats.Stats poolStats = poolThreadStats.get(pool);
                ThreadPool.Info poolInfo = poolThreadInfo.get(pool);

                Long maxQueueSize = null;
                String keepAlive = null;
                Integer minThreads = null;
                Integer maxThreads = null;

                if (poolInfo != null) {
                    if (poolInfo.getQueueSize() != null) {
                        maxQueueSize = poolInfo.getQueueSize().singles();
                    }
                    if (poolInfo.getKeepAlive() != null) {
                        keepAlive = poolInfo.getKeepAlive().toString();
                    }
                    if (poolInfo.getMin() >= 0) {
                        minThreads = poolInfo.getMin();
                    }
                    if (poolInfo.getMax() >= 0) {
                        maxThreads = poolInfo.getMax();
                    }
                }

                table.addCell(poolInfo == null  ? null : poolInfo.getType());
                table.addCell(poolStats == null ? null : poolStats.getActive());
                table.addCell(poolStats == null ? null : poolStats.getThreads());
                table.addCell(poolStats == null ? null : poolStats.getQueue());
                table.addCell(maxQueueSize);
                table.addCell(poolStats == null ? null : poolStats.getRejected());
                table.addCell(poolStats == null ? null : poolStats.getLargest());
                table.addCell(poolStats == null ? null : poolStats.getCompleted());
                table.addCell(minThreads);
                table.addCell(maxThreads);
                table.addCell(keepAlive);
            }

            table.endRow();
        }

        return table;
    }

    // The thread pool columns should always be in the same order.
    private String[] fetchSortedPools(RestRequest request, String[] defaults) {
        String[] headers = request.paramAsStringArray("h", null);
        if (headers == null) {
            return defaults;
        } else {
            Set<String> requestedPools = new LinkedHashSet<>(headers.length);
            for (String header : headers) {
                int dotIndex = header.indexOf('.');
                if (dotIndex != -1) {
                    String headerPrefix = header.substring(0, dotIndex);
                    if (THREAD_POOL_TO_ALIAS.containsKey(headerPrefix)) {
                        requestedPools.add(headerPrefix);
                    }
                } else if (ALIAS_TO_THREAD_POOL.containsKey(header)) {
                    requestedPools.add(ALIAS_TO_THREAD_POOL.get(header));
                }

            }
            return requestedPools.toArray(new String[requestedPools.size()]);
        }
    }
}
