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

import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodesAction extends AbstractCatAction {

    @Inject
    public RestNodesAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/nodes", this);
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/nodes\n");
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
                nodesInfoRequest.clear().jvm(true).os(true).process(true);
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestActionListener<NodesInfoResponse>(channel) {
                    @Override
                    public void processResponse(final NodesInfoResponse nodesInfoResponse) {
                        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
                        nodesStatsRequest.clear().jvm(true).os(true).fs(true).indices(true);
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
    Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("id", "default:false;alias:id,nodeId;desc:unique node id");
        table.addCell("pid", "default:false;alias:p;desc:process id");
        table.addCell("host", "alias:h;desc:host name");
        table.addCell("ip", "alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");

        table.addCell("version", "default:false;alias:v;desc:es version");
        table.addCell("build", "default:false;alias:b;desc:es build hash");
        table.addCell("jdk", "default:false;alias:j;desc:jdk version");
        table.addCell("disk.avail", "default:false;alias:d,disk,diskAvail;text-align:right;desc:available disk space");
        table.addCell("heap.percent", "alias:hp,heapPercent;text-align:right;desc:used heap ratio");
        table.addCell("heap.max", "default:false;alias:hm,heapMax;text-align:right;desc:max configured heap");
        table.addCell("ram.percent", "alias:rp,ramPercent;text-align:right;desc:used machine memory ratio");
        table.addCell("ram.max", "default:false;alias:rm,ramMax;text-align:right;desc:total machine memory");

        table.addCell("load", "alias:l;text-align:right;desc:most recent load avg");
        table.addCell("uptime", "default:false;alias:u;text-align:right;desc:node uptime");
        table.addCell("node.role", "alias:r,role,dc,nodeRole;desc:d:data node, c:client node");
        table.addCell("master", "alias:m;desc:m:master-eligible, *:current master");
        table.addCell("name", "alias:n;desc:node name");

        table.addCell("completion.size", "alias:cs,completionSize;default:false;text-align:right;desc:size of completion");

        table.addCell("fielddata.memory_size", "alias:fm,fielddataMemory;default:false;text-align:right;desc:used fielddata cache");
        table.addCell("fielddata.evictions", "alias:fe,fielddataEvictions;default:false;text-align:right;desc:fielddata evictions");

        table.addCell("filter_cache.memory_size", "alias:fcm,filterCacheMemory;default:false;text-align:right;desc:used filter cache");
        table.addCell("filter_cache.evictions", "alias:fce,filterCacheEvictions;default:false;text-align:right;desc:filter cache evictions");

        table.addCell("flush.total", "alias:ft,flushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("flush.total_time", "alias:ftt,flushTotalTime;default:false;text-align:right;desc:time spent in flush");

        table.addCell("get.current", "alias:gc,getCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("get.time", "alias:gti,getTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("get.total", "alias:gto,getTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("get.exists_time", "alias:geti,getExistsTime;default:false;text-align:right;desc:time spent in successful gets");
        table.addCell("get.exists_total", "alias:geto,getExistsTotal;default:false;text-align:right;desc:number of successful gets");
        table.addCell("get.missing_time", "alias:gmti,getMissingTime;default:false;text-align:right;desc:time spent in failed gets");
        table.addCell("get.missing_total", "alias:gmto,getMissingTotal;default:false;text-align:right;desc:number of failed gets");

        table.addCell("id_cache.memory_size", "alias:im,idCacheMemory;default:false;text-align:right;desc:used id cache");

        table.addCell("indexing.delete_current", "alias:idc,indexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions");
        table.addCell("indexing.delete_time", "alias:idti,indexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("indexing.delete_total", "alias:idto,indexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell("indexing.index_current", "alias:iic,indexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops");
        table.addCell("indexing.index_time", "alias:iiti,indexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("indexing.index_total", "alias:iito,indexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");

        table.addCell("merges.current", "alias:mc,mergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell("merges.current_docs", "alias:mcd,mergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs");
        table.addCell("merges.current_size", "alias:mcs,mergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("merges.total", "alias:mt,mergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("merges.total_docs", "alias:mtd,mergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("merges.total_size", "alias:mts,mergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("merges.total_time", "alias:mtt,mergesTotalTime;default:false;text-align:right;desc:time spent in merges");

        table.addCell("percolate.current", "alias:pc,percolateCurrent;default:false;text-align:right;desc:number of current percolations");
        table.addCell("percolate.memory_size", "alias:pm,percolateMemory;default:false;text-align:right;desc:memory used by percolations");
        table.addCell("percolate.queries", "alias:pq,percolateQueries;default:false;text-align:right;desc:number of registered percolation queries");
        table.addCell("percolate.time", "alias:pti,percolateTime;default:false;text-align:right;desc:time spent percolating");
        table.addCell("percolate.total", "alias:pto,percolateTotal;default:false;text-align:right;desc:total percolations");

        table.addCell("refresh.total", "alias:rto,refreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("refresh.time", "alias:rti,refreshTime;default:false;text-align:right;desc:time spent in refreshes");

        table.addCell("search.fetch_current", "alias:sfc,searchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("search.fetch_time", "alias:sfti,searchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("search.fetch_total", "alias:sfto,searchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("search.open_contexts", "alias:so,searchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("search.query_current", "alias:sqc,searchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("search.query_time", "alias:sqti,searchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("search.query_total", "alias:sqto,searchQueryTotal;default:false;text-align:right;desc:total query phase ops");

        table.addCell("segments.count", "alias:sc,segmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("segments.memory", "alias:sm,segmentsMemory;default:false;text-align:right;desc:memory used by segments");

        table.addCell("suggest.current", "alias:suc,suggestCurrent;default:false;text-align:right;desc:number of current suggest ops");
        table.addCell("suggest.time", "alias:suti,suggestTime;default:false;text-align:right;desc:time spend in suggest");
        table.addCell("suggest.total", "alias:suto,suggestTotal;default:false;text-align:right;desc:number of suggest ops");

        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
        boolean fullId = req.paramAsBoolean("full_id", false);

        DiscoveryNodes nodes = state.getState().nodes();
        String masterId = nodes.masterNodeId();
        Table table = getTableWithHeader(req);

        for (DiscoveryNode node : nodes) {
            NodeInfo info = nodesInfo.getNodesMap().get(node.id());
            NodeStats stats = nodesStats.getNodesMap().get(node.id());

            table.startRow();

            table.addCell(fullId ? node.id() : Strings.substring(node.getId(), 0, 4));
            table.addCell(info == null ? null : info.getProcess().id());
            table.addCell(node.getHostName());
            table.addCell(node.getHostAddress());
            if (node.address() instanceof InetSocketTransportAddress) {
                table.addCell(((InetSocketTransportAddress) node.address()).address().getPort());
            } else {
                table.addCell("-");
            }

            table.addCell(node.getVersion().number());
            table.addCell(info == null ? null : info.getBuild().hashShort());
            table.addCell(info == null ? null : info.getJvm().version());
            table.addCell(stats == null ? null : stats.getFs() == null ? null : stats.getFs().total().getAvailable());
            table.addCell(stats == null ? null : stats.getJvm().getMem().getHeapUsedPrecent());
            table.addCell(info == null ? null : info.getJvm().getMem().getHeapMax());
            table.addCell(stats == null ? null : stats.getOs().mem() == null ? null : stats.getOs().mem().usedPercent());
            table.addCell(info == null ? null : info.getOs().mem() == null ? null : info.getOs().mem().total()); // sigar fails to load in IntelliJ

            table.addCell(stats == null ? null : stats.getOs() == null ? null : stats.getOs().getLoadAverage().length < 1 ? null : String.format(Locale.ROOT, "%.2f", stats.getOs().getLoadAverage()[0]));
            table.addCell(stats == null ? null : stats.getJvm().uptime());
            table.addCell(node.clientNode() ? "c" : node.dataNode() ? "d" : "-");
            table.addCell(masterId == null ? "x" : masterId.equals(node.id()) ? "*" : node.masterNode() ? "m" : "-");
            table.addCell(node.name());

            table.addCell(stats == null ? null : stats.getIndices().getCompletion().getSize());

            table.addCell(stats == null ? null : stats.getIndices().getFieldData().getMemorySize());
            table.addCell(stats == null ? null : stats.getIndices().getFieldData().getEvictions());

            table.addCell(stats == null ? null : stats.getIndices().getFilterCache().getMemorySize());
            table.addCell(stats == null ? null : stats.getIndices().getFilterCache().getEvictions());

            table.addCell(stats == null ? null : stats.getIndices().getFlush().getTotal());
            table.addCell(stats == null ? null : stats.getIndices().getFlush().getTotalTime());

            table.addCell(stats == null ? null : stats.getIndices().getGet().current());
            table.addCell(stats == null ? null : stats.getIndices().getGet().getTime());
            table.addCell(stats == null ? null : stats.getIndices().getGet().getCount());
            table.addCell(stats == null ? null : stats.getIndices().getGet().getExistsTime());
            table.addCell(stats == null ? null : stats.getIndices().getGet().getExistsCount());
            table.addCell(stats == null ? null : stats.getIndices().getGet().getMissingTime());
            table.addCell(stats == null ? null : stats.getIndices().getGet().getMissingCount());

            table.addCell(stats == null ? null : stats.getIndices().getIdCache().getMemorySize());

            table.addCell(stats == null ? null : stats.getIndices().getIndexing().getTotal().getDeleteCurrent());
            table.addCell(stats == null ? null : stats.getIndices().getIndexing().getTotal().getDeleteTime());
            table.addCell(stats == null ? null : stats.getIndices().getIndexing().getTotal().getDeleteCount());
            table.addCell(stats == null ? null : stats.getIndices().getIndexing().getTotal().getIndexCurrent());
            table.addCell(stats == null ? null : stats.getIndices().getIndexing().getTotal().getIndexTime());
            table.addCell(stats == null ? null : stats.getIndices().getIndexing().getTotal().getIndexCount());

            table.addCell(stats == null ? null : stats.getIndices().getMerge().getCurrent());
            table.addCell(stats == null ? null : stats.getIndices().getMerge().getCurrentNumDocs());
            table.addCell(stats == null ? null : stats.getIndices().getMerge().getCurrentSize());
            table.addCell(stats == null ? null : stats.getIndices().getMerge().getTotal());
            table.addCell(stats == null ? null : stats.getIndices().getMerge().getTotalNumDocs());
            table.addCell(stats == null ? null : stats.getIndices().getMerge().getTotalSize());
            table.addCell(stats == null ? null : stats.getIndices().getMerge().getTotalTime());

            table.addCell(stats == null ? null : stats.getIndices().getPercolate().getCurrent());
            table.addCell(stats == null ? null : stats.getIndices().getPercolate().getMemorySize());
            table.addCell(stats == null ? null : stats.getIndices().getPercolate().getNumQueries());
            table.addCell(stats == null ? null : stats.getIndices().getPercolate().getTime());
            table.addCell(stats == null ? null : stats.getIndices().getPercolate().getCount());

            table.addCell(stats == null ? null : stats.getIndices().getRefresh().getTotal());
            table.addCell(stats == null ? null : stats.getIndices().getRefresh().getTotalTime());

            table.addCell(stats == null ? null : stats.getIndices().getSearch().getTotal().getFetchCurrent());
            table.addCell(stats == null ? null : stats.getIndices().getSearch().getTotal().getFetchTime());
            table.addCell(stats == null ? null : stats.getIndices().getSearch().getTotal().getFetchCount());
            table.addCell(stats == null ? null : stats.getIndices().getSearch().getOpenContexts());
            table.addCell(stats == null ? null : stats.getIndices().getSearch().getTotal().getQueryCurrent());
            table.addCell(stats == null ? null : stats.getIndices().getSearch().getTotal().getQueryTime());
            table.addCell(stats == null ? null : stats.getIndices().getSearch().getTotal().getQueryCount());

            table.addCell(stats == null ? null : stats.getIndices().getSegments().getCount());
            table.addCell(stats == null ? null : stats.getIndices().getSegments().getMemory());

            table.addCell(stats == null ? null : stats.getIndices().getSuggest().getCurrent());
            table.addCell(stats == null ? null : stats.getIndices().getSuggest().getTime());
            table.addCell(stats == null ? null : stats.getIndices().getSuggest().getCount());

            table.endRow();
        }

        return table;
    }
}
