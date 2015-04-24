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
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.id.IdCacheStats;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.percolator.stats.PercolateStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.suggest.stats.SuggestStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActionListener;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodesAction extends AbstractCatAction {

    @Inject
    public RestNodesAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
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
                        nodesStatsRequest.clear().jvm(true).os(true).fs(true).indices(true).process(true);
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
        table.addCell("heap.current", "default:false;alias:hc,heapCurrent;text-align:right;desc:used heap");
        table.addCell("heap.percent", "alias:hp,heapPercent;text-align:right;desc:used heap ratio");
        table.addCell("heap.max", "default:false;alias:hm,heapMax;text-align:right;desc:max configured heap");
        table.addCell("ram.current", "default:false;alias:rc,ramCurrent;text-align:right;desc:used machine memory");
        table.addCell("ram.percent", "alias:rp,ramPercent;text-align:right;desc:used machine memory ratio");
        table.addCell("ram.max", "default:false;alias:rm,ramMax;text-align:right;desc:total machine memory");
        table.addCell("file_desc.current", "default:false;alias:fdc,fileDescriptorCurrent;text-align:right;desc:used file descriptors");
        table.addCell("file_desc.percent", "default:false;alias:fdp,fileDescriptorPercent;text-align:right;desc:used file descriptor ratio");
        table.addCell("file_desc.max", "default:false;alias:fdm,fileDescriptorMax;text-align:right;desc:max file descriptors");

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

        table.addCell("query_cache.memory_size", "alias:qcm,queryCacheMemory;default:false;text-align:right;desc:used query cache");
        table.addCell("query_cache.evictions", "alias:qce,queryCacheEvictions;default:false;text-align:right;desc:query cache evictions");
        table.addCell("query_cache.hit_count", "alias:qchc,queryCacheHitCount;default:false;text-align:right;desc:query cache hit counts");
        table.addCell("query_cache.miss_count", "alias:qcmc,queryCacheMissCount;default:false;text-align:right;desc:query cache miss counts");

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
        table.addCell("segments.index_writer_memory", "alias:siwm,segmentsIndexWriterMemory;default:false;text-align:right;desc:memory used by index writer");
        table.addCell("segments.index_writer_max_memory", "alias:siwmx,segmentsIndexWriterMaxMemory;default:false;text-align:right;desc:maximum memory index writer may use before it must write buffered documents to a new segment");
        table.addCell("segments.version_map_memory", "alias:svmm,segmentsVersionMapMemory;default:false;text-align:right;desc:memory used by version map");
        table.addCell("segments.fixed_bitset_memory", "alias:sfbm,fixedBitsetMemory;default:false;text-align:right;desc:memory used by fixed bit sets for nested object field types and type filters for types referred in _parent fields");

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

            JvmInfo jvmInfo = info == null ? null : info.getJvm();
            OsInfo osInfo = info == null ? null : info.getOs();
            ProcessInfo processInfo = info == null ? null : info.getProcess();

            JvmStats jvmStats = stats == null ? null : stats.getJvm();
            FsStats fsStats = stats == null ? null : stats.getFs();
            OsStats osStats = stats == null ? null : stats.getOs();
            ProcessStats processStats = stats == null ? null : stats.getProcess();
            NodeIndicesStats indicesStats = stats == null ? null : stats.getIndices();

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
            table.addCell(jvmInfo == null ? null : jvmInfo.version());
            table.addCell(fsStats == null ? null : fsStats.getTotal().getAvailable());
            table.addCell(jvmStats == null ? null : jvmStats.getMem().getHeapUsed());
            table.addCell(jvmStats == null ? null : jvmStats.getMem().getHeapUsedPercent());
            table.addCell(jvmInfo == null ? null : jvmInfo.getMem().getHeapMax());
            table.addCell(osStats == null ? null : osStats.getMem() == null ? null : osStats.getMem().used());
            table.addCell(osStats == null ? null : osStats.getMem() == null ? null : osStats.getMem().usedPercent());
            table.addCell(osInfo == null ? null : osInfo.getMem() == null ? null : osInfo.getMem().total()); // sigar fails to load in IntelliJ
            table.addCell(processStats == null ? null : processStats.getOpenFileDescriptors());
            table.addCell(processStats == null || processInfo == null ? null :
                          calculatePercentage(processStats.getOpenFileDescriptors(), processInfo.getMaxFileDescriptors()));
            table.addCell(processInfo == null ? null : processInfo.getMaxFileDescriptors());

            table.addCell(osStats == null ? null : osStats.getLoadAverage().length < 1 ? null : String.format(Locale.ROOT, "%.2f", osStats.getLoadAverage()[0]));
            table.addCell(jvmStats == null ? null : jvmStats.getUptime());
            table.addCell(node.clientNode() ? "c" : node.dataNode() ? "d" : "-");
            table.addCell(masterId == null ? "x" : masterId.equals(node.id()) ? "*" : node.masterNode() ? "m" : "-");
            table.addCell(node.name());

            CompletionStats completionStats = indicesStats == null ? null : stats.getIndices().getCompletion();
            table.addCell(completionStats == null ? null : completionStats.getSize());

            FieldDataStats fdStats = indicesStats == null ? null : stats.getIndices().getFieldData();
            table.addCell(fdStats == null ? null : fdStats.getMemorySize());
            table.addCell(fdStats == null ? null : fdStats.getEvictions());

            FilterCacheStats fcStats = indicesStats == null ? null : indicesStats.getFilterCache();
            table.addCell(fcStats == null ? null : fcStats.getMemorySize());
            table.addCell(fcStats == null ? null : fcStats.getEvictions());

            QueryCacheStats qcStats = indicesStats == null ? null : indicesStats.getQueryCache();
            table.addCell(qcStats == null ? null : qcStats.getMemorySize());
            table.addCell(qcStats == null ? null : qcStats.getEvictions());
            table.addCell(qcStats == null ? null : qcStats.getHitCount());
            table.addCell(qcStats == null ? null : qcStats.getMissCount());

            FlushStats flushStats = indicesStats == null ? null : indicesStats.getFlush();
            table.addCell(flushStats == null ? null : flushStats.getTotal());
            table.addCell(flushStats == null ? null : flushStats.getTotalTime());

            GetStats getStats = indicesStats == null ? null : indicesStats.getGet();
            table.addCell(getStats == null ? null : getStats.current());
            table.addCell(getStats == null ? null : getStats.getTime());
            table.addCell(getStats == null ? null : getStats.getCount());
            table.addCell(getStats == null ? null : getStats.getExistsTime());
            table.addCell(getStats == null ? null : getStats.getExistsCount());
            table.addCell(getStats == null ? null : getStats.getMissingTime());
            table.addCell(getStats == null ? null : getStats.getMissingCount());

            IdCacheStats idCacheStats = indicesStats == null ? null : indicesStats.getIdCache();
            table.addCell(idCacheStats == null ? null : idCacheStats.getMemorySize());

            IndexingStats indexingStats = indicesStats == null ? null : indicesStats.getIndexing();
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getDeleteCurrent());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getDeleteTime());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getDeleteCount());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getIndexCurrent());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getIndexTime());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getIndexCount());

            MergeStats mergeStats = indicesStats == null ? null : indicesStats.getMerge();
            table.addCell(mergeStats == null ? null : mergeStats.getCurrent());
            table.addCell(mergeStats == null ? null : mergeStats.getCurrentNumDocs());
            table.addCell(mergeStats == null ? null : mergeStats.getCurrentSize());
            table.addCell(mergeStats == null ? null : mergeStats.getTotal());
            table.addCell(mergeStats == null ? null : mergeStats.getTotalNumDocs());
            table.addCell(mergeStats == null ? null : mergeStats.getTotalSize());
            table.addCell(mergeStats == null ? null : mergeStats.getTotalTime());

            PercolateStats percolateStats = indicesStats == null ? null : indicesStats.getPercolate();
            table.addCell(percolateStats == null ? null : percolateStats.getCurrent());
            table.addCell(percolateStats == null ? null : percolateStats.getMemorySize());
            table.addCell(percolateStats == null ? null : percolateStats.getNumQueries());
            table.addCell(percolateStats == null ? null : percolateStats.getTime());
            table.addCell(percolateStats == null ? null : percolateStats.getCount());

            RefreshStats refreshStats = indicesStats == null ? null : indicesStats.getRefresh();
            table.addCell(refreshStats == null ? null : refreshStats.getTotal());
            table.addCell(refreshStats == null ? null : refreshStats.getTotalTime());

            SearchStats searchStats = indicesStats == null ? null : indicesStats.getSearch();
            table.addCell(searchStats == null ? null : searchStats.getTotal().getFetchCurrent());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getFetchTime());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getFetchCount());
            table.addCell(searchStats == null ? null : searchStats.getOpenContexts());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getQueryCurrent());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getQueryTime());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getQueryCount());

            SegmentsStats segmentsStats = indicesStats == null ? null : indicesStats.getSegments();
            table.addCell(segmentsStats == null ? null : segmentsStats.getCount());
            table.addCell(segmentsStats == null ? null : segmentsStats.getMemory());
            table.addCell(segmentsStats == null ? null : segmentsStats.getIndexWriterMemory());
            table.addCell(segmentsStats == null ? null : segmentsStats.getIndexWriterMaxMemory());
            table.addCell(segmentsStats == null ? null : segmentsStats.getVersionMapMemory());
            table.addCell(segmentsStats == null ? null : segmentsStats.getBitsetMemory());

            SuggestStats suggestStats = indicesStats == null ? null : indicesStats.getSuggest();
            table.addCell(suggestStats == null ? null : suggestStats.getCurrent());
            table.addCell(suggestStats == null ? null : suggestStats.getTime());
            table.addCell(suggestStats == null ? null : suggestStats.getCount());

            table.endRow();
        }

        return table;
    }

    /**
     * Calculate the percentage of {@code used} from the {@code max} number.
     * @param used The currently used number.
     * @param max The maximum number.
     * @return 0 if {@code max} is <= 0. Otherwise 100 * {@code used} / {@code max}.
     */
    private short calculatePercentage(long used, long max) {
        return max <= 0 ? 0 : (short)((100d * used) / max);
    }
}
