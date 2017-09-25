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

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestShardsAction extends AbstractCatAction {
    public RestShardsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_cat/shards", this);
        controller.registerHandler(GET, "/_cat/shards/{index}", this);
    }

    @Override
    public String getName() {
        return "cat_shards_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/shards\n");
        sb.append("/_cat/shards/{index}\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        clusterStateRequest.clear().nodes(true).metaData(true).routingTable(true).indices(indices);
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                indicesStatsRequest.all();
                indicesStatsRequest.indices(indices);
                client.admin().indices().stats(indicesStatsRequest, new RestResponseListener<IndicesStatsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(IndicesStatsResponse indicesStatsResponse) throws Exception {
                        return RestTable.buildResponse(buildTable(request, clusterStateResponse, indicesStatsResponse), channel);
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders()
                .addCell("index", "default:true;alias:i,idx;desc:index name")
                .addCell("shard", "default:true;alias:s,sh;desc:shard name")
                .addCell("prirep", "alias:p,pr,primaryOrReplica;default:true;desc:primary or replica")
                .addCell("state", "default:true;alias:st;desc:shard state")
                .addCell("docs", "alias:d,dc;text-align:right;desc:number of docs in shard")
                .addCell("store", "alias:sto;text-align:right;desc:store size of shard (how much disk it uses)")
                .addCell("ip", "default:true;desc:ip of node where it lives")
                .addCell("id", "default:false;desc:unique id of node where it lives")
                .addCell("node", "default:true;alias:n;desc:name of node where it lives");

        table.addCell("sync_id", "alias:sync_id;default:false;desc:sync id");

        table.addCell("unassigned.reason", "alias:ur;default:false;desc:reason shard is unassigned");
        table.addCell("unassigned.at", "alias:ua;default:false;desc:time shard became unassigned (UTC)");
        table.addCell("unassigned.for", "alias:uf;default:false;text-align:right;desc:time has been unassigned");
        table.addCell("unassigned.details", "alias:ud;default:false;desc:additional details as to why the shard became unassigned");

        table.addCell("recoverysource.type", "alias:rs;default:false;desc:recovery source type");

        table.addCell("completion.size", "alias:cs,completionSize;default:false;text-align:right;desc:size of completion");

        table.addCell("fielddata.memory_size", "alias:fm,fielddataMemory;default:false;text-align:right;desc:used fielddata cache");
        table.addCell("fielddata.evictions", "alias:fe,fielddataEvictions;default:false;text-align:right;desc:fielddata evictions");

        table.addCell("query_cache.memory_size", "alias:qcm,queryCacheMemory;default:false;text-align:right;desc:used query cache");
        table.addCell("query_cache.evictions", "alias:qce,queryCacheEvictions;default:false;text-align:right;desc:query cache evictions");

        table.addCell("flush.total", "alias:ft,flushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("flush.total_time", "alias:ftt,flushTotalTime;default:false;text-align:right;desc:time spent in flush");

        table.addCell("get.current", "alias:gc,getCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("get.time", "alias:gti,getTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("get.total", "alias:gto,getTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("get.exists_time", "alias:geti,getExistsTime;default:false;text-align:right;desc:time spent in successful gets");
        table.addCell("get.exists_total", "alias:geto,getExistsTotal;default:false;text-align:right;desc:number of successful gets");
        table.addCell("get.missing_time", "alias:gmti,getMissingTime;default:false;text-align:right;desc:time spent in failed gets");
        table.addCell("get.missing_total", "alias:gmto,getMissingTotal;default:false;text-align:right;desc:number of failed gets");

        table.addCell("indexing.delete_current", "alias:idc,indexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions");
        table.addCell("indexing.delete_time", "alias:idti,indexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("indexing.delete_total", "alias:idto,indexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell("indexing.index_current", "alias:iic,indexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops");
        table.addCell("indexing.index_time", "alias:iiti,indexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("indexing.index_total", "alias:iito,indexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");
        table.addCell("indexing.index_failed", "alias:iif,indexingIndexFailed;default:false;text-align:right;desc:number of failed indexing ops");

        table.addCell("merges.current", "alias:mc,mergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell("merges.current_docs", "alias:mcd,mergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs");
        table.addCell("merges.current_size", "alias:mcs,mergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("merges.total", "alias:mt,mergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("merges.total_docs", "alias:mtd,mergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("merges.total_size", "alias:mts,mergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("merges.total_time", "alias:mtt,mergesTotalTime;default:false;text-align:right;desc:time spent in merges");

        table.addCell("refresh.total", "alias:rto,refreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("refresh.time", "alias:rti,refreshTime;default:false;text-align:right;desc:time spent in refreshes");
        table.addCell("refresh.listeners", "alias:rli,refreshListeners;default:false;text-align:right;desc:number of pending refresh listeners");

        table.addCell("search.fetch_current", "alias:sfc,searchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("search.fetch_time", "alias:sfti,searchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("search.fetch_total", "alias:sfto,searchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("search.open_contexts", "alias:so,searchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("search.query_current", "alias:sqc,searchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("search.query_time", "alias:sqti,searchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("search.query_total", "alias:sqto,searchQueryTotal;default:false;text-align:right;desc:total query phase ops");
        table.addCell("search.scroll_current", "alias:scc,searchScrollCurrent;default:false;text-align:right;desc:open scroll contexts");
        table.addCell("search.scroll_time", "alias:scti,searchScrollTime;default:false;text-align:right;desc:time scroll contexts held open");
        table.addCell("search.scroll_total", "alias:scto,searchScrollTotal;default:false;text-align:right;desc:completed scroll contexts");

        table.addCell("segments.count", "alias:sc,segmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("segments.memory", "alias:sm,segmentsMemory;default:false;text-align:right;desc:memory used by segments");
        table.addCell("segments.index_writer_memory", "alias:siwm,segmentsIndexWriterMemory;default:false;text-align:right;desc:memory used by index writer");
        table.addCell("segments.version_map_memory", "alias:svmm,segmentsVersionMapMemory;default:false;text-align:right;desc:memory used by version map");
        table.addCell("segments.fixed_bitset_memory", "alias:sfbm,fixedBitsetMemory;default:false;text-align:right;desc:memory used by fixed bit sets for nested object field types and type filters for types referred in _parent fields");

        table.addCell("seq_no.max", "alias:sqm,maxSeqNo;default:false;text-align:right;desc:max sequence number");
        table.addCell("seq_no.local_checkpoint", "alias:sql,localCheckpoint;default:false;text-align:right;desc:local checkpoint");
        table.addCell("seq_no.global_checkpoint", "alias:sqg,globalCheckpoint;default:false;text-align:right;desc:global checkpoint");

        table.addCell("warmer.current", "alias:wc,warmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("warmer.total", "alias:wto,warmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("warmer.total_time", "alias:wtt,warmerTotalTime;default:false;text-align:right;desc:time spent in warmers");

        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, ClusterStateResponse state, IndicesStatsResponse stats) {
        Table table = getTableWithHeader(request);

        for (ShardRouting shard : state.getState().routingTable().allShards()) {
            ShardStats shardStats = stats.asMap().get(shard);
            CommonStats commonStats = null;
            CommitStats commitStats = null;
            if (shardStats != null) {
                commonStats = shardStats.getStats();
                commitStats = shardStats.getCommitStats();
            }

            table.startRow();

            table.addCell(shard.getIndexName());
            table.addCell(shard.id());

            IndexMetaData indexMeta = state.getState().getMetaData().getIndexSafe(shard.index());
            if (shard.primary()) {
                table.addCell("p");
            } else {
                table.addCell("r");
            }
            table.addCell(shard.state());
            table.addCell(commonStats == null ? null : commonStats.getDocs().getCount());
            table.addCell(commonStats == null ? null : commonStats.getStore().getSize());
            if (shard.assignedToNode()) {
                String ip = state.getState().nodes().get(shard.currentNodeId()).getHostAddress();
                String nodeId = shard.currentNodeId();
                StringBuilder name = new StringBuilder();
                name.append(state.getState().nodes().get(shard.currentNodeId()).getName());
                if (shard.relocating()) {
                    String reloIp = state.getState().nodes().get(shard.relocatingNodeId()).getHostAddress();
                    String reloNme = state.getState().nodes().get(shard.relocatingNodeId()).getName();
                    String reloNodeId = shard.relocatingNodeId();
                    name.append(" -> ");
                    name.append(reloIp);
                    name.append(" ");
                    name.append(reloNodeId);
                    name.append(" ");
                    name.append(reloNme);
                }
                table.addCell(ip);
                table.addCell(nodeId);
                table.addCell(name);
            } else {
                table.addCell(null);
                table.addCell(null);
                table.addCell(null);
            }

            table.addCell(commitStats == null ? null : commitStats.getUserData().get(Engine.SYNC_COMMIT_ID));

            if (shard.unassignedInfo() != null) {
                table.addCell(shard.unassignedInfo().getReason());
                table.addCell(UnassignedInfo.DATE_TIME_FORMATTER.printer().print(shard.unassignedInfo().getUnassignedTimeInMillis()));
                table.addCell(TimeValue.timeValueMillis(System.currentTimeMillis() - shard.unassignedInfo().getUnassignedTimeInMillis()));
                table.addCell(shard.unassignedInfo().getDetails());
            } else {
                table.addCell(null);
                table.addCell(null);
                table.addCell(null);
                table.addCell(null);
            }

            if (shard.recoverySource() != null) {
                table.addCell(shard.recoverySource().getType().toString().toLowerCase(Locale.ROOT));
            } else {
                table.addCell(null);
            }

            table.addCell(commonStats == null ? null : commonStats.getCompletion().getSize());

            table.addCell(commonStats == null ? null : commonStats.getFieldData().getMemorySize());
            table.addCell(commonStats == null ? null : commonStats.getFieldData().getEvictions());

            table.addCell(commonStats == null ? null : commonStats.getQueryCache().getMemorySize());
            table.addCell(commonStats == null ? null : commonStats.getQueryCache().getEvictions());

            table.addCell(commonStats == null ? null : commonStats.getFlush().getTotal());
            table.addCell(commonStats == null ? null : commonStats.getFlush().getTotalTime());

            table.addCell(commonStats == null ? null : commonStats.getGet().current());
            table.addCell(commonStats == null ? null : commonStats.getGet().getTime());
            table.addCell(commonStats == null ? null : commonStats.getGet().getCount());
            table.addCell(commonStats == null ? null : commonStats.getGet().getExistsTime());
            table.addCell(commonStats == null ? null : commonStats.getGet().getExistsCount());
            table.addCell(commonStats == null ? null : commonStats.getGet().getMissingTime());
            table.addCell(commonStats == null ? null : commonStats.getGet().getMissingCount());

            table.addCell(commonStats == null ? null : commonStats.getIndexing().getTotal().getDeleteCurrent());
            table.addCell(commonStats == null ? null : commonStats.getIndexing().getTotal().getDeleteTime());
            table.addCell(commonStats == null ? null : commonStats.getIndexing().getTotal().getDeleteCount());
            table.addCell(commonStats == null ? null : commonStats.getIndexing().getTotal().getIndexCurrent());
            table.addCell(commonStats == null ? null : commonStats.getIndexing().getTotal().getIndexTime());
            table.addCell(commonStats == null ? null : commonStats.getIndexing().getTotal().getIndexCount());
            table.addCell(commonStats == null ? null : commonStats.getIndexing().getTotal().getIndexFailedCount());

            table.addCell(commonStats == null ? null : commonStats.getMerge().getCurrent());
            table.addCell(commonStats == null ? null : commonStats.getMerge().getCurrentNumDocs());
            table.addCell(commonStats == null ? null : commonStats.getMerge().getCurrentSize());
            table.addCell(commonStats == null ? null : commonStats.getMerge().getTotal());
            table.addCell(commonStats == null ? null : commonStats.getMerge().getTotalNumDocs());
            table.addCell(commonStats == null ? null : commonStats.getMerge().getTotalSize());
            table.addCell(commonStats == null ? null : commonStats.getMerge().getTotalTime());

            table.addCell(commonStats == null ? null : commonStats.getRefresh().getTotal());
            table.addCell(commonStats == null ? null : commonStats.getRefresh().getTotalTime());
            table.addCell(commonStats == null ? null : commonStats.getRefresh().getListeners());

            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getFetchCurrent());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getFetchTime());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getFetchCount());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getOpenContexts());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getQueryCurrent());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getQueryTime());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getQueryCount());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getScrollCurrent());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getScrollTime());
            table.addCell(commonStats == null ? null : commonStats.getSearch().getTotal().getScrollCount());

            table.addCell(commonStats == null ? null : commonStats.getSegments().getCount());
            table.addCell(commonStats == null ? null : commonStats.getSegments().getMemory());
            table.addCell(commonStats == null ? null : commonStats.getSegments().getIndexWriterMemory());
            table.addCell(commonStats == null ? null : commonStats.getSegments().getVersionMapMemory());
            table.addCell(commonStats == null ? null : commonStats.getSegments().getBitsetMemory());

            table.addCell(shardStats == null || shardStats.getSeqNoStats() == null ? null : shardStats.getSeqNoStats().getMaxSeqNo());
            table.addCell(shardStats == null || shardStats.getSeqNoStats() == null ? null : shardStats.getSeqNoStats().getLocalCheckpoint());
            table.addCell(commitStats == null || shardStats.getSeqNoStats() == null ? null : shardStats.getSeqNoStats().getGlobalCheckpoint());

            table.addCell(commonStats == null ? null : commonStats.getWarmer().current());
            table.addCell(commonStats == null ? null : commonStats.getWarmer().total());
            table.addCell(commonStats == null ? null : commonStats.getWarmer().totalTime());

            table.endRow();
        }

        return table;
    }
}
