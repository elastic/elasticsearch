/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.bulk.stats.BulkStats;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestShardsAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/shards"), new Route(GET, "/_cat/shards/{index}"));
    }

    @Override
    public String getName() {
        return "cat_shards_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
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
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        clusterStateRequest.clear().nodes(true).routingTable(true).indices(indices);
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

        table.addCell("unassigned.reason", "alias:ur;default:false;desc:reason shard became unassigned");
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

        table.addCell(
            "indexing.delete_current",
            "alias:idc,indexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions"
        );
        table.addCell("indexing.delete_time", "alias:idti,indexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("indexing.delete_total", "alias:idto,indexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell(
            "indexing.index_current",
            "alias:iic,indexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops"
        );
        table.addCell("indexing.index_time", "alias:iiti,indexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("indexing.index_total", "alias:iito,indexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");
        table.addCell(
            "indexing.index_failed",
            "alias:iif,indexingIndexFailed;default:false;text-align:right;desc:number of failed indexing ops"
        );

        table.addCell("merges.current", "alias:mc,mergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell(
            "merges.current_docs",
            "alias:mcd,mergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs"
        );
        table.addCell("merges.current_size", "alias:mcs,mergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("merges.total", "alias:mt,mergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("merges.total_docs", "alias:mtd,mergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("merges.total_size", "alias:mts,mergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("merges.total_time", "alias:mtt,mergesTotalTime;default:false;text-align:right;desc:time spent in merges");

        table.addCell("refresh.total", "alias:rto,refreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("refresh.time", "alias:rti,refreshTime;default:false;text-align:right;desc:time spent in refreshes");
        table.addCell("refresh.external_total", "alias:rto,refreshTotal;default:false;text-align:right;desc:total external refreshes");
        table.addCell(
            "refresh.external_time",
            "alias:rti,refreshTime;default:false;text-align:right;desc:time spent in external refreshes"
        );
        table.addCell(
            "refresh.listeners",
            "alias:rli,refreshListeners;default:false;text-align:right;desc:number of pending refresh listeners"
        );

        table.addCell("search.fetch_current", "alias:sfc,searchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("search.fetch_time", "alias:sfti,searchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("search.fetch_total", "alias:sfto,searchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("search.open_contexts", "alias:so,searchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("search.query_current", "alias:sqc,searchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("search.query_time", "alias:sqti,searchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("search.query_total", "alias:sqto,searchQueryTotal;default:false;text-align:right;desc:total query phase ops");
        table.addCell("search.scroll_current", "alias:scc,searchScrollCurrent;default:false;text-align:right;desc:open scroll contexts");
        table.addCell(
            "search.scroll_time",
            "alias:scti,searchScrollTime;default:false;text-align:right;desc:time scroll contexts held open"
        );
        table.addCell("search.scroll_total", "alias:scto,searchScrollTotal;default:false;text-align:right;desc:completed scroll contexts");

        table.addCell("segments.count", "alias:sc,segmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("segments.memory", "alias:sm,segmentsMemory;default:false;text-align:right;desc:memory used by segments");
        table.addCell(
            "segments.index_writer_memory",
            "alias:siwm,segmentsIndexWriterMemory;default:false;text-align:right;desc:memory used by index writer"
        );
        table.addCell(
            "segments.version_map_memory",
            "alias:svmm,segmentsVersionMapMemory;default:false;text-align:right;desc:memory used by version map"
        );
        table.addCell(
            "segments.fixed_bitset_memory",
            "alias:sfbm,fixedBitsetMemory;default:false;text-align:right;desc:memory used by fixed bit sets for nested object"
                + " field types and type filters for types referred in _parent fields"
        );

        table.addCell("seq_no.max", "alias:sqm,maxSeqNo;default:false;text-align:right;desc:max sequence number");
        table.addCell("seq_no.local_checkpoint", "alias:sql,localCheckpoint;default:false;text-align:right;desc:local checkpoint");
        table.addCell("seq_no.global_checkpoint", "alias:sqg,globalCheckpoint;default:false;text-align:right;desc:global checkpoint");

        table.addCell("warmer.current", "alias:wc,warmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("warmer.total", "alias:wto,warmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("warmer.total_time", "alias:wtt,warmerTotalTime;default:false;text-align:right;desc:time spent in warmers");

        table.addCell("path.data", "alias:pd,dataPath;default:false;text-align:right;desc:shard data path");
        table.addCell("path.state", "alias:ps,statsPath;default:false;text-align:right;desc:shard state path");

        table.addCell(
            "bulk.total_operations",
            "alias:bto,bulkTotalOperations;default:false;text-align:right;desc:number of bulk shard ops"
        );
        table.addCell("bulk.total_time", "alias:btti,bulkTotalTime;default:false;text-align:right;desc:time spend in shard bulk");
        table.addCell(
            "bulk.total_size_in_bytes",
            "alias:btsi,bulkTotalSizeInBytes;default:false;text-align:right;desc:total size in bytes of shard bulk"
        );
        table.addCell("bulk.avg_time", "alias:bati,bulkAvgTime;default:false;text-align:right;desc:average time spend in shard bulk");
        table.addCell(
            "bulk.avg_size_in_bytes",
            "alias:basi,bulkAvgSizeInBytes;default:false;text-align:right;desc:avg size in bytes of shard bulk"
        );

        table.endHeaders();
        return table;
    }

    private static <S, T> Object getOrNull(S stats, Function<S, T> accessor, Function<T, Object> func) {
        if (stats != null) {
            T t = accessor.apply(stats);
            if (t != null) {
                return func.apply(t);
            }
        }
        return null;
    }

    // package private for testing
    Table buildTable(RestRequest request, ClusterStateResponse state, IndicesStatsResponse stats) {
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

            if (shard.primary()) {
                table.addCell("p");
            } else {
                table.addCell("r");
            }
            table.addCell(shard.state());
            table.addCell(getOrNull(commonStats, CommonStats::getDocs, DocsStats::getCount));
            table.addCell(getOrNull(commonStats, CommonStats::getStore, StoreStats::getSize));
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
                Instant unassignedTime = Instant.ofEpochMilli(shard.unassignedInfo().getUnassignedTimeInMillis());
                table.addCell(UnassignedInfo.DATE_TIME_FORMATTER.format(unassignedTime));
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

            table.addCell(getOrNull(commonStats, CommonStats::getCompletion, CompletionStats::getSize));

            table.addCell(getOrNull(commonStats, CommonStats::getFieldData, FieldDataStats::getMemorySize));
            table.addCell(getOrNull(commonStats, CommonStats::getFieldData, FieldDataStats::getEvictions));

            table.addCell(getOrNull(commonStats, CommonStats::getQueryCache, QueryCacheStats::getMemorySize));
            table.addCell(getOrNull(commonStats, CommonStats::getQueryCache, QueryCacheStats::getEvictions));

            table.addCell(getOrNull(commonStats, CommonStats::getFlush, FlushStats::getTotal));
            table.addCell(getOrNull(commonStats, CommonStats::getFlush, FlushStats::getTotalTime));

            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::current));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getTime));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getCount));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getExistsTime));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getExistsCount));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getMissingTime));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getMissingCount));

            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getDeleteCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getDeleteTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getDeleteCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getIndexCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getIndexTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getIndexCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getIndexFailedCount()));

            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrent));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrentNumDocs));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrentSize));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotal));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalNumDocs));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalSize));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalTime));

            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getTotal));
            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getTotalTime));
            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getExternalTotal));
            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getExternalTotalTime));
            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getListeners));

            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getFetchCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getFetchTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getFetchCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, SearchStats::getOpenContexts));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getQueryCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getQueryTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getQueryCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getScrollCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getScrollTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getScrollCount()));

            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getCount));
            table.addCell(getOrNull(commonStats, CommonStats::getSegments, ss -> new ByteSizeValue(0)));
            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getIndexWriterMemory));
            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getVersionMapMemory));
            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getBitsetMemory));

            table.addCell(getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getMaxSeqNo));
            table.addCell(getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getLocalCheckpoint));
            table.addCell(getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getGlobalCheckpoint));

            table.addCell(getOrNull(commonStats, CommonStats::getWarmer, WarmerStats::current));
            table.addCell(getOrNull(commonStats, CommonStats::getWarmer, WarmerStats::total));
            table.addCell(getOrNull(commonStats, CommonStats::getWarmer, WarmerStats::totalTime));

            table.addCell(getOrNull(shardStats, ShardStats::getDataPath, s -> s));
            table.addCell(getOrNull(shardStats, ShardStats::getStatePath, s -> s));

            table.addCell(getOrNull(commonStats, CommonStats::getBulk, BulkStats::getTotalOperations));
            table.addCell(getOrNull(commonStats, CommonStats::getBulk, BulkStats::getTotalTime));
            table.addCell(getOrNull(commonStats, CommonStats::getBulk, BulkStats::getTotalSizeInBytes));
            table.addCell(getOrNull(commonStats, CommonStats::getBulk, BulkStats::getAvgTime));
            table.addCell(getOrNull(commonStats, CommonStats::getBulk, BulkStats::getAvgSizeInBytes));

            table.endRow();
        }

        return table;
    }
}
