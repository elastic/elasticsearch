/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Sets;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestShardsAction extends AbstractCatAction {

    @Inject
    public RestShardsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/shards", this);
        controller.registerHandler(GET, "/_cat/shards/{index}", this);
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/shards\n");
        sb.append("/_cat/shards/{index}\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                final String[] concreteIndices = clusterStateResponse.getState().metaData().concreteIndicesIgnoreMissing(indices);
                IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                indicesStatsRequest.all();
                client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                    @Override
                    public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                        try {
                            channel.sendResponse(RestTable.buildResponse(buildTable(request, concreteIndices, clusterStateResponse, indicesStatsResponse), request, channel));
                        } catch (Throwable e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(new XContentThrowableRestResponse(request, e));
                        } catch (IOException e1) {
                            logger.error("Failed to send failure response", e1);
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override
    Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders()
                .addCell("index", "default:true;desc:index name")
                .addCell("shard", "default:true;desc:shard name")
                .addCell("p/r", "default:true;desc:primary or replica")
                .addCell("state", "default:true;desc:shard state")
                .addCell("docs", "text-align:right;desc:number of docs in shard")
                .addCell("store", "text-align:right;desc:store size of shard (how much disk it uses)")
                .addCell("ip", "default:true;desc:ip of node where it lives")
                .addCell("node", "default:true;desc:name of node where it lives");

        table.addCell("total.completion.size", "alias:tcs,totalCompletionSize;default:false;text-align:right;desc:size of completion");

        table.addCell("total.fielddata.memory_size", "alias:tfm,totalFielddataMemory;default:false;text-align:right;desc:used fielddata cache");
        table.addCell("total.fielddata.evictions", "alias:tfe,totalFielddataEvictions;default:false;text-align:right;desc:fielddata evictions");

        table.addCell("total.filter_cache.memory_size", "alias:tfcm,totalFilterCacheMemory;default:false;text-align:right;desc:used filter cache");
        table.addCell("total.filter_cache.evictions", "alias:tfce,totalFilterCacheEvictions;default:false;text-align:right;desc:filter cache evictions");

        table.addCell("total.flush.total", "alias:tft,totalFlushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("total.flush.total_time", "alias:tftt,totalFlushTotalTime;default:false;text-align:right;desc:time spent in flush");

        table.addCell("total.get.current", "alias:tgc,totalGetCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("total.get.time", "alias:tgti,totalGetTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("total.get.total", "alias:tgto,totalGetTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("total.get.exists_time", "alias:tgeti,totalGetExistsTime;default:false;text-align:right;desc:time spent in successful gets");
        table.addCell("total.get.exists_total", "alias:tgeto,totalGetExistsTotal;default:false;text-align:right;desc:number of successful gets");
        table.addCell("total.get.missing_time", "alias:tgmti,totalGetMissingTime;default:false;text-align:right;desc:time spent in failed gets");
        table.addCell("total.get.missing_total", "alias:tgmto,totalGetMissingTotal;default:false;text-align:right;desc:number of failed gets");

        table.addCell("total.id_cache.memory_size", "alias:tim,totalIdCacheMemory;default:false;text-align:right;desc:used id cache");

        table.addCell("total.indexing.delete_current", "alias:tidc,totalIndexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions");
        table.addCell("total.indexing.delete_time", "alias:tidti,totalIndexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("total.indexing.delete_total", "alias:tidto,totalIndexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell("total.indexing.index_current", "alias:tiic,totalIndexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops");
        table.addCell("total.indexing.index_time", "alias:tiiti,totalIndexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("total.indexing.index_total", "alias:tiito,totalIndexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");

        table.addCell("total.merges.current", "alias:tmc,totalMergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell("total.merges.current_docs", "alias:tmcd,totalMergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs");
        table.addCell("total.merges.current_size", "alias:tmcs,totalMergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("total.merges.total", "alias:tmt,totalMergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("total.merges.total_docs", "alias:tmtd,totalMergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("total.merges.total_size", "alias:tmts,totalMergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("total.merges.total_time", "alias:tmtt,totalMergesTotalTime;default:false;text-align:right;desc:time spent in merges");

        table.addCell("total.percolate.current", "alias:tpc,totalPercolateCurrent;default:false;text-align:right;desc:number of current percolations");
        table.addCell("total.percolate.memory_size", "alias:tpm,totalPercolateMemory;default:false;text-align:right;desc:memory used by percolations");
        table.addCell("total.percolate.queries", "alias:tpq,totalPercolateQueries;default:false;text-align:right;desc:number of registered percolation queries");
        table.addCell("total.percolate.time", "alias:tpti,totalPercolateTime;default:false;text-align:right;desc:time spent percolating");
        table.addCell("total.percolate.total", "alias:tpto,totalPercolateTotal;default:false;text-align:right;desc:total percolations");

        table.addCell("total.refresh.total", "alias:trto,totalRefreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("total.refresh.time", "alias:trti,totalRefreshTime;default:false;text-align:right;desc:time spent in refreshes");

        table.addCell("total.search.fetch_current", "alias:tsfc,totalSearchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("total.search.fetch_time", "alias:tsfti,totalSearchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("total.search.fetch_total", "alias:tsfto,totalSearchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("total.search.open_contexts", "alias:tso,totalSearchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("total.search.query_current", "alias:tsqc,totalSearchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("total.search.query_time", "alias:tsqti,totalSearchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("total.search.query_total", "alias:tsqto,totalSearchQueryTotal;default:false;text-align:right;desc:total query phase ops");

        table.addCell("total.segments.count", "alias:tsc,totalSegmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("total.segments.memory", "alias:tsm,totalSegmentsMemory;default:false;text-align:right;desc:memory used by segments");

        table.addCell("total.warmer.current", "alias:twc,totalWarmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("total.warmer.total", "alias:twto,totalWarmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("total.warmer.total_time", "alias:twtt,totalWarmerTotalTime;default:false;text-align:right;desc:time spent in warmers");

        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, String[] concreteIndices, ClusterStateResponse state, IndicesStatsResponse stats) {
        Table table = getTableWithHeader(request);

        Set<String> indices = Sets.newHashSet(concreteIndices);
        for (ShardRouting shard : state.getState().routingTable().allShards()) {
            if (!indices.contains(shard.index())) {
                continue;
            }

            CommonStats shardStats = stats.asMap().get(shard);

            table.startRow();

            table.addCell(shard.index());
            table.addCell(shard.id());
            table.addCell(shard.primary() ? "p" : "r");
            table.addCell(shard.state());
            table.addCell(shardStats == null ? null : shardStats.getDocs().getCount());
            table.addCell(shardStats == null ? null : shardStats.getStore().getSize());
            if (shard.assignedToNode()) {
                String ip = ((InetSocketTransportAddress) state.getState().nodes().get(shard.currentNodeId()).address()).address().getAddress().getHostAddress();
                StringBuilder name = new StringBuilder();
                name.append(state.getState().nodes().get(shard.currentNodeId()).name());
                if (shard.relocating()) {
                    String reloIp = ((InetSocketTransportAddress) state.getState().nodes().get(shard.relocatingNodeId()).address()).address().getAddress().getHostAddress();
                    String reloNme = state.getState().nodes().get(shard.relocatingNodeId()).name();
                    name.append(" -> ");
                    name.append(reloIp);
                    name.append(" ");
                    name.append(reloNme);
                }
                table.addCell(ip);
                table.addCell(name);
            } else {
                table.addCell(null);
                table.addCell(null);
            }

            table.addCell(shardStats == null ? null : shardStats.getCompletion().getSize());

            table.addCell(shardStats == null ? null : shardStats.getFieldData().getMemorySize());
            table.addCell(shardStats == null ? null : shardStats.getFieldData().getEvictions());

            table.addCell(shardStats == null ? null : shardStats.getFilterCache().getMemorySize());
            table.addCell(shardStats == null ? null : shardStats.getFilterCache().getEvictions());

            table.addCell(shardStats == null ? null : shardStats.getFlush().getTotal());
            table.addCell(shardStats == null ? null : shardStats.getFlush().getTotalTime());

            table.addCell(shardStats == null ? null : shardStats.getGet().current());
            table.addCell(shardStats == null ? null : shardStats.getGet().getTime());
            table.addCell(shardStats == null ? null : shardStats.getGet().getCount());
            table.addCell(shardStats == null ? null : shardStats.getGet().getExistsTime());
            table.addCell(shardStats == null ? null : shardStats.getGet().getExistsCount());
            table.addCell(shardStats == null ? null : shardStats.getGet().getMissingTime());
            table.addCell(shardStats == null ? null : shardStats.getGet().getMissingCount());

            table.addCell(shardStats == null ? null : shardStats.getIdCache().getMemorySize());

            table.addCell(shardStats == null ? null : shardStats.getIndexing().getTotal().getDeleteCurrent());
            table.addCell(shardStats == null ? null : shardStats.getIndexing().getTotal().getDeleteTime());
            table.addCell(shardStats == null ? null : shardStats.getIndexing().getTotal().getDeleteCount());
            table.addCell(shardStats == null ? null : shardStats.getIndexing().getTotal().getIndexCurrent());
            table.addCell(shardStats == null ? null : shardStats.getIndexing().getTotal().getIndexTime());
            table.addCell(shardStats == null ? null : shardStats.getIndexing().getTotal().getIndexCount());

            table.addCell(shardStats == null ? null : shardStats.getMerge().getCurrent());
            table.addCell(shardStats == null ? null : shardStats.getMerge().getCurrentNumDocs());
            table.addCell(shardStats == null ? null : shardStats.getMerge().getCurrentSize());
            table.addCell(shardStats == null ? null : shardStats.getMerge().getTotal());
            table.addCell(shardStats == null ? null : shardStats.getMerge().getTotalNumDocs());
            table.addCell(shardStats == null ? null : shardStats.getMerge().getTotalSize());
            table.addCell(shardStats == null ? null : shardStats.getMerge().getTotalTime());

            table.addCell(shardStats == null ? null : shardStats.getPercolate().getCurrent());
            table.addCell(shardStats == null ? null : shardStats.getPercolate().getMemorySize());
            table.addCell(shardStats == null ? null : shardStats.getPercolate().getNumQueries());
            table.addCell(shardStats == null ? null : shardStats.getPercolate().getTime());
            table.addCell(shardStats == null ? null : shardStats.getPercolate().getCount());

            table.addCell(shardStats == null ? null : shardStats.getRefresh().getTotal());
            table.addCell(shardStats == null ? null : shardStats.getRefresh().getTotalTime());

            table.addCell(shardStats == null ? null : shardStats.getSearch().getTotal().getFetchCurrent());
            table.addCell(shardStats == null ? null : shardStats.getSearch().getTotal().getFetchTime());
            table.addCell(shardStats == null ? null : shardStats.getSearch().getTotal().getFetchCount());
            table.addCell(shardStats == null ? null : shardStats.getSearch().getOpenContexts());
            table.addCell(shardStats == null ? null : shardStats.getSearch().getTotal().getQueryCurrent());
            table.addCell(shardStats == null ? null : shardStats.getSearch().getTotal().getQueryTime());
            table.addCell(shardStats == null ? null : shardStats.getSearch().getTotal().getQueryCount());

            table.addCell(shardStats == null ? null : shardStats.getSegments().getCount());
            table.addCell(shardStats == null ? null : shardStats.getSegments().getMemory());

            table.addCell(shardStats == null ? null : shardStats.getWarmer().current());
            table.addCell(shardStats == null ? null : shardStats.getWarmer().total());
            table.addCell(shardStats == null ? null : shardStats.getWarmer().totalTime());

            table.endRow();
        }

        return table;
    }
}
