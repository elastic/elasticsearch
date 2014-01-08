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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestIndicesAction extends AbstractCatAction {

    @Inject
    public RestIndicesAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/indices", this);
        controller.registerHandler(GET, "/_cat/indices/{index}", this);
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/indices\n");
        sb.append("/_cat/indices/{index}\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().indices(indices).metaData(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                final String[] concreteIndices = clusterStateResponse.getState().metaData().concreteIndicesIgnoreMissing(indices);
                ClusterHealthRequest clusterHealthRequest = Requests.clusterHealthRequest(concreteIndices);
                clusterHealthRequest.local(request.paramAsBoolean("local", clusterHealthRequest.local()));
                client.admin().cluster().health(clusterHealthRequest, new ActionListener<ClusterHealthResponse>() {
                    @Override
                    public void onResponse(final ClusterHealthResponse clusterHealthResponse) {
                        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                        indicesStatsRequest.all();
                        client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                            @Override
                            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                                try {
                                    Table tab = buildTable(request, concreteIndices, clusterHealthResponse, indicesStatsResponse);
                                    channel.sendResponse(RestTable.buildResponse(tab, request, channel));
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
        table.startHeaders();
        table.addCell("health", "desc:current health status");
        table.addCell("index", "desc:index name");
        table.addCell("pri", "text-align:right;desc:number of primary shards");
        table.addCell("rep", "text-align:right;desc:number of replica shards");
        table.addCell("docs.count", "text-align:right;desc:available docs");
        table.addCell("docs.deleted", "text-align:right;desc:deleted docs");
        table.addCell("primaries.store.size", "text-align:right;desc:store size of primaries");
        table.addCell("total.store.size", "text-align:right;desc:store size of primaries & replicas");

        table.addCell("pri.completion.size", "alias:pcs,priCompletionSize;default:false;text-align:right;desc:size of completion");

        table.addCell("pri.fielddata.memory_size", "alias:pfm,priFielddataMemory;default:false;text-align:right;desc:used fielddata cache");
        table.addCell("pri.fielddata.evictions", "alias:pfe,priFielddataEvictions;default:false;text-align:right;desc:fielddata evictions");

        table.addCell("pri.filter_cache.memory_size", "alias:pfcm,priFilterCacheMemory;default:false;text-align:right;desc:used filter cache");
        table.addCell("pri.filter_cache.evictions", "alias:pfce,priFilterCacheEvictions;default:false;text-align:right;desc:filter cache evictions");

        table.addCell("pri.flush.total", "alias:pft,priFlushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("pri.flush.total_time", "alias:pftt,priFlushTotalTime;default:false;text-align:right;desc:time spent in flush");

        table.addCell("pri.get.current", "alias:pgc,priGetCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("pri.get.time", "alias:pgti,priGetTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("pri.get.total", "alias:pgto,priGetTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("pri.get.exists_time", "alias:pgeti,priGetExistsTime;default:false;text-align:right;desc:time spent in successful gets");
        table.addCell("pri.get.exists_total", "alias:pgeto,priGetExistsTotal;default:false;text-align:right;desc:number of successful gets");
        table.addCell("pri.get.missing_time", "alias:pgmti,priGetMissingTime;default:false;text-align:right;desc:time spent in failed gets");
        table.addCell("pri.get.missing_total", "alias:pgmto,priGetMissingTotal;default:false;text-align:right;desc:number of failed gets");

        table.addCell("pri.id_cache.memory_size", "alias:pim,priIdCacheMemory;default:false;text-align:right;desc:used id cache");

        table.addCell("pri.indexing.delete_current", "alias:pidc,priIndexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions");
        table.addCell("pri.indexing.delete_time", "alias:pidti,priIndexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("pri.indexing.delete_total", "alias:pidto,priIndexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell("pri.indexing.index_current", "alias:piic,priIndexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops");
        table.addCell("pri.indexing.index_time", "alias:piiti,priIndexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("pri.indexing.index_total", "alias:piito,priIndexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");

        table.addCell("pri.merges.current", "alias:pmc,priMergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell("pri.merges.current_docs", "alias:pmcd,priMergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs");
        table.addCell("pri.merges.current_size", "alias:pmcs,priMergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("pri.merges.total", "alias:pmt,priMergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("pri.merges.total_docs", "alias:pmtd,priMergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("pri.merges.total_size", "alias:pmts,priMergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("pri.merges.total_time", "alias:pmtt,priMergesTotalTime;default:false;text-align:right;desc:time spent in merges");

        table.addCell("pri.percolate.current", "alias:ppc,priPercolateCurrent;default:false;text-align:right;desc:number of current percolations");
        table.addCell("pri.percolate.memory_size", "alias:ppm,priPercolateMemory;default:false;text-align:right;desc:memory used by percolations");
        table.addCell("pri.percolate.queries", "alias:ppq,priPercolateQueries;default:false;text-align:right;desc:number of registered percolation queries");
        table.addCell("pri.percolate.time", "alias:ppti,priPercolateTime;default:false;text-align:right;desc:time spent percolating");
        table.addCell("pri.percolate.total", "alias:ppto,priPercolateTotal;default:false;text-align:right;desc:total percolations");

        table.addCell("pri.refresh.total", "alias:prto,priRefreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("pri.refresh.time", "alias:prti,priRefreshTime;default:false;text-align:right;desc:time spent in refreshes");

        table.addCell("pri.search.fetch_current", "alias:psfc,priSearchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("pri.search.fetch_time", "alias:psfti,priSearchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("pri.search.fetch_total", "alias:psfto,priSearchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("pri.search.open_contexts", "alias:pso,priSearchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("pri.search.query_current", "alias:psqc,priSearchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("pri.search.query_time", "alias:psqti,priSearchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("pri.search.query_total", "alias:psqto,priSearchQueryTotal;default:false;text-align:right;desc:total query phase ops");

        table.addCell("pri.segments.count", "alias:psc,priSegmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("pri.segments.memory", "alias:psm,priSegmentsMemory;default:false;text-align:right;desc:memory used by segments");

        table.addCell("pri.warmer.current", "alias:pwc,priWarmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("pri.warmer.total", "alias:pwto,priWarmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("pri.warmer.total_time", "alias:pwtt,priWarmerTotalTime;default:false;text-align:right;desc:time spent in warmers");

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

        table.addCell("warmer.current", "alias:wc,warmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("warmer.total", "alias:wto,warmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("warmer.total_time", "alias:wtt,warmerTotalTime;default:false;text-align:right;desc:time spent in warmers");

        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, String[] indices, ClusterHealthResponse health, IndicesStatsResponse stats) {
        Table table = getTableWithHeader(request);

        for (String index : indices) {
            ClusterIndexHealth indexHealth = health.getIndices().get(index);
            IndexStats indexStats = stats.getIndices().get(index);

            table.startRow();
            table.addCell(indexHealth == null ? "red*" : indexHealth.getStatus().toString().toLowerCase(Locale.getDefault()));
            table.addCell(index);
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfShards());
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfReplicas());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getDocs().getCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getDocs().getDeleted());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getStore().size());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getStore().size());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getCompletion().getSize());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFieldData().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFieldData().getEvictions());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFilterCache().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFilterCache().getEvictions());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFlush().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFlush().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().current());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getExistsTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getExistsCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getMissingTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getMissingCount());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIdCache().getMemorySize());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getDeleteCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getDeleteTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getDeleteCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getIndexCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getIndexTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getIndexCount());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getCurrentNumDocs());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getCurrentSize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getTotalNumDocs());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getTotalSize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getPercolate().getCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getPercolate().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getPercolate().getNumQueries());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getPercolate().getTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getPercolate().getCount());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getRefresh().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getRefresh().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getFetchCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getFetchTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getFetchCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getOpenContexts());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getQueryCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getQueryTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getQueryCount());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSegments().getCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSegments().getMemory());

            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getWarmer().current());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getWarmer().total());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getWarmer().totalTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getCompletion().getSize());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getFieldData().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getFieldData().getEvictions());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getFilterCache().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getFilterCache().getEvictions());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getFlush().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getFlush().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().current());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getTime());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getCount());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getExistsTime());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getExistsCount());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getMissingTime());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getMissingCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIdCache().getMemorySize());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getDeleteCurrent());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getDeleteTime());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getDeleteCount());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getIndexCurrent());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getIndexTime());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getIndexCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getCurrent());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getCurrentNumDocs());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getCurrentSize());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getTotalNumDocs());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getTotalSize());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getPercolate().getCurrent());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getPercolate().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getPercolate().getNumQueries());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getPercolate().getTime());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getPercolate().getCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getRefresh().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getRefresh().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getFetchCurrent());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getFetchTime());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getFetchCount());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getOpenContexts());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getQueryCurrent());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getQueryTime());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getQueryCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSegments().getCount());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getSegments().getMemory());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getWarmer().current());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getWarmer().total());
            table.addCell(indexStats == null ? null : indexStats.getTotal().getWarmer().totalTime());

            table.endRow();
        }

        return table;
    }
}
