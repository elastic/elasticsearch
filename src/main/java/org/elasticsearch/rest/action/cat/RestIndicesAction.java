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
        clusterStateRequest.filteredIndices(indices);
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

        table.addCell("primaries.completion.size", "alias:pcs,primariesCompletionSize;default:false;text-align:right;desc:size of completion");

        table.addCell("primaries.fielddata.memory_size", "alias:pfm,primariesFielddataMemory;default:false;text-align:right;desc:used fielddata cache");
        table.addCell("primaries.fielddata.evictions", "alias:pfe,primariesFielddataEvictions;default:false;text-align:right;desc:fielddata evictions");

        table.addCell("primaries.filter_cache.memory_size", "alias:pfcm,primariesFilterCacheMemory;default:false;text-align:right;desc:used filter cache");
        table.addCell("primaries.filter_cache.evictions", "alias:pfce,primariesFilterCacheEvictions;default:false;text-align:right;desc:filter cache evictions");

        table.addCell("primaries.flush.total", "alias:pft,primariesFlushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("primaries.flush.total_time", "alias:pftt,primariesFlushTotalTime;default:false;text-align:right;desc:time spent in flush");

        table.addCell("primaries.get.current", "alias:pgc,primariesGetCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("primaries.get.time", "alias:pgti,primariesGetTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("primaries.get.total", "alias:pgto,primariesGetTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("primaries.get.exists_time", "alias:pgeti,primariesGetExistsTime;default:false;text-align:right;desc:time spent in successful gets");
        table.addCell("primaries.get.exists_total", "alias:pgeto,primariesGetExistsTotal;default:false;text-align:right;desc:number of successful gets");
        table.addCell("primaries.get.missing_time", "alias:pgmti,primariesGetMissingTime;default:false;text-align:right;desc:time spent in failed gets");
        table.addCell("primaries.get.missing_total", "alias:pgmto,primariesGetMissingTotal;default:false;text-align:right;desc:number of failed gets");

        table.addCell("primaries.id_cache.memory_size", "alias:pim,primariesIdCacheMemory;default:false;text-align:right;desc:used id cache");

        table.addCell("primaries.indexing.delete_current", "alias:pidc,primariesIndexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions");
        table.addCell("primaries.indexing.delete_time", "alias:pidti,primariesIndexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("primaries.indexing.delete_total", "alias:pidto,primariesIndexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell("primaries.indexing.index_current", "alias:piic,primariesIndexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops");
        table.addCell("primaries.indexing.index_time", "alias:piiti,primariesIndexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("primaries.indexing.index_total", "alias:piito,primariesIndexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");

        table.addCell("primaries.merges.current", "alias:pmc,primariesMergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell("primaries.merges.current_docs", "alias:pmcd,primariesMergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs");
        table.addCell("primaries.merges.current_size", "alias:pmcs,primariesMergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("primaries.merges.total", "alias:pmt,primariesMergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("primaries.merges.total_docs", "alias:pmtd,primariesMergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("primaries.merges.total_size", "alias:pmts,primariesMergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("primaries.merges.total_time", "alias:pmtt,primariesMergesTotalTime;default:false;text-align:right;desc:time spent in merges");

        table.addCell("primaries.percolate.current", "alias:ppc,primariesPercolateCurrent;default:false;text-align:right;desc:number of current percolations");
        table.addCell("primaries.percolate.memory_size", "alias:ppm,primariesPercolateMemory;default:false;text-align:right;desc:memory used by percolations");
        table.addCell("primaries.percolate.queries", "alias:ppq,primariesPercolateQueries;default:false;text-align:right;desc:number of registered percolation queries");
        table.addCell("primaries.percolate.time", "alias:ppti,primariesPercolateTime;default:false;text-align:right;desc:time spent percolating");
        table.addCell("primaries.percolate.total", "alias:ppto,primariesPercolateTotal;default:false;text-align:right;desc:total percolations");

        table.addCell("primaries.refresh.total", "alias:prto,primariesRefreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("primaries.refresh.time", "alias:prti,primariesRefreshTime;default:false;text-align:right;desc:time spent in refreshes");

        table.addCell("primaries.search.fetch_current", "alias:psfc,primariesSearchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("primaries.search.fetch_time", "alias:psfti,primariesSearchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("primaries.search.fetch_total", "alias:psfto,primariesSearchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("primaries.search.open_contexts", "alias:pso,primariesSearchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("primaries.search.query_current", "alias:psqc,primariesSearchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("primaries.search.query_time", "alias:psqti,primariesSearchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("primaries.search.query_total", "alias:psqto,primariesSearchQueryTotal;default:false;text-align:right;desc:total query phase ops");

        table.addCell("primaries.segments.count", "alias:psc,primariesSegmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("primaries.segments.memory", "alias:psm,primariesSegmentsMemory;default:false;text-align:right;desc:memory used by segments");

        table.addCell("primaries.warmer.current", "alias:pwc,primariesWarmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("primaries.warmer.total", "alias:pwto,primariesWarmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("primaries.warmer.total_time", "alias:pwtt,primariesWarmerTotalTime;default:false;text-align:right;desc:time spent in warmers");

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
