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

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestActionListener;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestIndicesAction extends AbstractCatAction {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public RestIndicesAction(Settings settings, RestController controller, Client client, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, controller, client);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        controller.registerHandler(GET, "/_cat/indices", this);
        controller.registerHandler(GET, "/_cat/indices/{index}", this);
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/indices\n");
        sb.append("/_cat/indices/{index}\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().indices(indices).metaData(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        final IndicesOptions strictExpandIndicesOptions = IndicesOptions.strictExpand();
        clusterStateRequest.indicesOptions(strictExpandIndicesOptions);

        client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                final ClusterState state = clusterStateResponse.getState();
                final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, strictExpandIndicesOptions, indices);
                // concreteIndices should contain exactly the indices in state.metaData() that were selected by clusterStateRequest using
                // IndicesOptions.strictExpand(). We select the indices again here so that they can be displayed in the resulting table
                // in the requesting order.
                assert concreteIndices.length == state.metaData().getIndices().size();

                // Indices that were successfully resolved during the cluster state request might be deleted when the subsequent cluster
                // health and indices stats requests execute. We have to distinguish two cases:
                // 1) the deleted index was explicitly passed as parameter to the /_cat/indices request. In this case we want the subsequent
                //    requests to fail.
                // 2) the deleted index was resolved as part of a wildcard or _all. In this case, we want the subsequent requests not to
                //    fail on the deleted index (as we want to ignore wildcards that cannot be resolved).
                // This behavior can be ensured by letting the cluster health and indices stats requests re-resolve the index names with the
                // same indices options that we used for the initial cluster state request (strictExpand). Unfortunately cluster health
                // requests hard-code their indices options and the best we can do is apply strictExpand to the indices stats request.
                ClusterHealthRequest clusterHealthRequest = Requests.clusterHealthRequest(indices);
                clusterHealthRequest.local(request.paramAsBoolean("local", clusterHealthRequest.local()));
                client.admin().cluster().health(clusterHealthRequest, new RestActionListener<ClusterHealthResponse>(channel) {
                    @Override
                    public void processResponse(final ClusterHealthResponse clusterHealthResponse) {
                        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                        indicesStatsRequest.indices(indices);
                        indicesStatsRequest.indicesOptions(strictExpandIndicesOptions);
                        indicesStatsRequest.all();
                        client.admin().indices().stats(indicesStatsRequest, new RestResponseListener<IndicesStatsResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(IndicesStatsResponse indicesStatsResponse) throws Exception {
                                Table tab = buildTable(request, concreteIndices, clusterHealthResponse, indicesStatsResponse, state.metaData());
                                return RestTable.buildResponse(tab, channel);
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
        table.addCell("health", "alias:h;desc:current health status");
        table.addCell("status", "alias:s;desc:open/close status");
        table.addCell("index", "alias:i,idx;desc:index name");
        table.addCell("pri", "alias:p,shards.primary,shardsPrimary;text-align:right;desc:number of primary shards");
        table.addCell("rep", "alias:r,shards.replica,shardsReplica;text-align:right;desc:number of replica shards");
        table.addCell("docs.count", "alias:dc,docsCount;text-align:right;desc:available docs");
        table.addCell("docs.deleted", "alias:dd,docsDeleted;text-align:right;desc:deleted docs");

        table.addCell("creation.date", "alias:cd;default:false;desc:index creation date (millisecond value)");
        table.addCell("creation.date.string", "alias:cds;default:false;desc:index creation date (as string)");

        table.addCell("store.size", "sibling:pri;alias:ss,storeSize;text-align:right;desc:store size of primaries & replicas");
        table.addCell("pri.store.size", "text-align:right;desc:store size of primaries");

        table.addCell("completion.size", "sibling:pri;alias:cs,completionSize;default:false;text-align:right;desc:size of completion");
        table.addCell("pri.completion.size", "default:false;text-align:right;desc:size of completion");

        table.addCell("fielddata.memory_size", "sibling:pri;alias:fm,fielddataMemory;default:false;text-align:right;desc:used fielddata cache");
        table.addCell("pri.fielddata.memory_size", "default:false;text-align:right;desc:used fielddata cache");

        table.addCell("fielddata.evictions", "sibling:pri;alias:fe,fielddataEvictions;default:false;text-align:right;desc:fielddata evictions");
        table.addCell("pri.fielddata.evictions", "default:false;text-align:right;desc:fielddata evictions");

        table.addCell("query_cache.memory_size", "sibling:pri;alias:qcm,queryCacheMemory;default:false;text-align:right;desc:used query cache");
        table.addCell("pri.query_cache.memory_size", "default:false;text-align:right;desc:used query cache");

        table.addCell("query_cache.evictions", "sibling:pri;alias:qce,queryCacheEvictions;default:false;text-align:right;desc:query cache evictions");
        table.addCell("pri.query_cache.evictions", "default:false;text-align:right;desc:query cache evictions");

        table.addCell("request_cache.memory_size", "sibling:pri;alias:rcm,requestCacheMemory;default:false;text-align:right;desc:used request cache");
        table.addCell("pri.request_cache.memory_size", "default:false;text-align:right;desc:used request cache");

        table.addCell("request_cache.evictions", "sibling:pri;alias:rce,requestCacheEvictions;default:false;text-align:right;desc:request cache evictions");
        table.addCell("pri.request_cache.evictions", "default:false;text-align:right;desc:request cache evictions");

        table.addCell("request_cache.hit_count", "sibling:pri;alias:rchc,requestCacheHitCount;default:false;text-align:right;desc:request cache hit count");
        table.addCell("pri.request_cache.hit_count", "default:false;text-align:right;desc:request cache hit count");

        table.addCell("request_cache.miss_count", "sibling:pri;alias:rcmc,requestCacheMissCount;default:false;text-align:right;desc:request cache miss count");
        table.addCell("pri.request_cache.miss_count", "default:false;text-align:right;desc:request cache miss count");

        table.addCell("flush.total", "sibling:pri;alias:ft,flushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("pri.flush.total", "default:false;text-align:right;desc:number of flushes");

        table.addCell("flush.total_time", "sibling:pri;alias:ftt,flushTotalTime;default:false;text-align:right;desc:time spent in flush");
        table.addCell("pri.flush.total_time", "default:false;text-align:right;desc:time spent in flush");

        table.addCell("get.current", "sibling:pri;alias:gc,getCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("pri.get.current", "default:false;text-align:right;desc:number of current get ops");

        table.addCell("get.time", "sibling:pri;alias:gti,getTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("pri.get.time", "default:false;text-align:right;desc:time spent in get");

        table.addCell("get.total", "sibling:pri;alias:gto,getTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("pri.get.total", "default:false;text-align:right;desc:number of get ops");

        table.addCell("get.exists_time", "sibling:pri;alias:geti,getExistsTime;default:false;text-align:right;desc:time spent in successful gets");
        table.addCell("pri.get.exists_time", "default:false;text-align:right;desc:time spent in successful gets");

        table.addCell("get.exists_total", "sibling:pri;alias:geto,getExistsTotal;default:false;text-align:right;desc:number of successful gets");
        table.addCell("pri.get.exists_total", "default:false;text-align:right;desc:number of successful gets");

        table.addCell("get.missing_time", "sibling:pri;alias:gmti,getMissingTime;default:false;text-align:right;desc:time spent in failed gets");
        table.addCell("pri.get.missing_time", "default:false;text-align:right;desc:time spent in failed gets");

        table.addCell("get.missing_total", "sibling:pri;alias:gmto,getMissingTotal;default:false;text-align:right;desc:number of failed gets");
        table.addCell("pri.get.missing_total", "default:false;text-align:right;desc:number of failed gets");

        table.addCell("indexing.delete_current", "sibling:pri;alias:idc,indexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions");
        table.addCell("pri.indexing.delete_current", "default:false;text-align:right;desc:number of current deletions");

        table.addCell("indexing.delete_time", "sibling:pri;alias:idti,indexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("pri.indexing.delete_time", "default:false;text-align:right;desc:time spent in deletions");

        table.addCell("indexing.delete_total", "sibling:pri;alias:idto,indexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell("pri.indexing.delete_total", "default:false;text-align:right;desc:number of delete ops");

        table.addCell("indexing.index_current", "sibling:pri;alias:iic,indexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops");
        table.addCell("pri.indexing.index_current", "default:false;text-align:right;desc:number of current indexing ops");

        table.addCell("indexing.index_time", "sibling:pri;alias:iiti,indexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("pri.indexing.index_time", "default:false;text-align:right;desc:time spent in indexing");

        table.addCell("indexing.index_total", "sibling:pri;alias:iito,indexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");
        table.addCell("pri.indexing.index_total", "default:false;text-align:right;desc:number of indexing ops");

        table.addCell("indexing.index_failed", "sibling:pri;alias:iif,indexingIndexFailed;default:false;text-align:right;desc:number of failed indexing ops");
        table.addCell("pri.indexing.index_failed", "default:false;text-align:right;desc:number of failed indexing ops");

        table.addCell("merges.current", "sibling:pri;alias:mc,mergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell("pri.merges.current", "default:false;text-align:right;desc:number of current merges");

        table.addCell("merges.current_docs", "sibling:pri;alias:mcd,mergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs");
        table.addCell("pri.merges.current_docs", "default:false;text-align:right;desc:number of current merging docs");

        table.addCell("merges.current_size", "sibling:pri;alias:mcs,mergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("pri.merges.current_size", "default:false;text-align:right;desc:size of current merges");

        table.addCell("merges.total", "sibling:pri;alias:mt,mergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("pri.merges.total", "default:false;text-align:right;desc:number of completed merge ops");

        table.addCell("merges.total_docs", "sibling:pri;alias:mtd,mergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("pri.merges.total_docs", "default:false;text-align:right;desc:docs merged");

        table.addCell("merges.total_size", "sibling:pri;alias:mts,mergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("pri.merges.total_size", "default:false;text-align:right;desc:size merged");

        table.addCell("merges.total_time", "sibling:pri;alias:mtt,mergesTotalTime;default:false;text-align:right;desc:time spent in merges");
        table.addCell("pri.merges.total_time", "default:false;text-align:right;desc:time spent in merges");

        table.addCell("refresh.total", "sibling:pri;alias:rto,refreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("pri.refresh.total", "default:false;text-align:right;desc:total refreshes");

        table.addCell("refresh.time", "sibling:pri;alias:rti,refreshTime;default:false;text-align:right;desc:time spent in refreshes");
        table.addCell("pri.refresh.time", "default:false;text-align:right;desc:time spent in refreshes");

        table.addCell("search.fetch_current", "sibling:pri;alias:sfc,searchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("pri.search.fetch_current", "default:false;text-align:right;desc:current fetch phase ops");

        table.addCell("search.fetch_time", "sibling:pri;alias:sfti,searchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("pri.search.fetch_time", "default:false;text-align:right;desc:time spent in fetch phase");

        table.addCell("search.fetch_total", "sibling:pri;alias:sfto,searchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("pri.search.fetch_total", "default:false;text-align:right;desc:total fetch ops");

        table.addCell("search.open_contexts", "sibling:pri;alias:so,searchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("pri.search.open_contexts", "default:false;text-align:right;desc:open search contexts");

        table.addCell("search.query_current", "sibling:pri;alias:sqc,searchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("pri.search.query_current", "default:false;text-align:right;desc:current query phase ops");

        table.addCell("search.query_time", "sibling:pri;alias:sqti,searchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("pri.search.query_time", "default:false;text-align:right;desc:time spent in query phase");

        table.addCell("search.query_total", "sibling:pri;alias:sqto,searchQueryTotal;default:false;text-align:right;desc:total query phase ops");
        table.addCell("pri.search.query_total", "default:false;text-align:right;desc:total query phase ops");

        table.addCell("search.scroll_current", "sibling:pri;alias:scc,searchScrollCurrent;default:false;text-align:right;desc:open scroll contexts");
        table.addCell("pri.search.scroll_current", "default:false;text-align:right;desc:open scroll contexts");

        table.addCell("search.scroll_time", "sibling:pri;alias:scti,searchScrollTime;default:false;text-align:right;desc:time scroll contexts held open");
        table.addCell("pri.search.scroll_time", "default:false;text-align:right;desc:time scroll contexts held open");

        table.addCell("search.scroll_total", "sibling:pri;alias:scto,searchScrollTotal;default:false;text-align:right;desc:completed scroll contexts");
        table.addCell("pri.search.scroll_total", "default:false;text-align:right;desc:completed scroll contexts");

        table.addCell("segments.count", "sibling:pri;alias:sc,segmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("pri.segments.count", "default:false;text-align:right;desc:number of segments");

        table.addCell("segments.memory", "sibling:pri;alias:sm,segmentsMemory;default:false;text-align:right;desc:memory used by segments");
        table.addCell("pri.segments.memory", "default:false;text-align:right;desc:memory used by segments");

        table.addCell("segments.index_writer_memory", "sibling:pri;alias:siwm,segmentsIndexWriterMemory;default:false;text-align:right;desc:memory used by index writer");
        table.addCell("pri.segments.index_writer_memory", "default:false;text-align:right;desc:memory used by index writer");

        table.addCell("segments.version_map_memory", "sibling:pri;alias:svmm,segmentsVersionMapMemory;default:false;text-align:right;desc:memory used by version map");
        table.addCell("pri.segments.version_map_memory", "default:false;text-align:right;desc:memory used by version map");

        table.addCell("segments.fixed_bitset_memory", "sibling:pri;alias:sfbm,fixedBitsetMemory;default:false;text-align:right;desc:memory used by fixed bit sets for nested object field types and type filters for types referred in _parent fields");
        table.addCell("pri.segments.fixed_bitset_memory", "default:false;text-align:right;desc:memory used by fixed bit sets for nested object field types and type filters for types referred in _parent fields");

        table.addCell("warmer.current", "sibling:pri;alias:wc,warmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("pri.warmer.current", "default:false;text-align:right;desc:current warmer ops");

        table.addCell("warmer.total", "sibling:pri;alias:wto,warmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("pri.warmer.total", "default:false;text-align:right;desc:total warmer ops");

        table.addCell("warmer.total_time", "sibling:pri;alias:wtt,warmerTotalTime;default:false;text-align:right;desc:time spent in warmers");
        table.addCell("pri.warmer.total_time", "default:false;text-align:right;desc:time spent in warmers");

        table.addCell("suggest.current", "sibling:pri;alias:suc,suggestCurrent;default:false;text-align:right;desc:number of current suggest ops");
        table.addCell("pri.suggest.current", "default:false;text-align:right;desc:number of current suggest ops");

        table.addCell("suggest.time", "sibling:pri;alias:suti,suggestTime;default:false;text-align:right;desc:time spend in suggest");
        table.addCell("pri.suggest.time", "default:false;text-align:right;desc:time spend in suggest");

        table.addCell("suggest.total", "sibling:pri;alias:suto,suggestTotal;default:false;text-align:right;desc:number of suggest ops");
        table.addCell("pri.suggest.total", "default:false;text-align:right;desc:number of suggest ops");

        table.addCell("memory.total", "sibling:pri;alias:tm,memoryTotal;default:false;text-align:right;desc:total used memory");
        table.addCell("pri.memory.total", "default:false;text-align:right;desc:total user memory");

        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, String[] indices, ClusterHealthResponse health, IndicesStatsResponse stats, MetaData indexMetaDatas) {
        Table table = getTableWithHeader(request);

        for (String index : indices) {
            ClusterIndexHealth indexHealth = health.getIndices().get(index);
            IndexStats indexStats = stats.getIndices().get(index);
            IndexMetaData indexMetaData = indexMetaDatas.getIndices().get(index);
            IndexMetaData.State state = indexMetaData.getState();

            table.startRow();
            table.addCell(state == IndexMetaData.State.OPEN ? (indexHealth == null ? "red*" : indexHealth.getStatus().toString().toLowerCase(Locale.ROOT)) : null);
            table.addCell(state.toString().toLowerCase(Locale.ROOT));
            table.addCell(index);
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfShards());
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfReplicas());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getDocs().getCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getDocs().getDeleted());

            table.addCell(indexMetaData.getCreationDate());
            table.addCell(new DateTime(indexMetaData.getCreationDate(), DateTimeZone.UTC));

            table.addCell(indexStats == null ? null : indexStats.getTotal().getStore().size());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getStore().size());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getCompletion().getSize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getCompletion().getSize());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getFieldData().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFieldData().getMemorySize());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getFieldData().getEvictions());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFieldData().getEvictions());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getQueryCache().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getQueryCache().getMemorySize());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getQueryCache().getEvictions());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getQueryCache().getEvictions());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getRequestCache().getMemorySize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getRequestCache().getMemorySize());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getRequestCache().getEvictions());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getRequestCache().getEvictions());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getRequestCache().getHitCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getRequestCache().getHitCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getRequestCache().getMissCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getRequestCache().getMissCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getFlush().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFlush().getTotal());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getFlush().getTotalTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getFlush().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().current());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().current());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getExistsTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getExistsTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getExistsCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getExistsCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getMissingTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getMissingTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getGet().getMissingCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getGet().getMissingCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getDeleteCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getDeleteCurrent());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getDeleteTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getDeleteTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getDeleteCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getDeleteCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getIndexCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getIndexCurrent());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getIndexTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getIndexTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getIndexCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getIndexCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getIndexing().getTotal().getIndexFailedCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getIndexing().getTotal().getIndexFailedCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getCurrent());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getCurrentNumDocs());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getCurrentNumDocs());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getCurrentSize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getCurrentSize());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getTotal());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getTotalNumDocs());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getTotalNumDocs());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getTotalSize());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getTotalSize());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getMerge().getTotalTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getMerge().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getRefresh().getTotal());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getRefresh().getTotal());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getRefresh().getTotalTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getRefresh().getTotalTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getFetchCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getFetchCurrent());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getFetchTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getFetchTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getFetchCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getFetchCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getOpenContexts());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getOpenContexts());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getQueryCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getQueryCurrent());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getQueryTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getQueryTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getQueryCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getQueryCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getScrollCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getScrollCurrent());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getScrollTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getScrollTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getScrollCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getScrollCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSegments().getCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSegments().getCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSegments().getMemory());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSegments().getMemory());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSegments().getIndexWriterMemory());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSegments().getIndexWriterMemory());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSegments().getVersionMapMemory());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSegments().getVersionMapMemory());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSegments().getBitsetMemory());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSegments().getBitsetMemory());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getWarmer().current());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getWarmer().current());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getWarmer().total());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getWarmer().total());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getWarmer().totalTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getWarmer().totalTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getSuggestCurrent());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getSuggestCurrent());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getSuggestTime());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getSuggestTime());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getSearch().getTotal().getSuggestCount());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getSearch().getTotal().getSuggestCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getTotalMemory());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getTotalMemory());

            table.endRow();
        }

        return table;
    }
}
