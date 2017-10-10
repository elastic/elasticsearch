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
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestIndicesAction extends AbstractCatAction {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public RestIndicesAction(Settings settings, RestController controller, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        controller.registerHandler(GET, "/_cat/indices", this);
        controller.registerHandler(GET, "/_cat/indices/{index}", this);
    }

    @Override
    public String getName() {
        return "cat_indices_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/indices\n");
        sb.append("/_cat/indices/{index}\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().indices(indices).metaData(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        final IndicesOptions strictExpandIndicesOptions = IndicesOptions.strictExpand();
        clusterStateRequest.indicesOptions(strictExpandIndicesOptions);

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                final ClusterState state = clusterStateResponse.getState();
                final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, strictExpandIndicesOptions, indices);
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

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(Arrays.asList("local", "health"));
        responseParams.addAll(AbstractCatAction.RESPONSE_PARAMS);
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("health", "alias:h;desc:current health status");
        table.addCell("status", "alias:s;desc:open/close status");
        table.addCell("index", "alias:i,idx;desc:index name");
        table.addCell("uuid", "alias:id,uuid;desc:index uuid");
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

        table.addCell("refresh.listeners", "sibling:pri;alias:rli,refreshListeners;default:false;text-align:right;desc:number of pending refresh listeners");
        table.addCell("pri.refresh.listeners", "default:false;text-align:right;desc:number of pending refresh listeners");

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

    // package private for testing
    Table buildTable(RestRequest request, Index[] indices, ClusterHealthResponse response, IndicesStatsResponse stats, MetaData indexMetaDatas) {
        final String healthParam = request.param("health");
        final ClusterHealthStatus status;
        if (healthParam != null) {
            status = ClusterHealthStatus.fromString(healthParam);
        } else {
            status = null;
        }

        Table table = getTableWithHeader(request);

        for (final Index index : indices) {
            final String indexName = index.getName();
            ClusterIndexHealth indexHealth = response.getIndices().get(indexName);
            IndexStats indexStats = stats.getIndices().get(indexName);
            IndexMetaData indexMetaData = indexMetaDatas.getIndices().get(indexName);
            IndexMetaData.State state = indexMetaData.getState();

            if (status != null) {
                if (state == IndexMetaData.State.CLOSE ||
                        (indexHealth == null && !ClusterHealthStatus.RED.equals(status)) ||
                        !indexHealth.getStatus().equals(status)) {
                    continue;
                }
            }

            final CommonStats primaryStats = indexStats == null ? new CommonStats() : indexStats.getPrimaries();
            final CommonStats totalStats = indexStats == null ? new CommonStats() : indexStats.getTotal();

            table.startRow();
            table.addCell(state == IndexMetaData.State.OPEN ? (indexHealth == null ? "red*" : indexHealth.getStatus().toString().toLowerCase(Locale.ROOT)) : null);
            table.addCell(state.toString().toLowerCase(Locale.ROOT));
            table.addCell(indexName);
            table.addCell(index.getUUID());
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfShards());
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfReplicas());

            table.addCell(primaryStats.getDocs() == null ? null : primaryStats.getDocs().getCount());
            table.addCell(primaryStats.getDocs() == null ? null : primaryStats.getDocs().getDeleted());

            table.addCell(indexMetaData.getCreationDate());
            table.addCell(new DateTime(indexMetaData.getCreationDate(), DateTimeZone.UTC));

            table.addCell(totalStats.getStore() == null ? null : totalStats.getStore().size());
            table.addCell(primaryStats.getStore() == null ? null : primaryStats.getStore().size());

            table.addCell(totalStats.getCompletion() == null ? null : totalStats.getCompletion().getSize());
            table.addCell(primaryStats.getCompletion() == null ? null : primaryStats.getCompletion().getSize());

            table.addCell(totalStats.getFieldData() == null ? null : totalStats.getFieldData().getMemorySize());
            table.addCell(primaryStats.getFieldData() == null ? null : primaryStats.getFieldData().getMemorySize());

            table.addCell(totalStats.getFieldData() == null ? null : totalStats.getFieldData().getEvictions());
            table.addCell(primaryStats.getFieldData() == null ? null : primaryStats.getFieldData().getEvictions());

            table.addCell(totalStats.getQueryCache() == null ? null : totalStats.getQueryCache().getMemorySize());
            table.addCell(primaryStats.getQueryCache() == null ? null : primaryStats.getQueryCache().getMemorySize());

            table.addCell(totalStats.getQueryCache() == null ? null : totalStats.getQueryCache().getEvictions());
            table.addCell(primaryStats.getQueryCache() == null ? null : primaryStats.getQueryCache().getEvictions());

            table.addCell(totalStats.getRequestCache() == null ? null : totalStats.getRequestCache().getMemorySize());
            table.addCell(primaryStats.getRequestCache() == null ? null : primaryStats.getRequestCache().getMemorySize());

            table.addCell(totalStats.getRequestCache() == null ? null : totalStats.getRequestCache().getEvictions());
            table.addCell(primaryStats.getRequestCache() == null ? null : primaryStats.getRequestCache().getEvictions());

            table.addCell(totalStats.getRequestCache() == null ? null : totalStats.getRequestCache().getHitCount());
            table.addCell(primaryStats.getRequestCache() == null ? null : primaryStats.getRequestCache().getHitCount());

            table.addCell(totalStats.getRequestCache() == null ? null : totalStats.getRequestCache().getMissCount());
            table.addCell(primaryStats.getRequestCache() == null ? null : primaryStats.getRequestCache().getMissCount());

            table.addCell(totalStats.getFlush() == null ? null : totalStats.getFlush().getTotal());
            table.addCell(primaryStats.getFlush() == null ? null : primaryStats.getFlush().getTotal());

            table.addCell(totalStats.getFlush() == null ? null : totalStats.getFlush().getTotalTime());
            table.addCell(primaryStats.getFlush() == null ? null : primaryStats.getFlush().getTotalTime());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().current());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().current());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getTime());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getTime());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getCount());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getCount());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getExistsTime());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getExistsTime());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getExistsCount());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getExistsCount());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getMissingTime());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getMissingTime());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getMissingCount());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getMissingCount());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getDeleteCurrent());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getDeleteCurrent());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getDeleteTime());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getDeleteTime());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getDeleteCount());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getDeleteCount());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getIndexCurrent());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getIndexCurrent());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getIndexTime());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getIndexTime());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getIndexCount());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getIndexCount());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getIndexFailedCount());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getIndexFailedCount());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getCurrent());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getCurrent());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getCurrentNumDocs());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getCurrentNumDocs());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getCurrentSize());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getCurrentSize());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getTotal());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getTotal());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getTotalNumDocs());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getTotalNumDocs());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getTotalSize());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getTotalSize());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getTotalTime());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getTotalTime());

            table.addCell(totalStats.getRefresh() == null ? null : totalStats.getRefresh().getTotal());
            table.addCell(primaryStats.getRefresh() == null ? null : primaryStats.getRefresh().getTotal());

            table.addCell(totalStats.getRefresh() == null ? null : totalStats.getRefresh().getTotalTime());
            table.addCell(primaryStats.getRefresh() == null ? null : primaryStats.getRefresh().getTotalTime());

            table.addCell(totalStats.getRefresh() == null ? null : totalStats.getRefresh().getListeners());
            table.addCell(primaryStats.getRefresh() == null ? null : primaryStats.getRefresh().getListeners());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getFetchCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getFetchCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getFetchTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getFetchTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getFetchCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getFetchCount());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getOpenContexts());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getOpenContexts());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getQueryCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getQueryCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getQueryTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getQueryTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getQueryCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getQueryCount());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getScrollCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getScrollCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getScrollTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getScrollTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getScrollCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getScrollCount());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getCount());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getCount());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getMemory());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getMemory());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getIndexWriterMemory());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getIndexWriterMemory());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getVersionMapMemory());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getVersionMapMemory());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getBitsetMemory());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getBitsetMemory());

            table.addCell(totalStats.getWarmer() == null ? null : totalStats.getWarmer().current());
            table.addCell(primaryStats.getWarmer() == null ? null : primaryStats.getWarmer().current());

            table.addCell(totalStats.getWarmer() == null ? null : totalStats.getWarmer().total());
            table.addCell(primaryStats.getWarmer() == null ? null : primaryStats.getWarmer().total());

            table.addCell(totalStats.getWarmer() == null ? null : totalStats.getWarmer().totalTime());
            table.addCell(primaryStats.getWarmer() == null ? null : primaryStats.getWarmer().totalTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getSuggestCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getSuggestCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getSuggestTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getSuggestTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getSuggestCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getSuggestCount());

            table.addCell(indexStats == null ? null : indexStats.getTotal().getTotalMemory());
            table.addCell(indexStats == null ? null : indexStats.getPrimaries().getTotalMemory());

            table.endRow();
        }

        return table;
    }
}
