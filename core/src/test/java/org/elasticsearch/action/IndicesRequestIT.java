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

package org.elasticsearch.action;

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.TransportShardFlushAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.SUITE, numClientNodes = 1, minNumDataNodes = 2)
public class IndicesRequestIT extends IndicesRequestTestCase {
    @Override
    protected int minimumNumberOfShards() {
        //makes sure that a reduce is always needed when searching
        return 2;
    }

    @Override
    protected int minimumNumberOfReplicas() {
        //makes sure that write operations get sent to the replica as well
        //so we are able to intercept those messages and check them
        return 1;
    }

    @Override
    protected Settings nodeSettings(int ordinal) {
        // must set this independently of the plugin so it overrides MockTransportService
        return Settings.builder().put(super.nodeSettings(ordinal))
            // InternalClusterInfoService sends IndicesStatsRequest periodically which messes with this test
            // this setting disables it...
            .put("cluster.routing.allocation.disk.threshold_enabled", false).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.addAll(super.nodePlugins());
        plugins.add(CustomScriptPlugin.class);
        return plugins;
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("ctx.op='delete'", vars -> ((Map<String, Object>) vars.get("ctx")).put("op", "delete"));
        }
    }

    public void testGetFieldMappings() {
        String getFieldMappingsShardAction = GetFieldMappingsAction.NAME + "[index][s]";
        interceptTransportActions(getFieldMappingsShardAction);

        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest();
        getFieldMappingsRequest.indices(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().getFieldMappings(getFieldMappingsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(getFieldMappingsRequest, getFieldMappingsShardAction);
    }

    public void testIndex() {
        String[] indexShardActions = new String[]{BulkAction.NAME + "[s][p]", BulkAction.NAME + "[s][r]"};
        interceptTransportActions(indexShardActions);

        IndexRequest indexRequest = new IndexRequest(randomIndexOrAlias(), "type", "id")
            .source(Requests.INDEX_CONTENT_TYPE, "field", "value");
        internalCluster().coordOnlyNodeClient().index(indexRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(indexRequest, indexShardActions);
    }

    public void testDelete() {
        String[] deleteShardActions = new String[]{BulkAction.NAME + "[s][p]", BulkAction.NAME + "[s][r]"};
        interceptTransportActions(deleteShardActions);

        DeleteRequest deleteRequest = new DeleteRequest(randomIndexOrAlias(), "type", "id");
        internalCluster().coordOnlyNodeClient().delete(deleteRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(deleteRequest, deleteShardActions);
    }

    public void testUpdate() {
        //update action goes to the primary, index op gets executed locally, then replicated
        String[] updateShardActions = new String[]{UpdateAction.NAME + "[s]", BulkAction.NAME + "[s][p]", BulkAction.NAME + "[s][r]"};
        interceptTransportActions(updateShardActions);

        String indexOrAlias = randomIndexOrAlias();
        client().prepareIndex(indexOrAlias, "type", "id").setSource("field", "value").get();
        UpdateRequest updateRequest = new UpdateRequest(indexOrAlias, "type", "id").doc(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
        UpdateResponse updateResponse = internalCluster().coordOnlyNodeClient().update(updateRequest).actionGet();
        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());

        clearInterceptedActions();
        assertSameIndices(updateRequest, updateShardActions);
    }

    public void testUpdateUpsert() {
        //update action goes to the primary, index op gets executed locally, then replicated
        String[] updateShardActions = new String[]{UpdateAction.NAME + "[s]", BulkAction.NAME + "[s][p]", BulkAction.NAME + "[s][r]"};
        interceptTransportActions(updateShardActions);

        String indexOrAlias = randomIndexOrAlias();
        UpdateRequest updateRequest = new UpdateRequest(indexOrAlias, "type", "id").upsert(Requests.INDEX_CONTENT_TYPE, "field", "value")
            .doc(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
        UpdateResponse updateResponse = internalCluster().coordOnlyNodeClient().update(updateRequest).actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, updateResponse.getResult());

        clearInterceptedActions();
        assertSameIndices(updateRequest, updateShardActions);
    }

    public void testUpdateDelete() {
        //update action goes to the primary, delete op gets executed locally, then replicated
        String[] updateShardActions = new String[]{UpdateAction.NAME + "[s]", BulkAction.NAME + "[s][p]", BulkAction.NAME + "[s][r]"};
        interceptTransportActions(updateShardActions);

        String indexOrAlias = randomIndexOrAlias();
        client().prepareIndex(indexOrAlias, "type", "id").setSource("field", "value").get();
        UpdateRequest updateRequest = new UpdateRequest(indexOrAlias, "type", "id")
                .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "ctx.op='delete'", Collections.emptyMap()));
        UpdateResponse updateResponse = internalCluster().coordOnlyNodeClient().update(updateRequest).actionGet();
        assertEquals(DocWriteResponse.Result.DELETED, updateResponse.getResult());

        clearInterceptedActions();
        assertSameIndices(updateRequest, updateShardActions);
    }

    public void testBulk() {
        String[] bulkShardActions = new String[]{BulkAction.NAME + "[s][p]", BulkAction.NAME + "[s][r]"};
        interceptTransportActions(bulkShardActions);

        List<String> indices = new ArrayList<>();
        BulkRequest bulkRequest = new BulkRequest();
        int numIndexRequests = iterations(1, 10);
        for (int i = 0; i < numIndexRequests; i++) {
            String indexOrAlias = randomIndexOrAlias();
            bulkRequest.add(new IndexRequest(indexOrAlias, "type", "id").source(Requests.INDEX_CONTENT_TYPE, "field", "value"));
            indices.add(indexOrAlias);
        }
        int numDeleteRequests = iterations(1, 10);
        for (int i = 0; i < numDeleteRequests; i++) {
            String indexOrAlias = randomIndexOrAlias();
            bulkRequest.add(new DeleteRequest(indexOrAlias, "type", "id"));
            indices.add(indexOrAlias);
        }
        int numUpdateRequests = iterations(1, 10);
        for (int i = 0; i < numUpdateRequests; i++) {
            String indexOrAlias = randomIndexOrAlias();
            bulkRequest.add(new UpdateRequest(indexOrAlias, "type", "id").doc(Requests.INDEX_CONTENT_TYPE, "field1", "value1"));
            indices.add(indexOrAlias);
        }

        internalCluster().coordOnlyNodeClient().bulk(bulkRequest).actionGet();

        clearInterceptedActions();
        assertIndicesSubset(indices, bulkShardActions);
    }

    public void testGet() {
        String getShardAction = GetAction.NAME + "[s]";
        interceptTransportActions(getShardAction);

        GetRequest getRequest = new GetRequest(randomIndexOrAlias(), "type", "id");
        internalCluster().coordOnlyNodeClient().get(getRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(getRequest, getShardAction);
    }

    public void testExplain() {
        String explainShardAction = ExplainAction.NAME + "[s]";
        interceptTransportActions(explainShardAction);

        ExplainRequest explainRequest = new ExplainRequest(randomIndexOrAlias(), "type", "id").query(QueryBuilders.matchAllQuery());
        internalCluster().coordOnlyNodeClient().explain(explainRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(explainRequest, explainShardAction);
    }

    public void testTermVector() {
        String termVectorShardAction = TermVectorsAction.NAME + "[s]";
        interceptTransportActions(termVectorShardAction);

        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(randomIndexOrAlias(), "type", "id");
        internalCluster().coordOnlyNodeClient().termVectors(termVectorsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(termVectorsRequest, termVectorShardAction);
    }

    public void testMultiTermVector() {
        String multiTermVectorsShardAction = MultiTermVectorsAction.NAME + "[shard][s]";
        interceptTransportActions(multiTermVectorsShardAction);

        List<String> indices = new ArrayList<>();
        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        int numDocs = iterations(1, 30);
        for (int i = 0; i < numDocs; i++) {
            String indexOrAlias = randomIndexOrAlias();
            multiTermVectorsRequest.add(indexOrAlias, "type", Integer.toString(i));
            indices.add(indexOrAlias);
        }
        internalCluster().coordOnlyNodeClient().multiTermVectors(multiTermVectorsRequest).actionGet();

        clearInterceptedActions();
        assertIndicesSubset(indices, multiTermVectorsShardAction);
    }

    public void testMultiGet() {
        String multiGetShardAction = MultiGetAction.NAME + "[shard][s]";
        interceptTransportActions(multiGetShardAction);

        List<String> indices = new ArrayList<>();
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        int numDocs = iterations(1, 30);
        for (int i = 0; i < numDocs; i++) {
            String indexOrAlias = randomIndexOrAlias();
            multiGetRequest.add(indexOrAlias, "type", Integer.toString(i));
            indices.add(indexOrAlias);
        }
        internalCluster().coordOnlyNodeClient().multiGet(multiGetRequest).actionGet();

        clearInterceptedActions();
        assertIndicesSubset(indices, multiGetShardAction);
    }

    public void testFlush() {
        String[] indexShardActions = new String[]{TransportShardFlushAction.NAME, TransportShardFlushAction.NAME + "[r]",
                TransportShardFlushAction.NAME + "[p]"};
        interceptTransportActions(indexShardActions);

        FlushRequest flushRequest = new FlushRequest(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().flush(flushRequest).actionGet();

        clearInterceptedActions();
        String[] indices = new IndexNameExpressionResolver(Settings.EMPTY)
                .concreteIndexNames(client().admin().cluster().prepareState().get().getState(), flushRequest);
        assertIndicesSubset(Arrays.asList(indices), indexShardActions);
    }

    public void testForceMerge() {
        String mergeShardAction = ForceMergeAction.NAME + "[n]";
        interceptTransportActions(mergeShardAction);

        ForceMergeRequest mergeRequest = new ForceMergeRequest(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().forceMerge(mergeRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(mergeRequest, mergeShardAction);
    }

    public void testRefresh() {
        String[] indexShardActions = new String[]{TransportShardRefreshAction.NAME, TransportShardRefreshAction.NAME + "[r]",
                TransportShardRefreshAction.NAME + "[p]"};
        interceptTransportActions(indexShardActions);

        RefreshRequest refreshRequest = new RefreshRequest(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().refresh(refreshRequest).actionGet();

        clearInterceptedActions();
        String[] indices = new IndexNameExpressionResolver(Settings.EMPTY)
                .concreteIndexNames(client().admin().cluster().prepareState().get().getState(), refreshRequest);
        assertIndicesSubset(Arrays.asList(indices), indexShardActions);
    }

    public void testClearCache() {
        String clearCacheAction = ClearIndicesCacheAction.NAME + "[n]";
        interceptTransportActions(clearCacheAction);

        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().clearCache(clearIndicesCacheRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(clearIndicesCacheRequest, clearCacheAction);
    }

    public void testRecovery() {
        String recoveryAction = RecoveryAction.NAME + "[n]";
        interceptTransportActions(recoveryAction);

        RecoveryRequest recoveryRequest = new RecoveryRequest(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().recoveries(recoveryRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(recoveryRequest, recoveryAction);
    }

    public void testSegments() {
        String segmentsAction = IndicesSegmentsAction.NAME + "[n]";
        interceptTransportActions(segmentsAction);

        IndicesSegmentsRequest segmentsRequest = new IndicesSegmentsRequest(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().segments(segmentsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(segmentsRequest, segmentsAction);
    }

    public void testIndicesStats() {
        String indicesStats = IndicesStatsAction.NAME + "[n]";
        interceptTransportActions(indicesStats);

        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest().indices(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().stats(indicesStatsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(indicesStatsRequest, indicesStats);
    }

    public void testValidateQuery() {
        String validateQueryShardAction = ValidateQueryAction.NAME + "[s]";
        interceptTransportActions(validateQueryShardAction);

        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().validateQuery(validateQueryRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(validateQueryRequest, validateQueryShardAction);
    }

    public void testOpenIndex() {
        interceptTransportActions(OpenIndexAction.NAME);

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(randomUniqueIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().open(openIndexRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(openIndexRequest, OpenIndexAction.NAME);
    }

    public void testCloseIndex() {
        interceptTransportActions(CloseIndexAction.NAME);

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(randomUniqueIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().close(closeIndexRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(closeIndexRequest, CloseIndexAction.NAME);
    }

    public void testDeleteIndex() {
        interceptTransportActions(DeleteIndexAction.NAME);

        String[] randomIndicesOrAliases = randomUniqueIndicesOrAliases();
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(randomIndicesOrAliases);
        assertAcked(internalCluster().coordOnlyNodeClient().admin().indices().delete(deleteIndexRequest).actionGet());

        clearInterceptedActions();
        assertSameIndices(deleteIndexRequest, DeleteIndexAction.NAME);
    }

    public void testGetMappings() {
        interceptTransportActions(GetMappingsAction.NAME);

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().getMappings(getMappingsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(getMappingsRequest, GetMappingsAction.NAME);
    }

    public void testPutMapping() {
        interceptTransportActions(PutMappingAction.NAME);

        PutMappingRequest putMappingRequest = new PutMappingRequest(randomUniqueIndicesOrAliases())
                .type("type")
                .source("field", "type=text");
        internalCluster().coordOnlyNodeClient().admin().indices().putMapping(putMappingRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(putMappingRequest, PutMappingAction.NAME);
    }

    public void testGetSettings() {
        interceptTransportActions(GetSettingsAction.NAME);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(randomIndicesOrAliases());
        internalCluster().coordOnlyNodeClient().admin().indices().getSettings(getSettingsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(getSettingsRequest, GetSettingsAction.NAME);
    }

    public void testUpdateSettings() {
        interceptTransportActions(UpdateSettingsAction.NAME);

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(randomIndicesOrAliases())
                .settings(Settings.builder().put("refresh_interval", -1));
        internalCluster().coordOnlyNodeClient().admin().indices().updateSettings(updateSettingsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(updateSettingsRequest, UpdateSettingsAction.NAME);
    }

    public void testSearchQueryThenFetch() throws Exception {
        interceptTransportActions(SearchTransportService.QUERY_ACTION_NAME,
                SearchTransportService.FETCH_ID_ACTION_NAME, SearchTransportService.FREE_CONTEXT_ACTION_NAME);

        String[] randomIndicesOrAliases = randomIndicesOrAliases();
        for (int i = 0; i < randomIndicesOrAliases.length; i++) {
            client().prepareIndex(randomIndicesOrAliases[i], "type", "id-" + i).setSource("field", "value").get();
        }
        refresh();

        SearchRequest searchRequest = new SearchRequest(randomIndicesOrAliases).searchType(SearchType.QUERY_THEN_FETCH);
        SearchResponse searchResponse = internalCluster().coordOnlyNodeClient().search(searchRequest).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));

        clearInterceptedActions();
        assertSameIndices(searchRequest, SearchTransportService.QUERY_ACTION_NAME, SearchTransportService.FETCH_ID_ACTION_NAME);
        //free context messages are not necessarily sent, but if they are, check their indices
        assertSameIndicesOptionalRequests(searchRequest, SearchTransportService.FREE_CONTEXT_ACTION_NAME);
    }

    public void testSearchDfsQueryThenFetch() throws Exception {
        interceptTransportActions(SearchTransportService.DFS_ACTION_NAME, SearchTransportService.QUERY_ID_ACTION_NAME,
                SearchTransportService.FETCH_ID_ACTION_NAME, SearchTransportService.FREE_CONTEXT_ACTION_NAME);

        String[] randomIndicesOrAliases = randomIndicesOrAliases();
        for (int i = 0; i < randomIndicesOrAliases.length; i++) {
            client().prepareIndex(randomIndicesOrAliases[i], "type", "id-" + i).setSource("field", "value").get();
        }
        refresh();

        SearchRequest searchRequest = new SearchRequest(randomIndicesOrAliases).searchType(SearchType.DFS_QUERY_THEN_FETCH);
        SearchResponse searchResponse = internalCluster().coordOnlyNodeClient().search(searchRequest).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));

        clearInterceptedActions();
        assertSameIndices(searchRequest, SearchTransportService.DFS_ACTION_NAME, SearchTransportService.QUERY_ID_ACTION_NAME,
                SearchTransportService.FETCH_ID_ACTION_NAME);
        //free context messages are not necessarily sent, but if they are, check their indices
        assertSameIndicesOptionalRequests(searchRequest, SearchTransportService.FREE_CONTEXT_ACTION_NAME);
    }
}
