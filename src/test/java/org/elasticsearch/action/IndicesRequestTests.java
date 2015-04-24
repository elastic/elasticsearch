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

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.count.CountAction;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.exists.ExistsAction;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.percolate.MultiPercolateAction;
import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.SUITE, numClientNodes = 1)
@Slow
public class IndicesRequestTests extends ElasticsearchIntegrationTest {

    private final List<String> indices = new ArrayList<>();

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
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, InterceptingTransportService.class.getName())
                .build();
    }

    @Before
    public void setup() {
        int numIndices = iterations(1, 5);
        for (int i = 0; i < numIndices; i++) {
            indices.add("test" + i);
        }
        for (String index : indices) {
            assertAcked(prepareCreate(index).addAlias(new Alias(index + "-alias")));
        }
        ensureGreen();
    }

    @After
    public void cleanUp() {
        assertAllRequestsHaveBeenConsumed();
        indices.clear();
    }

    @Test
    public void testGetFieldMappings() {
        String getFieldMappingsShardAction = GetFieldMappingsAction.NAME + "[index][s]";
        interceptTransportActions(getFieldMappingsShardAction);

        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest();
        getFieldMappingsRequest.indices(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().getFieldMappings(getFieldMappingsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(getFieldMappingsRequest, getFieldMappingsShardAction);
    }

    @Test
    public void testAnalyze() {
        String analyzeShardAction = AnalyzeAction.NAME + "[s]";
        interceptTransportActions(analyzeShardAction);

        AnalyzeRequest analyzeRequest = new AnalyzeRequest(randomIndexOrAlias());
        analyzeRequest.text("text");
        internalCluster().clientNodeClient().admin().indices().analyze(analyzeRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(analyzeRequest, analyzeShardAction);
    }

    @Test
    public void testIndex() {
        String[] indexShardActions = new String[]{IndexAction.NAME, IndexAction.NAME + "[r]"};
        interceptTransportActions(indexShardActions);

        IndexRequest indexRequest = new IndexRequest(randomIndexOrAlias(), "type", "id").source("field", "value");
        internalCluster().clientNodeClient().index(indexRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(indexRequest, indexShardActions);
    }

    @Test
    public void testDelete() {
        String[] deleteShardActions = new String[]{DeleteAction.NAME, DeleteAction.NAME + "[r]"};
        interceptTransportActions(deleteShardActions);

        DeleteRequest deleteRequest = new DeleteRequest(randomIndexOrAlias(), "type", "id");
        internalCluster().clientNodeClient().delete(deleteRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(deleteRequest, deleteShardActions);
    }

    @Test
    public void testUpdate() {
        //update action goes to the primary, index op gets executed locally, then replicated
        String[] updateShardActions = new String[]{UpdateAction.NAME, IndexAction.NAME + "[r]"};
        interceptTransportActions(updateShardActions);

        String indexOrAlias = randomIndexOrAlias();
        client().prepareIndex(indexOrAlias, "type", "id").setSource("field", "value").get();
        UpdateRequest updateRequest = new UpdateRequest(indexOrAlias, "type", "id").doc("field1", "value1");
        UpdateResponse updateResponse = internalCluster().clientNodeClient().update(updateRequest).actionGet();
        assertThat(updateResponse.isCreated(), equalTo(false));

        clearInterceptedActions();
        assertSameIndices(updateRequest, updateShardActions);
    }

    @Test
    public void testUpdateUpsert() {
        //update action goes to the primary, index op gets executed locally, then replicated
        String[] updateShardActions = new String[]{UpdateAction.NAME, IndexAction.NAME + "[r]"};
        interceptTransportActions(updateShardActions);

        String indexOrAlias = randomIndexOrAlias();
        UpdateRequest updateRequest = new UpdateRequest(indexOrAlias, "type", "id").upsert("field", "value").doc("field1", "value1");
        UpdateResponse updateResponse = internalCluster().clientNodeClient().update(updateRequest).actionGet();
        assertThat(updateResponse.isCreated(), equalTo(true));

        clearInterceptedActions();
        assertSameIndices(updateRequest, updateShardActions);
    }

    @Test
    public void testUpdateDelete() {
        //update action goes to the primary, delete op gets executed locally, then replicated
        String[] updateShardActions = new String[]{UpdateAction.NAME, DeleteAction.NAME + "[r]"};
        interceptTransportActions(updateShardActions);

        String indexOrAlias = randomIndexOrAlias();
        client().prepareIndex(indexOrAlias, "type", "id").setSource("field", "value").get();
        UpdateRequest updateRequest = new UpdateRequest(indexOrAlias, "type", "id").script("ctx.op='delete'");
        UpdateResponse updateResponse = internalCluster().clientNodeClient().update(updateRequest).actionGet();
        assertThat(updateResponse.isCreated(), equalTo(false));

        clearInterceptedActions();
        assertSameIndices(updateRequest, updateShardActions);
    }

    @Test
    public void testDeleteByQuery() {
        String[] deleteByQueryShardActions = new String[]{DeleteByQueryAction.NAME + "[s]", DeleteByQueryAction.NAME + "[s][r]"};
        interceptTransportActions(deleteByQueryShardActions);

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(randomIndicesOrAliases()).source(new QuerySourceBuilder().setQuery(QueryBuilders.matchAllQuery()));
        internalCluster().clientNodeClient().deleteByQuery(deleteByQueryRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(deleteByQueryRequest, deleteByQueryShardActions);
    }

    @Test
    public void testBulk() {
        String[] bulkShardActions = new String[]{BulkAction.NAME + "[s]", BulkAction.NAME + "[s][r]"};
        interceptTransportActions(bulkShardActions);

        List<String> indices = new ArrayList<>();
        BulkRequest bulkRequest = new BulkRequest();
        int numIndexRequests = iterations(1, 10);
        for (int i = 0; i < numIndexRequests; i++) {
            String indexOrAlias = randomIndexOrAlias();
            bulkRequest.add(new IndexRequest(indexOrAlias, "type", "id").source("field", "value"));
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
            bulkRequest.add(new UpdateRequest(indexOrAlias, "type", "id").doc("field1", "value1"));
            indices.add(indexOrAlias);
        }

        internalCluster().clientNodeClient().bulk(bulkRequest).actionGet();

        clearInterceptedActions();
        assertIndicesSubset(indices, bulkShardActions);
    }

    @Test
    public void testGet() {
        String getShardAction = GetAction.NAME + "[s]";
        interceptTransportActions(getShardAction);

        GetRequest getRequest = new GetRequest(randomIndexOrAlias(), "type", "id");
        internalCluster().clientNodeClient().get(getRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(getRequest, getShardAction);
    }

    @Test
    public void testExplain() {
        String explainShardAction = ExplainAction.NAME + "[s]";
        interceptTransportActions(explainShardAction);

        ExplainRequest explainRequest = new ExplainRequest(randomIndexOrAlias(), "type", "id").source(new QuerySourceBuilder().setQuery(QueryBuilders.matchAllQuery()));
        internalCluster().clientNodeClient().explain(explainRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(explainRequest, explainShardAction);
    }

    @Test
    public void testTermVector() {
        String termVectorShardAction = TermVectorsAction.NAME + "[s]";
        interceptTransportActions(termVectorShardAction);

        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(randomIndexOrAlias(), "type", "id");
        internalCluster().clientNodeClient().termVectors(termVectorsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(termVectorsRequest, termVectorShardAction);
    }

    @Test
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
        internalCluster().clientNodeClient().multiTermVectors(multiTermVectorsRequest).actionGet();

        clearInterceptedActions();
        assertIndicesSubset(indices, multiTermVectorsShardAction);
    }

    @Test
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
        internalCluster().clientNodeClient().multiGet(multiGetRequest).actionGet();

        clearInterceptedActions();
        assertIndicesSubset(indices, multiGetShardAction);
    }

    @Test
    public void testCount() {
        String countShardAction = CountAction.NAME + "[s]";
        interceptTransportActions(countShardAction);

        CountRequest countRequest = new CountRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().count(countRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(countRequest, countShardAction);
    }

    @Test
    public void testExists() {
        String existsShardAction = ExistsAction.NAME + "[s]";
        interceptTransportActions(existsShardAction);

        ExistsRequest existsRequest = new ExistsRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().exists(existsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(existsRequest, existsShardAction);
    }

    @Test
    public void testFlush() {
        String flushShardAction = FlushAction.NAME + "[s]";
        interceptTransportActions(flushShardAction);

        FlushRequest flushRequest = new FlushRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().flush(flushRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(flushRequest, flushShardAction);
    }

    @Test
    public void testOptimize() {
        String optimizeShardAction = OptimizeAction.NAME + "[s]";
        interceptTransportActions(optimizeShardAction);

        OptimizeRequest optimizeRequest = new OptimizeRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().optimize(optimizeRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(optimizeRequest, optimizeShardAction);
    }

    @Test
    public void testRefresh() {
        String refreshShardAction = RefreshAction.NAME + "[s]";
        interceptTransportActions(refreshShardAction);

        RefreshRequest refreshRequest = new RefreshRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().refresh(refreshRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(refreshRequest, refreshShardAction);
    }

    @Test
    public void testClearCache() {
        String clearCacheAction = ClearIndicesCacheAction.NAME + "[s]";
        interceptTransportActions(clearCacheAction);

        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().clearCache(clearIndicesCacheRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(clearIndicesCacheRequest, clearCacheAction);
    }

    @Test
    public void testRecovery() {
        String recoveryAction = RecoveryAction.NAME + "[s]";
        interceptTransportActions(recoveryAction);

        RecoveryRequest recoveryRequest = new RecoveryRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().recoveries(recoveryRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(recoveryRequest, recoveryAction);
    }

    @Test
    public void testSegments() {
        String segmentsAction = IndicesSegmentsAction.NAME + "[s]";
        interceptTransportActions(segmentsAction);

        IndicesSegmentsRequest segmentsRequest = new IndicesSegmentsRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().segments(segmentsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(segmentsRequest, segmentsAction);
    }

    @Test
    public void testIndicesStats() {
        String indicesStats = IndicesStatsAction.NAME + "[s]";
        interceptTransportActions(indicesStats);

        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest().indices(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().stats(indicesStatsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(indicesStatsRequest, indicesStats);
    }

    @Test
    public void testSuggest() {
        String suggestAction = SuggestAction.NAME + "[s]";
        interceptTransportActions(suggestAction);

        SuggestRequest suggestRequest = new SuggestRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().suggest(suggestRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(suggestRequest, suggestAction);
    }

    @Test
    public void testValidateQuery() {
        String validateQueryShardAction = ValidateQueryAction.NAME + "[s]";
        interceptTransportActions(validateQueryShardAction);

        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().validateQuery(validateQueryRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(validateQueryRequest, validateQueryShardAction);
    }

    @Test
    public void testPercolate() {
        String percolateShardAction = PercolateAction.NAME + "[s]";
        interceptTransportActions(percolateShardAction);

        client().prepareIndex("test-get", "type", "1").setSource("field","value").get();

        PercolateRequest percolateRequest = new PercolateRequest().indices(randomIndicesOrAliases()).documentType("type");
        if (randomBoolean()) {
            percolateRequest.getRequest(new GetRequest("test-get", "type", "1"));
        } else {
            percolateRequest.source("\"field\":\"value\"");
        }
        internalCluster().clientNodeClient().percolate(percolateRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(percolateRequest, percolateShardAction);
    }

    @Test
    public void testMultiPercolate() {
        String multiPercolateShardAction = MultiPercolateAction.NAME + "[shard][s]";
        interceptTransportActions(multiPercolateShardAction);

        client().prepareIndex("test-get", "type", "1").setSource("field", "value").get();

        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest();
        List<String> indices = new ArrayList<>();
        int numRequests = iterations(1, 30);
        for (int i = 0; i < numRequests; i++) {
            String[] indicesOrAliases = randomIndicesOrAliases();
            Collections.addAll(indices, indicesOrAliases);
            PercolateRequest percolateRequest = new PercolateRequest().indices(indicesOrAliases).documentType("type");
            if (randomBoolean()) {
                percolateRequest.getRequest(new GetRequest("test-get", "type", "1"));
            } else {
                percolateRequest.source("\"field\":\"value\"");
            }
            multiPercolateRequest.add(percolateRequest);
        }

        internalCluster().clientNodeClient().multiPercolate(multiPercolateRequest).actionGet();

        clearInterceptedActions();
        assertIndicesSubset(indices, multiPercolateShardAction);
    }

    @Test
    public void testOpenIndex() {
        interceptTransportActions(OpenIndexAction.NAME);

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(randomUniqueIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().open(openIndexRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(openIndexRequest, OpenIndexAction.NAME);
    }

    @Test
    public void testCloseIndex() {
        interceptTransportActions(CloseIndexAction.NAME);

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(randomUniqueIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().close(closeIndexRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(closeIndexRequest, CloseIndexAction.NAME);
    }

    @Test
    public void testDeleteIndex() {
        interceptTransportActions(DeleteIndexAction.NAME);

        String[] randomIndicesOrAliases = randomUniqueIndicesOrAliases();
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(randomIndicesOrAliases);
        assertAcked(internalCluster().clientNodeClient().admin().indices().delete(deleteIndexRequest).actionGet());

        clearInterceptedActions();
        assertSameIndices(deleteIndexRequest, DeleteIndexAction.NAME);
    }

    @Test
    public void testGetMappings() {
        interceptTransportActions(GetMappingsAction.NAME);

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().getMappings(getMappingsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(getMappingsRequest, GetMappingsAction.NAME);
    }

    @Test
    public void testPutMapping() {
        interceptTransportActions(PutMappingAction.NAME);

        PutMappingRequest putMappingRequest = new PutMappingRequest(randomUniqueIndicesOrAliases()).type("type").source("field", "type=string");
        internalCluster().clientNodeClient().admin().indices().putMapping(putMappingRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(putMappingRequest, PutMappingAction.NAME);
    }

    @Test
    public void testGetSettings() {
        interceptTransportActions(GetSettingsAction.NAME);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(randomIndicesOrAliases());
        internalCluster().clientNodeClient().admin().indices().getSettings(getSettingsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(getSettingsRequest, GetSettingsAction.NAME);
    }

    @Test
    public void testUpdateSettings() {
        interceptTransportActions(UpdateSettingsAction.NAME);

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(randomIndicesOrAliases()).settings(ImmutableSettings.builder().put("refresh_interval", -1));
        internalCluster().clientNodeClient().admin().indices().updateSettings(updateSettingsRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(updateSettingsRequest, UpdateSettingsAction.NAME);
    }

    @Test
    public void testSearchQueryThenFetch() throws Exception {
        interceptTransportActions(SearchServiceTransportAction.QUERY_ACTION_NAME,
                SearchServiceTransportAction.FETCH_ID_ACTION_NAME, SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);

        String[] randomIndicesOrAliases = randomIndicesOrAliases();
        for (int i = 0; i < randomIndicesOrAliases.length; i++) {
            client().prepareIndex(randomIndicesOrAliases[i], "type", "id-" + i).setSource("field", "value").get();
        }
        refresh();

        SearchRequest searchRequest = new SearchRequest(randomIndicesOrAliases).searchType(SearchType.QUERY_THEN_FETCH);
        SearchResponse searchResponse = internalCluster().clientNodeClient().search(searchRequest).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), greaterThan(0l));

        clearInterceptedActions();
        assertSameIndices(searchRequest, SearchServiceTransportAction.QUERY_ACTION_NAME, SearchServiceTransportAction.FETCH_ID_ACTION_NAME);
        //free context messages are not necessarily sent, but if they are, check their indices
        assertSameIndicesOptionalRequests(searchRequest, SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);
    }

    @Test
    public void testSearchDfsQueryThenFetch() throws Exception {
        interceptTransportActions(SearchServiceTransportAction.DFS_ACTION_NAME, SearchServiceTransportAction.QUERY_ID_ACTION_NAME,
                SearchServiceTransportAction.FETCH_ID_ACTION_NAME, SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);

        String[] randomIndicesOrAliases = randomIndicesOrAliases();
        for (int i = 0; i < randomIndicesOrAliases.length; i++) {
            client().prepareIndex(randomIndicesOrAliases[i], "type", "id-" + i).setSource("field", "value").get();
        }
        refresh();

        SearchRequest searchRequest = new SearchRequest(randomIndicesOrAliases).searchType(SearchType.DFS_QUERY_THEN_FETCH);
        SearchResponse searchResponse = internalCluster().clientNodeClient().search(searchRequest).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), greaterThan(0l));

        clearInterceptedActions();
        assertSameIndices(searchRequest, SearchServiceTransportAction.DFS_ACTION_NAME, SearchServiceTransportAction.QUERY_ID_ACTION_NAME,
                SearchServiceTransportAction.FETCH_ID_ACTION_NAME);
        //free context messages are not necessarily sent, but if they are, check their indices
        assertSameIndicesOptionalRequests(searchRequest, SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);
    }

    @Test
    public void testSearchQueryAndFetch() throws Exception {
        interceptTransportActions(SearchServiceTransportAction.QUERY_FETCH_ACTION_NAME,
                SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);

        String[] randomIndicesOrAliases = randomIndicesOrAliases();
        for (int i = 0; i < randomIndicesOrAliases.length; i++) {
            client().prepareIndex(randomIndicesOrAliases[i], "type", "id-" + i).setSource("field", "value").get();
        }
        refresh();

        SearchRequest searchRequest = new SearchRequest(randomIndicesOrAliases).searchType(SearchType.QUERY_AND_FETCH);
        SearchResponse searchResponse = internalCluster().clientNodeClient().search(searchRequest).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), greaterThan(0l));

        clearInterceptedActions();
        assertSameIndices(searchRequest, SearchServiceTransportAction.QUERY_FETCH_ACTION_NAME);
        //free context messages are not necessarily sent, but if they are, check their indices
        assertSameIndicesOptionalRequests(searchRequest, SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);
    }

    @Test
    public void testSearchDfsQueryAndFetch() throws Exception {
        interceptTransportActions(SearchServiceTransportAction.QUERY_QUERY_FETCH_ACTION_NAME,
                SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);

        String[] randomIndicesOrAliases = randomIndicesOrAliases();
        for (int i = 0; i < randomIndicesOrAliases.length; i++) {
            client().prepareIndex(randomIndicesOrAliases[i], "type", "id-" + i).setSource("field", "value").get();
        }
        refresh();

        SearchRequest searchRequest = new SearchRequest(randomIndicesOrAliases).searchType(SearchType.DFS_QUERY_AND_FETCH);
        SearchResponse searchResponse = internalCluster().clientNodeClient().search(searchRequest).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), greaterThan(0l));

        clearInterceptedActions();
        assertSameIndices(searchRequest, SearchServiceTransportAction.QUERY_QUERY_FETCH_ACTION_NAME);
        //free context messages are not necessarily sent, but if they are, check their indices
        assertSameIndicesOptionalRequests(searchRequest, SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);
    }

    @Test
    public void testSearchScan() throws Exception {
        interceptTransportActions(SearchServiceTransportAction.SCAN_ACTION_NAME);

        String[] randomIndicesOrAliases = randomIndicesOrAliases();
        for (int i = 0; i < randomIndicesOrAliases.length; i++) {
            client().prepareIndex(randomIndicesOrAliases[i], "type", "id-" + i).setSource("field", "value").get();
        }
        refresh();

        SearchRequest searchRequest = new SearchRequest(randomIndicesOrAliases).searchType(SearchType.SCAN).scroll(new TimeValue(500));
        SearchResponse searchResponse = internalCluster().clientNodeClient().search(searchRequest).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), greaterThan(0l));

        client().prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();

        clearInterceptedActions();
        assertSameIndices(searchRequest, SearchServiceTransportAction.SCAN_ACTION_NAME);
    }

    @Test
    public void testMoreLikeThis() {
        interceptTransportActions(GetAction.NAME + "[s]", SearchServiceTransportAction.QUERY_ACTION_NAME,
                SearchServiceTransportAction.FETCH_ID_ACTION_NAME, SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);

        String[] randomIndicesOrAliases = randomIndicesOrAliases();
        for (int i = 0; i < randomIndicesOrAliases.length; i++) {
            client().prepareIndex(randomIndicesOrAliases[i], "type", "id-" + i).setSource("field", "value").get();
        }
        refresh();

        assertAcked(prepareCreate("test-get").addAlias(new Alias("alias-get")));
        client().prepareIndex("test-get", "type", "1").setSource("field","value").get();
        String indexGet = randomBoolean() ? "test-get" : "alias-get";

        MoreLikeThisRequest moreLikeThisRequest = new MoreLikeThisRequest(indexGet).type("type").id("1")
                .searchIndices(randomIndicesOrAliases());
        internalCluster().clientNodeClient().moreLikeThis(moreLikeThisRequest).actionGet();

        clearInterceptedActions();
        //get might end up being executed locally, only optionally over the transport
        assertSameIndicesOptionalRequests(new String[]{indexGet}, GetAction.NAME + "[s]");
        //query might end up being executed locally as well, only optionally over the transport
        assertSameIndicesOptionalRequests(moreLikeThisRequest.searchIndices(), SearchServiceTransportAction.QUERY_ACTION_NAME);
        //free context messages are not necessarily sent through the transport, but if they are, check their indices
        assertSameIndicesOptionalRequests(moreLikeThisRequest.searchIndices(), SearchServiceTransportAction.FETCH_ID_ACTION_NAME, SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME);
    }

    private static void assertSameIndices(IndicesRequest originalRequest, String... actions) {
        assertSameIndices(originalRequest, false, actions);
    }

    private static void assertSameIndicesOptionalRequests(IndicesRequest originalRequest, String... actions) {
        assertSameIndices(originalRequest, true, actions);
    }

    private static void assertSameIndices(IndicesRequest originalRequest, boolean optional, String... actions) {
        for (String action : actions) {
            List<TransportRequest> requests = consumeTransportRequests(action);
            if (!optional) {
                assertThat("no internal requests intercepted for action [" + action + "]", requests.size(), greaterThan(0));
            }
            for (TransportRequest internalRequest : requests) {
                assertThat(internalRequest, instanceOf(IndicesRequest.class));
                assertThat(internalRequest.getClass().getName(), ((IndicesRequest)internalRequest).indices(), equalTo(originalRequest.indices()));
                assertThat(((IndicesRequest)internalRequest).indicesOptions(), equalTo(originalRequest.indicesOptions()));
            }
        }
    }

    private static void assertSameIndicesOptionalRequests(String[] indices, String... actions) {
        assertSameIndices(indices, true, actions);
    }

    private static void assertSameIndices(String[] indices, boolean optional, String... actions) {
        for (String action : actions) {
            List<TransportRequest> requests = consumeTransportRequests(action);
            if (!optional) {
                assertThat("no internal requests intercepted for action [" + action + "]", requests.size(), greaterThan(0));
            }
            for (TransportRequest internalRequest : requests) {
                assertThat(internalRequest, instanceOf(IndicesRequest.class));
                assertThat(internalRequest.getClass().getName(), ((IndicesRequest)internalRequest).indices(), equalTo(indices));
            }
        }
    }

    private static void assertIndicesSubset(List<String> indices, String... actions) {
        //indices returned by each bulk shard request need to be a subset of the original indices
        for (String action : actions) {
            List<TransportRequest> requests = consumeTransportRequests(action);
            assertThat("no internal requests intercepted for action [" + action + "]", requests.size(), greaterThan(0));
            for (TransportRequest internalRequest : requests) {
                assertThat(internalRequest, instanceOf(IndicesRequest.class));
                for (String index : ((IndicesRequest) internalRequest).indices()) {
                    assertThat(indices, hasItem(index));
                }
            }
        }
    }

    private String randomIndexOrAlias() {
        String index = randomFrom(indices);
        if (randomBoolean()) {
            return index + "-alias";
        } else {
            return index;
        }
    }

    private String[] randomIndicesOrAliases() {
        int count = randomIntBetween(1, indices.size() * 2); //every index has an alias
        String[] indices = new String[count];
        for (int i = 0; i < count; i++) {
            indices[i] = randomIndexOrAlias();
        }
        return indices;
    }

    private String[] randomUniqueIndicesOrAliases() {
        Set<String> uniqueIndices = new HashSet<>();
        int count = randomIntBetween(1, this.indices.size());
        while (uniqueIndices.size() < count) {
            uniqueIndices.add(randomFrom(this.indices));
        }
        String[] indices = new String[count];
        int i = 0;
        for (String index : uniqueIndices) {
            indices[i++] = randomBoolean() ? index + "-alias" : index;
        }
        return indices;
    }

    private static void assertAllRequestsHaveBeenConsumed() {
        Iterable<TransportService> transportServices = internalCluster().getInstances(TransportService.class);
        for (TransportService transportService : transportServices) {
            assertThat(((InterceptingTransportService)transportService).requests.entrySet(), emptyIterable());
        }
    }

    private static void clearInterceptedActions() {
        Iterable<TransportService> transportServices = internalCluster().getInstances(TransportService.class);
        for (TransportService transportService : transportServices) {
            ((InterceptingTransportService) transportService).clearInterceptedActions();
        }
    }

    private static void interceptTransportActions(String... actions) {
        Iterable<TransportService> transportServices = internalCluster().getInstances(TransportService.class);
        for (TransportService transportService : transportServices) {
            ((InterceptingTransportService) transportService).interceptTransportActions(actions);
        }
    }

    private static List<TransportRequest> consumeTransportRequests(String action) {
        List<TransportRequest> requests = new ArrayList<>();
        Iterable<TransportService> transportServices = internalCluster().getInstances(TransportService.class);
        for (TransportService transportService : transportServices) {
            List<TransportRequest> transportRequests = ((InterceptingTransportService) transportService).consumeRequests(action);
            if (transportRequests != null) {
                requests.addAll(transportRequests);
            }
        }
        return requests;
    }

    public static class InterceptingTransportService extends TransportService {

        private final Set<String> actions = new HashSet<>();

        private final Map<String, List<TransportRequest>> requests = new HashMap<>();

        @Inject
        public InterceptingTransportService(Settings settings, Transport transport, ThreadPool threadPool) {
            super(settings, transport, threadPool);
        }

        synchronized List<TransportRequest> consumeRequests(String action) {
            return requests.remove(action);
        }

        synchronized void interceptTransportActions(String... actions) {
            Collections.addAll(this.actions, actions);
        }

        synchronized void clearInterceptedActions() {
            actions.clear();
        }

        @Override
        public <Request extends TransportRequest> void registerRequestHandler(String action, Class<Request> request, String executor, boolean forceExecution, TransportRequestHandler<Request> handler) {
            super.registerRequestHandler(action, request, executor, forceExecution, new InterceptingRequestHandler(action, handler));
        }

        private class InterceptingRequestHandler implements TransportRequestHandler {

            private final TransportRequestHandler requestHandler;
            private final String action;

            InterceptingRequestHandler(String action, TransportRequestHandler requestHandler) {
                this.requestHandler = requestHandler;
                this.action = action;
            }

            @Override
            public void messageReceived(TransportRequest request, TransportChannel channel) throws Exception {
                synchronized (InterceptingTransportService.this) {
                    if (actions.contains(action)) {
                        List<TransportRequest> requestList = requests.get(action);
                        if (requestList == null) {
                            requestList = new ArrayList<>();
                            requestList.add(request);
                            requests.put(action, requestList);
                        } else {
                            requestList.add(request);
                        }
                    }
                }
                requestHandler.messageReceived(request, channel);
            }
        }
    }
}
