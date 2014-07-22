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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerRequest;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersRequest;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerRequest;
import org.elasticsearch.action.bench.BenchmarkCompetitorBuilder;
import org.elasticsearch.action.bench.BenchmarkRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.index.IndexDeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.percolate.TransportShardMultiPercolateAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.termvector.MultiTermVectorsRequest;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;

public class IndicesRelatedRequestTests extends ElasticsearchTestCase {

    /*
    Begin single index operations (no _all, no wildcards)
     */
    @Test
    public void testIndexRequest() {
        IndexRequest indexRequest = new IndexRequest(randomIndex());
        assertThat(indexRequest.requestedIndices(), equalTo(ImmutableSet.of(indexRequest.index())));
        
    }

    @Test
    public void testDeleteRequest() {
        DeleteRequest deleteRequest = new DeleteRequest(randomIndex());
        assertThat(deleteRequest.requestedIndices(), equalTo(ImmutableSet.of(deleteRequest.index())));
    }

    @Test
    public void testUpdateRequest() {
        UpdateRequest updateRequest = new UpdateRequest(randomIndex(), "type", "id");
        assertThat(updateRequest.requestedIndices(), equalTo(ImmutableSet.of(updateRequest.index())));
    }

    @Test
    public void testBulkRequest() {
        Set<String> expectedIndices = Sets.newHashSet();
        BulkRequest bulkRequest = new BulkRequest();
        int iters = randomInt(10);
        for (int i = 0; i < iters; i++) {
            BulkOperation bulkOperation = randomFrom(BulkOperation.values());
            String randomIndex = randomIndex();
            bulkRequest.add(bulkOperation.request(randomIndex));
            expectedIndices.add(randomIndex);
        }

        assertThat(bulkRequest.requestedIndices(), equalTo(expectedIndices));
    }

    private static enum BulkOperation {
        INDEX {
            @Override
            ActionRequest request(String index) {
                return new IndexRequest(index).source("field", "value");
            }
        },
        DELETE {
            @Override
            ActionRequest request(String index) {
                return new DeleteRequest(index);
            }
        },
        UPDATE {
            @Override
            ActionRequest request(String index) {
                return new UpdateRequest(index, "type", "id").doc("field", "value");
            }
        };

        abstract ActionRequest request(String index);
    }

    @Test
    public void testExplainRequest() {
        ExplainRequest explainRequest = new ExplainRequest(randomIndex(), "type", "id");
        assertThat(explainRequest.requestedIndices(), equalTo(ImmutableSet.of(explainRequest.index())));
    }

    @Test
    public void testAnalyzeRequest() {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("text");
        if (randomBoolean()) {
            analyzeRequest.index(randomIndex());
        }
        if (analyzeRequest.index() == null) {
            assertThat(analyzeRequest.requestedIndices(), equalTo(ImmutableSet.<String>of()));
        } else {
            assertThat(analyzeRequest.requestedIndices(), equalTo(ImmutableSet.of(analyzeRequest.index())));
        }
    }

    @Test
    public void testTermVectorRequest() {
        TermVectorRequest termVectorRequest = new TermVectorRequest(randomIndex(), "type", "id");
        assertThat(termVectorRequest.requestedIndices(), equalTo(ImmutableSet.of(termVectorRequest.index())));
    }

    @Test
    public void testMultiTermVectorRequest() {
        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        Set<String> expectedIndices = Sets.newHashSet();
        int numRequests = randomInt(10);
        for (int i = 0; i < numRequests; i++) {
            String randomIndex = randomIndex();
            multiTermVectorsRequest.add(randomIndex, "type", "id");
            expectedIndices.add(randomIndex);
        }

        assertThat(multiTermVectorsRequest.requestedIndices(), equalTo(expectedIndices));
    }

    @Test
    public void testGetRequest() {
        GetRequest getRequest = new GetRequest(randomIndex(), "type", "id");
        assertThat(getRequest.requestedIndices(), equalTo(ImmutableSet.of(getRequest.index())));
    }

    @Test
    public void testMultiGetRequest() {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        Set<String> expectedIndices = Sets.newHashSet();
        int numRequests = randomInt(10);
        for (int i = 0; i < numRequests; i++) {
            String randomIndex = randomIndex();
            multiGetRequest.add(randomIndex, "type", "id");
            expectedIndices.add(randomIndex);
        }

        assertThat(multiGetRequest.requestedIndices(), equalTo(expectedIndices));
    }

    @Test
    public void testDeleteIndexedScriptRequest() {
        DeleteIndexedScriptRequest deleteIndexedScriptRequest = new DeleteIndexedScriptRequest();
        assertThat(deleteIndexedScriptRequest.requestedIndices(), equalTo(ImmutableSet.of(ScriptService.SCRIPT_INDEX)));
    }

    @Test
    public void testPutIndexedScriptRequest() {
        PutIndexedScriptRequest putIndexedScriptRequest = new PutIndexedScriptRequest();
        assertThat(putIndexedScriptRequest.requestedIndices(), equalTo(ImmutableSet.of(ScriptService.SCRIPT_INDEX)));
    }

    @Test
    public void testGetIndexedScriptRequest() {
        GetIndexedScriptRequest getIndexedScriptRequest = new GetIndexedScriptRequest();
        assertThat(getIndexedScriptRequest.requestedIndices(), equalTo(ImmutableSet.of(ScriptService.SCRIPT_INDEX)));
    }

    @Test
    public void testShardMultiPercolateRequest() {
        TransportShardMultiPercolateAction.Request request = new TransportShardMultiPercolateAction.Request(randomIndex(), 1, "pref");
        assertThat(request.requestedIndices(), equalTo(ImmutableSet.of(request.index())));
    }

    @Test
    public void testMappingUpdatedRequest() {
        MappingUpdatedAction.MappingUpdatedRequest request = new MappingUpdatedAction.MappingUpdatedRequest(randomIndex(), "uuid", "type", null, 0, "node");
        assertThat(request.requestedIndices(), equalTo(ImmutableSet.of(request.index())));
    }

    @Test
    public void testNodeMappingRefreshRequest() {
        NodeMappingRefreshAction.NodeMappingRefreshRequest request = new NodeMappingRefreshAction.NodeMappingRefreshRequest(randomIndex(), "uuid", Strings.EMPTY_ARRAY, "node");
        assertThat(request.requestedIndices(), equalTo(ImmutableSet.of(request.index())));
    }

    @Test
    public void testIndexDeleteRequest() {
        IndexDeleteRequest request = new IndexDeleteRequest(new DeleteRequest(randomIndex()));
        assertThat(request.requestedIndices(), equalTo(ImmutableSet.of(request.index())));
    }

    @Test
    public void testPercolateShardRequest() {
        PercolateShardRequest request = new PercolateShardRequest(randomIndex(), 1);
        assertThat(request.requestedIndices(), equalTo(ImmutableSet.of(request.index())));
    }

    @Test
    public void testShardSearchRequest() {
        ShardSearchRequest request = new ShardSearchRequest(randomIndex(), 1, 1, randomFrom(SearchType.values()));
        assertThat(request.requestedIndices(), equalTo(ImmutableSet.of(request.index()) ));
    }

    private static String randomIndex() {
        return randomAsciiOfLength(randomInt(30));
    }

    /*
    End single index operations (no _all, no wildcards)
     */

    /*
    Begin multiple indices operations
     */
    @Test
    public void testSearchRequest() {
        SearchRequest searchRequest = new SearchRequest().indices(randomNonNullIndices());
        assertThat(searchRequest.requestedIndices(), equalTo(expectedIndices(searchRequest.indices())));
    }

    @Test
    public void testMultiSearchRequest() {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        Set<String> expectedIndices = Sets.newHashSet();
        int numRequests = randomInt(10);
        for (int i = 0; i < numRequests; i++) {
            SearchRequest searchRequest = new SearchRequest().indices(randomNonNullIndices());
            multiSearchRequest.add(searchRequest);
            expectedIndices.addAll(expectedIndices(searchRequest.indices()));
        }

        assertThat(multiSearchRequest.requestedIndices(), equalTo(expectedIndices));
    }

    @Test
    public void testPutWarmerRequest() {
        PutWarmerRequest putWarmerRequest = new PutWarmerRequest("name");
        SearchRequest searchRequest = new SearchRequest().indices(randomNonNullIndices());
        putWarmerRequest.searchRequest(searchRequest);
        assertThat(putWarmerRequest.requestedIndices(), equalTo(expectedIndices(searchRequest.indices())));
    }

    @Test
    public void testBenchmarkRequest() {
        BenchmarkRequest benchmarkRequest = new BenchmarkRequest();
        Set<String> expectedIndices = Sets.newHashSet();
        int numCompetitors = randomInt(10);
        for (int i = 0; i < numCompetitors; i++) {
            BenchmarkCompetitorBuilder benchmarkCompetitorBuilder = new BenchmarkCompetitorBuilder();
            int numRequests = randomInt(10);
            for (int j = 0; j < numRequests; j++) {
                SearchRequest searchRequest = new SearchRequest().indices(randomNonNullIndices());
                benchmarkCompetitorBuilder.addSearchRequest(searchRequest);
                expectedIndices.addAll(expectedIndices(searchRequest.indices()));
            }
            benchmarkRequest.addCompetitor(benchmarkCompetitorBuilder.build());
        }

        assertThat(benchmarkRequest.requestedIndices(), equalTo(expectedIndices));
    }

    @Test
    public void testMoreLikeThisRequest() {
        Set<String> expectedIndices = Sets.newHashSet();
        String randomIndex = randomIndex();
        MoreLikeThisRequest moreLikeThisRequest = new MoreLikeThisRequest(randomIndex);
        if (randomBoolean()) {
            String[] randomIndices = randomNonNullIndices();
            moreLikeThisRequest.searchIndices(randomIndices);
            expectedIndices.addAll(expectedIndices(randomIndices));
        }
        expectedIndices.add(randomIndex);

        assertThat(moreLikeThisRequest.requestedIndices(), equalTo(expectedIndices));
    }

    @Test(expected = IllegalStateException.class)
    public void testMoreLikeThisRequestIllegalState() {
        MoreLikeThisRequest moreLikeThisRequest = new MoreLikeThisRequest(null);
        moreLikeThisRequest.requestedIndices();
    }

    @Test
    public void testMultiPercolateRequest() {
        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest();
        Set<String> expectedIndices = Sets.newHashSet();
        int iters = randomInt(10);
        for (int i = 0; i < iters; i++) {
            PercolateRequest percolateRequest = new PercolateRequest().indices(randomIndices());
            expectedIndices.addAll(expectedIndices(percolateRequest.indices()));
            if (randomBoolean()) {
                String randomIndex = randomIndex();
                percolateRequest.getRequest(new GetRequest(randomIndex));
                expectedIndices.add(randomIndex);
            }
            multiPercolateRequest.add(percolateRequest);
        }

        assertThat(multiPercolateRequest.requestedIndices(), equalTo(expectedIndices));
    }

    @Test
    public void testSuggestRequest() {
        SuggestRequest suggestRequest = new SuggestRequest(randomIndices());
        assertThat(suggestRequest.requestedIndices(), equalTo(expectedIndices(suggestRequest.indices())));
    }

    @Test
    public void testClearIndicesCacheRequest() {
        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(randomIndices());
        assertThat(clearIndicesCacheRequest.requestedIndices(), equalTo(expectedIndices(clearIndicesCacheRequest.indices())));
    }

    @Test
    public void testIndicesStatsRequest() {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest().indices(randomIndices());
        assertThat(indicesStatsRequest.requestedIndices(), equalTo(expectedIndices(indicesStatsRequest.indices())));
    }

    @Test
    public void testIndicesSegmentsRequest() {
        IndicesSegmentsRequest indicesSegmentsRequest = new IndicesSegmentsRequest(randomIndices());
        assertThat(indicesSegmentsRequest.requestedIndices(), equalTo(expectedIndices(indicesSegmentsRequest.indices())));
    }

    @Test
    public void testFlushRequest() {
        FlushRequest flushRequest = new FlushRequest(randomIndices());
        assertThat(flushRequest.requestedIndices(), equalTo(expectedIndices(flushRequest.indices())));
    }

    @Test
    public void testOptimizeRequest() {
        OptimizeRequest optimizeRequest = new OptimizeRequest(randomIndices());
        assertThat(optimizeRequest.requestedIndices(), equalTo(expectedIndices(optimizeRequest.indices())));
    }

    @Test
    public void testPercolateRequest() {
        PercolateRequest percolateRequest = new PercolateRequest().indices(randomIndices());
        Set<String> expectedIndices = expectedIndices(percolateRequest.indices());
        if (randomBoolean()) {
            String randomIndex = randomIndex();
            percolateRequest.getRequest(new GetRequest(randomIndex));
            expectedIndices.add(randomIndex);
        }
        assertThat(percolateRequest.requestedIndices(), equalTo(expectedIndices));
    }

    @Test
    public void testRecoveryRequest() {
        RecoveryRequest recoveryRequest = new RecoveryRequest(randomIndices());
        assertThat(recoveryRequest.requestedIndices(), equalTo(expectedIndices(recoveryRequest.indices())));
    }

    @Test
    public void testCountRequest() {
        CountRequest countRequest = new CountRequest(randomIndices());
        assertThat(countRequest.requestedIndices(), equalTo(expectedIndices(countRequest.indices())));
    }

    @Test
    public void testRefreshRequest() {
        RefreshRequest refreshRequest = new RefreshRequest(randomIndices());
        assertThat(refreshRequest.requestedIndices(), equalTo(expectedIndices(refreshRequest.indices())));
    }

    @Test
    public void testValidateQueryRequest() {
        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest(randomIndices());
        assertThat(validateQueryRequest.requestedIndices(), equalTo(expectedIndices(validateQueryRequest.indices())));
    }

    @Test
    public void testGetSettingsRequest() {
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(randomIndices());
        assertThat(getSettingsRequest.requestedIndices(), equalTo(expectedIndices(getSettingsRequest.indices())));
    }

    @Test
    public void testUpdateSettingsRequest() {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest().indices(randomIndices());
        assertThat(updateSettingsRequest.requestedIndices(), equalTo(expectedIndices(updateSettingsRequest.indices())));
    }

    @Test
    public void testIndicesExistsRequest() {
        IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(randomIndices());
        assertThat(indicesExistsRequest.requestedIndices(), equalTo(expectedIndices(indicesExistsRequest.indices())));
    }

    @Test
    public void testTypesExistsRequest() {
        TypesExistsRequest typesExistsRequest = new TypesExistsRequest(randomIndices(), "type");
        assertThat(typesExistsRequest.requestedIndices(), equalTo(expectedIndices(typesExistsRequest.indices())));
    }

    @Test
    public void testPutMappingRequest() {
        PutMappingRequest putMappingRequest = new PutMappingRequest(randomIndices());
        assertThat(putMappingRequest.requestedIndices(), equalTo(expectedIndices(putMappingRequest.indices())));
    }

    @Test
    public void testDeleteMappingRequest() {
        DeleteMappingRequest deleteMappingRequest = new DeleteMappingRequest(randomNonEmptyIndices());
        assertThat(deleteMappingRequest.requestedIndices(), equalTo(expectedIndices(deleteMappingRequest.indices())));
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteMappingRequestIllegalStateEmptyIndices() {
        new DeleteMappingRequest(Strings.EMPTY_ARRAY).requestedIndices();
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteMappingRequestIllegalStateNullIndices() {
        new DeleteMappingRequest(null).requestedIndices();
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteMappingRequestIllegalStateNoIndices() {
        new DeleteMappingRequest().requestedIndices();
    }

    @Test
    public void testGetMappingRequest() {
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(randomIndices());
        assertThat(getMappingsRequest.requestedIndices(), equalTo(expectedIndices(getMappingsRequest.indices())));
    }

    @Test
    public void testGetFieldMappingsRequest() {
        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest().indices(randomIndices());
        assertThat(getFieldMappingsRequest.requestedIndices(), equalTo(expectedIndices(getFieldMappingsRequest.indices())));
    }

    @Test
    public void testGetWarmerRequest() {
        GetWarmersRequest getWarmersRequest = new GetWarmersRequest().indices(randomIndices());
        assertThat(getWarmersRequest.requestedIndices(), equalTo(expectedIndices(getWarmersRequest.indices())));
    }

    @Test
    public void testDeleteWarmerRequest() {
        DeleteWarmerRequest deleteWarmerRequest = new DeleteWarmerRequest("name").indices(randomIndices());
        assertThat(deleteWarmerRequest.requestedIndices(), equalTo(expectedIndices(deleteWarmerRequest.indices())));
    }

    @Test
    public void testOpenIndexRequest() {
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(randomNonEmptyIndices());
        assertThat(openIndexRequest.requestedIndices(), equalTo(expectedIndices(openIndexRequest.indices())));
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenIndexRequestIllegalStateEmptyIndices() {
        new OpenIndexRequest(Strings.EMPTY_ARRAY).requestedIndices();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenIndexRequestIllegalStateNullIndices() {
        new OpenIndexRequest(null).requestedIndices();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenIndexRequestIllegalStateNoIndices() {
        new OpenIndexRequest().requestedIndices();
    }

    @Test
    public void testCloseIndexRequest() {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(randomNonEmptyIndices());
        assertThat(closeIndexRequest.requestedIndices(), equalTo(expectedIndices(closeIndexRequest.indices())));
    }

    @Test(expected = IllegalStateException.class)
    public void testCloseIndexRequestIllegalStateEmptyIndices() {
        new CloseIndexRequest(Strings.EMPTY_ARRAY).requestedIndices();
    }

    @Test(expected = IllegalStateException.class)
    public void testCloseIndexRequestIllegalStateNullIndices() {
        new CloseIndexRequest(null).requestedIndices();
    }

    @Test(expected = IllegalStateException.class)
    public void testCloseIndexRequestIllegalStateNoIndices() {
        new CloseIndexRequest().requestedIndices();
    }

    @Test
    public void testDeleteIndexRequest() {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(randomNonEmptyIndices());
        assertThat(deleteIndexRequest.requestedIndices(), equalTo(expectedIndices(deleteIndexRequest.indices())));
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteIndexRequestIllegalStateNoIndices() {
        new DeleteIndexRequest().requestedIndices();
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteIndexRequestIllegalStateNullIndices() {
        new DeleteIndexRequest().indices((String[])null).requestedIndices();
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteIndexRequestIllegalStateEmptyIndices() {
        new DeleteIndexRequest().indices(Strings.EMPTY_ARRAY).requestedIndices();
    }

    @Test
    public void testClusterHealthRequest() {
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(randomIndices());
        assertThat(clusterHealthRequest.requestedIndices(), equalTo(expectedIndices(clusterHealthRequest.indices())));
    }

    @Test
    public void testClusterStateRequest() {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest().indices(randomIndices());
        assertThat(clusterStateRequest.requestedIndices(), equalTo(expectedIndices(clusterStateRequest.indices())));
    }

    @Test
    public void testClusterSearchShardsRequest() {
        ClusterSearchShardsRequest clusterSearchShardsRequest = new ClusterSearchShardsRequest(randomNonNullIndices());
        assertThat(clusterSearchShardsRequest.requestedIndices(), equalTo(expectedIndices(clusterSearchShardsRequest.indices())));
    }

    @Test
    public void testDeleteByQueryRequest() {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(randomIndices());
        assertThat(deleteByQueryRequest.requestedIndices(), equalTo(expectedIndices(deleteByQueryRequest.indices())));
    }

    @Test
    public void testCreateSnapshotRequest() {
        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest("repo", "snap").indices(randomIndices());
        assertThat(createSnapshotRequest.requestedIndices(), equalTo(expectedIndices(createSnapshotRequest.indices())));
    }

    @Test
    public void testGetAliasesRequest() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(randomIndices());
        assertThat(getAliasesRequest.requestedIndices(), equalTo(expectedIndices(getAliasesRequest.indices())));
    }

    @Test
    public void testIndicesAliasesRequest() {
        Set<String> expectedIndices = Sets.newHashSet();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        int iters = randomInt(10);
        for (int i = 0; i < iters; i++) {
            String[] randomIndices = randomNonEmptyIndices();
            AliasOperation aliasOperation = randomFrom(AliasOperation.values());
            indicesAliasesRequest.addAliasAction(aliasOperation.aliasActions(randomIndices));
            Collections.addAll(expectedIndices, randomIndices);
        }

        assertThat(indicesAliasesRequest.requestedIndices(), equalTo(expectedIndices));
    }

    @Test(expected = IllegalStateException.class)
    public void testIndicesAliasesRequestIllegalState() {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        AliasOperation aliasOperation = randomFrom(AliasOperation.values());
        indicesAliasesRequest.addAliasAction(aliasOperation.aliasActions(Strings.EMPTY_ARRAY));
        indicesAliasesRequest.requestedIndices();
    }

    private static enum AliasOperation {
        ADD {
            @Override
            IndicesAliasesRequest.AliasActions aliasActions(String[] indices) {
                return new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD, indices, new String[]{"alias"});
            }
        },
        DELETE {
            @Override
            IndicesAliasesRequest.AliasActions aliasActions(String[] indices) {
                return new IndicesAliasesRequest.AliasActions(AliasAction.Type.REMOVE, indices, new String[]{"alias"});
            }
        };

        abstract IndicesAliasesRequest.AliasActions aliasActions(String[] indices);
    }

    /*
    End multiple indices operations
     */

    private static Set<String> expectedIndices(String... indices) {
        if (MetaData.isAllIndices(indices)) {
            return Sets.newHashSet(MetaData.ALL);
        }
        return Sets.newHashSet(indices);
    }

    private static String[] randomIndices() {
        return randomIndices(true, true);
    }

    private static String[] randomNonEmptyIndices() {
        return randomIndices(false, false);
    }

    private static String[] randomNonNullIndices() {
        return randomIndices(true, false);
    }

    private static String[] randomIndices(boolean emptyAllowed, boolean nullAllowed) {
        String[] randomIndices;
        if (frequently()) {
            randomIndices = new String[randomIntBetween(1, 10)];
            for (int j = 0; j < randomIndices.length; j++) {
                if (j > 0 && randomBoolean()) {
                    //ensure we have duplicates
                    randomIndices[j] = randomIndices[randomInt(j-1)];
                } else {
                    randomIndices[j] = randomIndex();
                }
            }
        } else {
            int randomInt = randomInt(2);
            if (emptyAllowed && randomInt == 0) {
                randomIndices = Strings.EMPTY_ARRAY;
            } else if (nullAllowed && randomInt == 1) {
                randomIndices = null;
            } else {
                randomIndices = new String[]{MetaData.ALL};
            }
        }
        return randomIndices;
    }
}
