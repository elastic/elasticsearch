/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.core.querydsl.query.SemanticQueryTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceResolverTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdown() {
        threadPool.shutdown();
    }

    public void testWithMultipleInferenceIds() {
        String query = """
              from test1,test2
              | where match(semantic_text_field, "x") AND match(host, "y")
            """;

        Map<String, Map<String, String>> inferenceIds = Map.of(
            "test1",
            Map.of("semantic_text_field", "inference1"),
            "test2",
            Map.of("semantic_text_field", "inference2")
        );

        VerificationException ve = expectThrows(VerificationException.class, () -> setInferenceResults(query, false, inferenceIds));

        assertThat(
            ve.getMessage(),
            containsString("[MATCH] function cannot operate on [semantic_text_field] because it is configured with multiple inference IDs.")
        );
    }

    public void testWithCCS() {
        String query = """
              from test
              | where match(semantic_text_field, "x") AND match(host, "y")
            """;
        VerificationException ve = expectThrows(VerificationException.class, () -> setInferenceResults(query, true));

        assertThat(ve.getMessage(), containsString("[MATCH] function does not allow semantic_text fields with cross cluster queries."));
    }

    public void testWithNoInferenceId() {
        String query = """
              from test1,test2
              | where match(st_ip, "x") AND match(host, "y")
            """;
        VerificationException ve = expectThrows(VerificationException.class, () -> setInferenceResults(query, false));

        assertThat(ve.getMessage(), containsString("[MATCH] function needs a configured inference ID but none could be found for [st_ip]"));
    }

    public void testValid() {
        String query = """
              from test1,test2
              | where match(semantic_text_field, "x") AND match(host, "y")
              | where match(semantic_text_field, "abcdef")
            """;
        AtomicInteger semanticMatchCount = new AtomicInteger(0);
        AtomicInteger nonSemanticMatchCount = new AtomicInteger(0);
        LogicalPlan plan = setInferenceResults(query, false);
        plan.forEachExpressionDown(Match.class, match -> {
            if (match.field().dataType() == DataType.SEMANTIC_TEXT) {
                assertNotNull(match.inferenceResults());
                semanticMatchCount.getAndIncrement();
            } else {
                assertNull(match.inferenceResults());
                nonSemanticMatchCount.getAndIncrement();
            }
        });

        assertEquals(2, semanticMatchCount.get());
        assertEquals(1, nonSemanticMatchCount.get());
    }

    private LogicalPlan setInferenceResults(String query, boolean isCrossClusterSearch) {
        Map<String, String> fieldInferenceIds = Map.of("semantic_text_field", "test_inference_id");

        Map<String, Map<String, String>> inferenceIds = Map.of("test1", fieldInferenceIds, "test2", fieldInferenceIds);

        return setInferenceResults(query, isCrossClusterSearch, inferenceIds);
    }

    private LogicalPlan setInferenceResults(String query, boolean isCrossClusterSearch, Map<String, Map<String, String>> inferenceIds) {
        LogicalPlan analyzedPlan = analyzedLogicalPlan(query, analyzer());

        AtomicReference<LogicalPlan> newPlanRef = new AtomicReference<>();
        inferenceResolver(inferenceIds).setInferenceResults(
            analyzedPlan,
            esqlExecutionInfo(isCrossClusterSearch),
            new PlainActionFuture<Result>(),
            (plan, next) -> newPlanRef.set(plan)
        );
        return newPlanRef.get();
    }

    private EsqlExecutionInfo esqlExecutionInfo(boolean isCrossClusterSearch) {
        EsqlExecutionInfo esqlExecutionInfo = mock(EsqlExecutionInfo.class);
        when(esqlExecutionInfo.isCrossClusterSearch()).thenReturn(isCrossClusterSearch);
        return esqlExecutionInfo;
    }

    private Analyzer analyzer() {
        EsIndex esIndex = new EsIndex(
            "test1,test2",
            EsqlTestUtils.loadMapping("mapping-semantic_text.json"),
            Map.of("test1", IndexMode.STANDARD, "test2", IndexMode.STANDARD)
        );
        IndexResolution indexResolution = IndexResolution.valid(esIndex);

        return new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolution,
                AnalyzerTestUtils.defaultEnrichResolution()
            ),
            TEST_VERIFIER
        );
    }

    private LogicalPlan analyzedLogicalPlan(String query, Analyzer analyzer) {
        LogicalPlan plan = new EsqlParser().createStatement(query);
        return analyzer.analyze(plan);
    }

    private Metadata clusterMetadata(Map<String, Map<String, String>> inferenceIds) {
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();

        for (String indexName : inferenceIds.keySet()) {
            Map<String, InferenceFieldMetadata> indexInferenceIds = new HashMap<>();
            for (String fieldName : inferenceIds.get(indexName).keySet()) {
                String inferenceId = inferenceIds.get(indexName).get(fieldName);
                indexInferenceIds.put(fieldName, new InferenceFieldMetadata(fieldName, inferenceId, inferenceId, new String[0]));
            }
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getInferenceFields()).thenReturn(indexInferenceIds);
            indexMetadataMap.put(indexName, indexMetadata);
        }

        Metadata metadata = mock(Metadata.class);
        when(metadata.getIndices()).thenReturn(indexMetadataMap);
        return metadata;
    }

    @SuppressWarnings("unchecked")
    private Client client() {
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        doAnswer(mock -> {
            ActionListener<InferenceAction.Response> listener = (ActionListener<InferenceAction.Response>) mock.getArguments()[2];
            TextExpansionResults inferenceResults = SemanticQueryTests.randomTextExpansionResults();

            SparseEmbeddingResults serviceResults = SparseEmbeddingResults.of(List.of(inferenceResults));
            InferenceAction.Response response = new InferenceAction.Response(serviceResults);

            listener.onResponse(response);
            return null;
        }).when(client).execute(eq(InferenceAction.INSTANCE), any(), any());

        return client;
    }

    private InferenceResolver inferenceResolver(Map<String, Map<String, String>> inferenceIds) {
        Metadata metadata = clusterMetadata(inferenceIds);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMetadata()).thenReturn(metadata);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        return new InferenceResolver(client(), clusterService);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

}
