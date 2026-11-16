/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.search.SearchCaptureContext;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.activity.QueryCaptureContext;
import org.elasticsearch.common.logging.activity.QueryLogger;
import org.elasticsearch.common.logging.activity.QuerySink;
import org.elasticsearch.common.logging.activity.QuerySinkProvider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.InterceptedQueryBuilderWrapper;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.FullyQualifiedInferenceId;
import org.elasticsearch.xpack.inference.queries.InterceptedInferenceQueryBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/** Verifies that real searches deliver completed semantic inference results to a sink loaded through SPI. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SemanticQueryCaptureIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test_index";
    private static final String FIELD_NAME = "semantic_field";
    private static final String VECTOR_FIELD_NAME = "vector_field";
    private static final String INFERENCE_ID = "capture_endpoint";
    private static final String QUERY_TEXT = "query text";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put(QueryLogger.QUERY_LOGGER_ENABLED.getKey(), false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateInferencePlugin.class,
            TestInferenceServicePlugin.class,
            ReindexPlugin.class,
            FakeMlPlugin.class,
            CapturePlugin.class
        );
    }

    @After
    public void cleanUp() {
        IntegrationTestUtils.deleteIndex(client(), INDEX_NAME);
        IntegrationTestUtils.deleteInferenceEndpoint(client(), TaskType.TEXT_EMBEDDING, INFERENCE_ID);
    }

    public void testSemanticQueryCapturesDenseVectors() throws Exception {
        assertCapturesInference(new SemanticQueryBuilder(FIELD_NAME, QUERY_TEXT));
    }

    public void testMatchQueryCapturesDenseVectors() throws Exception {
        assertCapturesInference(new MatchQueryBuilder(FIELD_NAME, QUERY_TEXT));
    }

    public void testKnnQueryVectorBuilderCapturesDenseVectors() throws Exception {
        var query = new KnnVectorQueryBuilder(
            VECTOR_FIELD_NAME,
            new TextEmbeddingQueryVectorBuilder(INFERENCE_ID, QUERY_TEXT),
            1,
            10,
            null,
            null
        );
        assertCapturesInference(query);
        assertNotNull(query.queryVectorBuilder());
    }

    private void assertCapturesInference(QueryBuilder query) throws Exception {
        IntegrationTestUtils.createInferenceEndpoint(
            client(),
            TaskType.TEXT_EMBEDDING,
            INFERENCE_ID,
            Map.of("model", "my_model", "dimensions", 16, "similarity", "cosine", "api_key", "my_api_key")
        );
        if (query instanceof KnnVectorQueryBuilder) {
            assertAcked(
                prepareCreate(INDEX_NAME).setMapping(
                    Map.of("properties", Map.of(VECTOR_FIELD_NAME, Map.of("type", "dense_vector", "dims", 16, "similarity", "cosine")))
                )
            );
            client().prepareIndex(INDEX_NAME).setId("1").setSource(Map.of(VECTOR_FIELD_NAME, Collections.nCopies(16, 1.0f))).get();
        } else {
            try (var mapping = IntegrationTestUtils.generateSemanticTextMapping(Map.of(FIELD_NAME, INFERENCE_ID))) {
                assertAcked(prepareCreate(INDEX_NAME).setMapping(mapping));
            }
            // Populate the embedding mapping using ingest inference before observing query-time inference.
            client().prepareIndex(INDEX_NAME).setId("1").setSource(Map.of(FIELD_NAME, "document text")).get();
        }
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get();

        var expectedResults = client().execute(
            InferenceAction.INSTANCE,
            new InferenceAction.Request(
                TaskType.TEXT_EMBEDDING,
                INFERENCE_ID,
                List.of(QUERY_TEXT),
                Map.of(),
                InputType.INTERNAL_SEARCH,
                TEST_REQUEST_TIMEOUT,
                false
            )
        ).actionGet(TEST_REQUEST_TIMEOUT).getResults().transformToCoordinationFormat();
        assertThat(expectedResults, hasSize(1));
        InferenceResults expected = expectedResults.getFirst();
        assertThat(expected, instanceOf(MlDenseEmbeddingResults.class));
        assertEquals(16, ((MlDenseEmbeddingResults) expected).getInference().length);

        var plugin = internalCluster().getInstance(PluginsService.class).filterPlugins(CapturePlugin.class).findFirst().orElseThrow();
        assertThat(plugin.captures, hasSize(0));

        // Enable query capture and search again
        plugin.enabled = true;
        SearchRequest request = new SearchRequest(INDEX_NAME).source(new SearchSourceBuilder().query(query));
        String originalQuery = request.source().toString(QueryCaptureContext.FORMAT_PARAMS);
        assertResponse(client().search(request), response -> assertEquals(1, response.getHits().getHits().length));

        assertThat(plugin.captures, hasSize(1));
        CapturedSearch capture = plugin.captures.getFirst();
        assertTrue(capture.success());
        assertEquals(originalQuery, capture.originalQuery());
        if (query instanceof KnnVectorQueryBuilder) {
            assertFalse(capture.hasQueryVectorBuilder());
            assertArrayEquals(((MlDenseEmbeddingResults) expected).getInferenceAsFloat(), capture.queryVector(), 0.0f);
            assertTrue(capture.results().isEmpty());
        } else {
            assertThat(capture.results(), equalTo(Map.of(new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, INFERENCE_ID), expected)));
        }
    }

    private record CapturedSearch(
        String originalQuery,
        boolean success,
        Map<FullyQualifiedInferenceId, InferenceResults> results,
        float[] queryVector,
        boolean hasQueryVectorBuilder
    ) {}

    /** Keeps captures scoped to this test's node, with query logging disabled. */
    public static class CapturePlugin extends Plugin {
        private final List<CapturedSearch> captures = new CopyOnWriteArrayList<>();
        private volatile boolean enabled;
    }

    /** Loaded only for nodes with CapturePlugin installed, using the production query-sink SPI. */
    public static class CaptureSinkProvider implements QuerySinkProvider {
        private final CapturePlugin plugin;

        public CaptureSinkProvider(CapturePlugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public List<QuerySink<QueryCaptureContext>> getQuerySinks() {
            return List.of(new QuerySink<>(() -> "semantic inference capture test") {
                @Override
                public boolean isEnabled() {
                    return plugin.enabled;
                }

                @Override
                public void accept(QueryCaptureContext context) {
                    if (context instanceof SearchCaptureContext searchContext && Arrays.asList(context.getIndices()).contains(INDEX_NAME)) {
                        QueryBuilder rewritten = searchContext.getRewrittenSource().query();
                        if (rewritten instanceof InterceptedQueryBuilderWrapper wrapper) {
                            rewritten = wrapper.query();
                        }
                        if (rewritten instanceof KnnVectorQueryBuilder knn) {
                            plugin.captures.add(
                                new CapturedSearch(
                                    context.getQuery(),
                                    context.isSuccess(),
                                    Map.of(),
                                    knn.queryVector() == null ? null : knn.queryVector().asFloatVector().clone(),
                                    knn.queryVectorBuilder() != null
                                )
                            );
                            return;
                        }
                        Map<FullyQualifiedInferenceId, InferenceResults> results = switch (rewritten) {
                            case SemanticQueryBuilder semantic -> semantic.getInferenceResults();
                            case InterceptedInferenceQueryBuilder<?> intercepted -> intercepted.getInferenceResults();
                            default -> throw new IllegalStateException("Unexpected rewritten query: " + rewritten);
                        };
                        var copiedResults = new HashMap<FullyQualifiedInferenceId, InferenceResults>();
                        results.forEach((id, result) -> copiedResults.put(id, copyResult(result)));
                        // Never retain the borrowed context, response, or query builders after the callback returns.
                        plugin.captures.add(
                            new CapturedSearch(context.getQuery(), context.isSuccess(), Map.copyOf(copiedResults), null, false)
                        );
                    }
                }
            });
        }

        private static InferenceResults copyResult(InferenceResults result) {
            return switch (result) {
                case MlDenseEmbeddingResults dense -> new MlDenseEmbeddingResults(
                    dense.getResultsField(),
                    dense.getInference().clone(),
                    dense.isTruncated()
                );
                default -> throw new IllegalStateException("Unexpected inference results: " + result);
            };
        }
    }
}
