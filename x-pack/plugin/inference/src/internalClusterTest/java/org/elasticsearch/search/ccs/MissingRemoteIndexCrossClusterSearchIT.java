/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.vectors.EmbeddingQueryVectorBuilder;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.inference.Utils.randomInferenceStringGroup;
import static org.hamcrest.Matchers.equalTo;

/**
 * Covers a missing remote index — both a concrete name and a wildcard that matches nothing — for every query type that performs remote
 * inference, across all request modes and both {@code skip_unavailable} values.
 */
public class MissingRemoteIndexCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String MISSING_INDEX_NAME = "missing-index";
    private static final String MISSING_INDEX_WILDCARD = MISSING_INDEX_NAME + "*";

    private static final String SPARSE_INFERENCE_ID = "sparse-inference-id";
    private static final String DENSE_INFERENCE_ID = "dense-inference-id";

    private static final String TEXT_FIELD = "text-field";
    private static final String SPARSE_FIELD = "sparse-field";
    private static final String DENSE_FIELD = "dense-field";
    private static final String SEMANTIC_DENSE_FIELD = "semantic-dense-field";

    private static final String FIELD_VALUE = "value";

    private final boolean skipUnavailable;

    public MissingRemoteIndexCrossClusterSearchIT(@Name("skipUnavailable") boolean skipUnavailable) {
        this.skipUnavailable = skipUnavailable;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, skipUnavailable);
    }

    @Override
    protected boolean reuseClusters() {
        // skip_unavailable is only applied while the remote connection is configured, so each parameter value needs its own clusters.
        return false;
    }

    @Before
    public void setupClusters() throws Exception {
        final Map<String, MinimalServiceSettings> inferenceEndpoints = Map.of(
            SPARSE_INFERENCE_ID,
            sparseEmbeddingServiceSettings(),
            DENSE_INFERENCE_ID,
            embeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
        );
        final Map<String, Object> mappings = Map.of(
            TEXT_FIELD,
            textMapping(),
            SPARSE_FIELD,
            semanticTextMapping(SPARSE_INFERENCE_ID),
            DENSE_FIELD,
            semanticTextMapping(DENSE_INFERENCE_ID),
            SEMANTIC_DENSE_FIELD,
            semanticFieldMapping(DENSE_INFERENCE_ID)
        );
        final Map<String, Map<String, Object>> docs = Map.of(
            getDocId(TEXT_FIELD),
            Map.of(TEXT_FIELD, FIELD_VALUE),
            getDocId(SPARSE_FIELD),
            Map.of(SPARSE_FIELD, FIELD_VALUE),
            getDocId(DENSE_FIELD),
            Map.of(DENSE_FIELD, FIELD_VALUE),
            getDocId(SEMANTIC_DENSE_FIELD),
            Map.of(SEMANTIC_DENSE_FIELD, FIELD_VALUE)
        );
        setupTwoClusters(
            new TestIndexInfo(LOCAL_INDEX_NAME, inferenceEndpoints, mappings, docs),
            new TestIndexInfo(REMOTE_INDEX_NAME, inferenceEndpoints, mappings, docs)
        );
    }

    public void testMissingRemoteIndex() throws Exception {
        // The fix relaxes the indices options of the remote inference lookup, so the search defaults (which is what the reported failure
        // used) are pinned here, and the seed explores the rest of the option space across runs.
        for (IndicesOptions indicesOptions : List.of(SearchRequest.DEFAULT_INDICES_OPTIONS, randomIndicesOptions())) {
            for (QueryCase queryCase : queryCases()) {
                for (RequestMode mode : requestModes()) {
                    assertMissingRemoteIndex(queryCase, indicesOptions, mode);
                    assertMissingRemoteIndexWildcard(queryCase, indicesOptions, mode);
                }
            }
        }
    }

    /**
     * A missing remote index next to one that resolves. The relaxed lookup must still gather the resolvable index's inference fields,
     * otherwise the query rewrites without them and the remote hit goes missing instead of the search reporting an error.
     */
    public void testMissingRemoteIndexAlongsideResolvableRemoteIndex() throws Exception {
        final List<String> indices = List.of(
            LOCAL_INDEX_NAME,
            FULLY_QUALIFIED_REMOTE_INDEX_NAME,
            fullyQualifiedIndexName(REMOTE_CLUSTER, MISSING_INDEX_NAME)
        );
        // Lenient options stop the missing index from failing the remote, which is what leaves the resolvable one to assert on.
        final IndicesOptions indicesOptions = IndicesOptions.LENIENT_EXPAND_OPEN;

        for (QueryCase queryCase : queryCases()) {
            for (RequestMode mode : requestModes()) {
                assertBothClustersReturnTheirHit(
                    describe(queryCase, indicesOptions, mode, MISSING_INDEX_NAME + " alongside " + REMOTE_INDEX_NAME),
                    queryCase,
                    indices,
                    mode,
                    indicesOptions
                );
            }
        }
    }

    /**
     * Asserts on hit membership rather than rank: sparse embeddings score high enough that boosting the local index does not reliably
     * order it first, and this test only cares that the remote index contributed its hit.
     */
    private void assertBothClustersReturnTheirHit(
        String context,
        QueryCase queryCase,
        List<String> indices,
        RequestMode mode,
        IndicesOptions indicesOptions
    ) throws Exception {
        final SearchRequest searchRequest = new SearchRequest(indices.toArray(new String[0])).source(
            new SearchSourceBuilder().query(queryCase.query().get()).size(2)
        );
        mode.modifier().andThen(s -> s.indicesOptions(indicesOptions)).accept(searchRequest);

        final Set<SearchResult> expected = Set.of(
            new SearchResult(getExpectedLocalClusterAlias(mode.minimizesRoundTrips()), LOCAL_INDEX_NAME, queryCase.expectedDocId()),
            new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, queryCase.expectedDocId())
        );

        final SetOnce<String> scrollId = new SetOnce<>();
        try {
            assertResponse(client().search(searchRequest), response -> {
                scrollId.set(response.getScrollId());
                assertThat(
                    Arrays.stream(response.getHits().getHits())
                        .map(hit -> new SearchResult(hit.getClusterAlias(), hit.getIndex(), hit.getId()))
                        .collect(Collectors.toSet()),
                    equalTo(expected)
                );
                for (String clusterAlias : response.getClusters().getClusterAliases()) {
                    assertThat(
                        response.getClusters().getCluster(clusterAlias).getStatus(),
                        equalTo(SearchResponse.Cluster.Status.SUCCESSFUL)
                    );
                }
            });
        } catch (Exception | AssertionError e) {
            throw new AssertionError(context, e);
        } finally {
            if (scrollId.get() != null) {
                client().prepareClearScroll().addScrollId(scrollId.get()).get(TEST_REQUEST_TIMEOUT);
            }
        }
    }

    private void assertMissingRemoteIndex(QueryCase queryCase, IndicesOptions indicesOptions, RequestMode mode) throws Exception {
        // The remote cluster resolves the missing concrete name:
        // - ignoreUnavailable == false → IndexNotFoundException
        // - ignoreUnavailable == true, allowNoIndices == false → empty result set → IndexNotFoundException
        // - ignoreUnavailable == true, allowNoIndices == true → zero shards, no error
        final boolean remoteFails = indicesOptions.ignoreUnavailable() == false || indicesOptions.allowNoIndices() == false;
        assertRemoteIndexExpression(queryCase, indicesOptions, mode, MISSING_INDEX_NAME, remoteFails);
    }

    private void assertMissingRemoteIndexWildcard(QueryCase queryCase, IndicesOptions indicesOptions, RequestMode mode) throws Exception {
        // The remote cluster resolves the non-matching wildcard. The outcome depends on wildcard expansion:
        // - expandWildcardExpressions == true: fails only when allowNoIndices == false (ignoreUnavailable is irrelevant)
        // - expandWildcardExpressions == false: the wildcard is treated as a concrete name, so the concrete-name rule applies
        // (ignoreUnavailable == false || allowNoIndices == false)
        final boolean remoteFails = indicesOptions.expandWildcardExpressions()
            ? indicesOptions.allowNoIndices() == false
            : indicesOptions.ignoreUnavailable() == false || indicesOptions.allowNoIndices() == false;
        assertRemoteIndexExpression(queryCase, indicesOptions, mode, MISSING_INDEX_WILDCARD, remoteFails);
    }

    private void assertRemoteIndexExpression(
        QueryCase queryCase,
        IndicesOptions indicesOptions,
        RequestMode mode,
        String remoteIndexExpression,
        boolean remoteFails
    ) throws Exception {
        final List<String> indices = List.of(LOCAL_INDEX_NAME, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexExpression));
        final Consumer<SearchRequest> modifier = mode.modifier().andThen(s -> s.indicesOptions(indicesOptions));
        final String context = describe(queryCase, indicesOptions, mode, remoteIndexExpression);
        final List<SearchResult> localOnly = List.of(
            new SearchResult(getExpectedLocalClusterAlias(mode.minimizesRoundTrips()), LOCAL_INDEX_NAME, queryCase.expectedDocId())
        );

        if (remoteFails == false) {
            // The remote cluster silently contributes zero shards — both clusters report SUCCESSFUL.
            assertSearch(context, queryCase, indices, localOnly, null, modifier);
        } else if (skipUnavailable) {
            assertSearch(
                context,
                queryCase,
                indices,
                localOnly,
                new ClusterFailure(
                    SearchResponse.Cluster.Status.SKIPPED,
                    Set.of(new FailureCause(IndexNotFoundException.class, missingIndexError(remoteIndexExpression)))
                ),
                modifier
            );
        } else {
            try {
                assertSearchFailure(
                    queryCase.query().get(),
                    indices,
                    IndexNotFoundException.class,
                    missingIndexError(remoteIndexExpression),
                    modifier
                );
            } catch (Exception | AssertionError e) {
                throw new AssertionError(context, e);
            }
        }
    }

    private void assertSearch(
        String context,
        QueryCase queryCase,
        List<String> indices,
        List<SearchResult> expectedSearchResults,
        @Nullable ClusterFailure expectedRemoteFailure,
        Consumer<SearchRequest> modifier
    ) throws Exception {
        final SetOnce<String> scrollId = new SetOnce<>();
        try {
            assertSearchResponse(
                queryCase.query().get(),
                indices,
                expectedSearchResults,
                expectedRemoteFailure,
                modifier,
                r -> scrollId.set(r.getScrollId())
            );
        } catch (Exception | AssertionError e) {
            // A single test method covers every combination, so name the one that failed.
            throw new AssertionError(context, e);
        } finally {
            if (scrollId.get() != null) {
                client().prepareClearScroll().addScrollId(scrollId.get()).get(TEST_REQUEST_TIMEOUT);
            }
        }
    }

    private String describe(QueryCase queryCase, IndicesOptions indicesOptions, RequestMode mode, String remoteIndexExpression) {
        return Strings.format(
            "query [%s], mode [%s], remote expression [%s], skip_unavailable [%s], %s",
            queryCase.name(),
            mode.name(),
            remoteIndexExpression,
            skipUnavailable,
            indicesOptions
        );
    }

    private static IndicesOptions randomIndicesOptions() {
        return IndicesOptions.builder()
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .matchOpen(randomBoolean())
                    .matchClosed(randomBoolean())
                    .includeHidden(randomBoolean())
                    .allowEmptyExpressions(randomBoolean())
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder().allowAliasToMultipleIndices(randomBoolean()).allowClosedIndices(randomBoolean())
            )
            .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveAliases(randomBoolean()))
            .build();
    }

    private static List<RequestMode> requestModes() {
        return List.of(
            new RequestMode("minimize_roundtrips=true", true, s -> s.setCcsMinimizeRoundtrips(true)),
            new RequestMode("minimize_roundtrips=false", false, s -> s.setCcsMinimizeRoundtrips(false)),
            // Scroll turns minimization off on its own, which is the path the reported failure took.
            new RequestMode("scroll", false, s -> s.scroll(TimeValue.timeValueMinutes(1)))
        );
    }

    /**
     * Every query type that can reach the remote inference lookup. The knn cases leave the inference ID unset, since setting one makes
     * the interceptor skip remotes and the lookup never runs.
     */
    private static List<QueryCase> queryCases() {
        return List.of(
            new QueryCase(
                "knn[semantic_text]",
                () -> knnQuery(DENSE_FIELD, new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null)),
                getDocId(DENSE_FIELD)
            ),
            new QueryCase(
                "knn[semantic]",
                () -> knnQuery(SEMANTIC_DENSE_FIELD, new EmbeddingQueryVectorBuilder(null, randomInferenceStringGroup(), null)),
                getDocId(SEMANTIC_DENSE_FIELD)
            ),
            new QueryCase(
                "knn[text_embedding]",
                () -> knnQuery(DENSE_FIELD, new TextEmbeddingQueryVectorBuilder(null, randomAlphaOfLength(10))),
                getDocId(DENSE_FIELD)
            ),
            new QueryCase("match[text]", () -> new MatchQueryBuilder(TEXT_FIELD, FIELD_VALUE), getDocId(TEXT_FIELD)),
            new QueryCase("match[semantic_text]", () -> new MatchQueryBuilder(SPARSE_FIELD, FIELD_VALUE), getDocId(SPARSE_FIELD)),
            new QueryCase("sparse_vector", () -> new SparseVectorQueryBuilder(SPARSE_FIELD, null, FIELD_VALUE), getDocId(SPARSE_FIELD)),
            new QueryCase("semantic", () -> new SemanticQueryBuilder(SPARSE_FIELD, FIELD_VALUE), getDocId(SPARSE_FIELD))
        );
    }

    private static KnnVectorQueryBuilder knnQuery(String field, QueryVectorBuilder queryVectorBuilder) {
        return new KnnVectorQueryBuilder(field, queryVectorBuilder, 10, 100, 10f, null);
    }

    private static String missingIndexError(String expression) {
        return "no such index [" + expression + "]";
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }

    private record RequestMode(String name, boolean minimizesRoundTrips, Consumer<SearchRequest> modifier) {}

    private record QueryCase(String name, Supplier<QueryBuilder> query, String expectedDocId) {}
}
