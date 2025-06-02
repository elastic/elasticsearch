/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder.QUERY_VECTOR_FIELD;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasSize;

public class SparseVectorQueryBuilderTests extends AbstractQueryTestCase<SparseVectorQueryBuilder> {

    private static final String SPARSE_VECTOR_FIELD = "mySparseVectorField";
    private static final List<WeightedToken> WEIGHTED_TOKENS = List.of(new WeightedToken("foo", .42f));
    private static final int NUM_TOKENS = WEIGHTED_TOKENS.size();
    private final IndexVersion indexVersionToTest;

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface InjectSparseVectorIndexOptions {
    }

    public SparseVectorQueryBuilderTests() {
        // The sparse_vector field is not supported on versions 8.0 to 8.10. Because of this we'll only allow
        // index versions after its reintroduction.
        indexVersionToTest = randomBoolean()
            ? IndexVersion.current()
            : IndexVersionUtils.randomVersionBetween(random(), IndexVersions.NEW_SPARSE_VECTOR, IndexVersion.current());
    }

    @Override
    protected SparseVectorQueryBuilder doCreateTestQueryBuilder() {
        TokenPruningConfig tokenPruningConfig = randomBoolean()
            ? new TokenPruningConfig(randomIntBetween(1, 100), randomFloat(), randomBoolean())
            : null;
        return createTestQueryBuilder(tokenPruningConfig);
    }

    private SparseVectorQueryBuilder createTestQueryBuilder(TokenPruningConfig tokenPruningConfig) {
        SparseVectorQueryBuilder builder;
        if (randomBoolean()) {
            builder = new SparseVectorQueryBuilder(
                SPARSE_VECTOR_FIELD,
                null,
                randomAlphaOfLength(10),
                randomAlphaOfLengthBetween(10, 25),
                tokenPruningConfig != null,
                tokenPruningConfig
            );
        } else {
            builder = new SparseVectorQueryBuilder(
                SPARSE_VECTOR_FIELD,
                WEIGHTED_TOKENS,
                null,
                null,
                tokenPruningConfig != null,
                tokenPruningConfig
            );
        }

        if (randomBoolean()) {
            builder.boost((float) randomDoubleBetween(0.1, 10.0, true));
        }
        if (randomBoolean()) {
            builder.queryName(randomAlphaOfLength(4));
        }
        return builder;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MapperExtrasPlugin.class, XPackClientPlugin.class);
    }

    @Override
    protected Settings createTestIndexSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexVersionToTest).build();
    }

    @Override
    protected boolean canSimulateMethod(Method method, Object[] args) throws NoSuchMethodException {
        return method.equals(Client.class.getMethod("execute", ActionType.class, ActionRequest.class, ActionListener.class))
            && (args[0] instanceof CoordinatedInferenceAction);
    }

    @Override
    protected Object simulateMethod(Method method, Object[] args) {
        CoordinatedInferenceAction.Request request = (CoordinatedInferenceAction.Request) args[1];
        assertEquals(InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API, request.getInferenceTimeout());
        assertEquals(TrainedModelPrefixStrings.PrefixType.SEARCH, request.getPrefixType());
        assertEquals(CoordinatedInferenceAction.Request.RequestModelType.NLP_MODEL, request.getRequestModelType());

        // Randomisation cannot be used here as {@code #doAssertLuceneQuery}
        // asserts that 2 rewritten queries are the same
        var tokens = new ArrayList<WeightedToken>();
        for (int i = 0; i < NUM_TOKENS; i++) {
            tokens.add(new WeightedToken(Integer.toString(i), (i + 1) * 1.0f));
        }

        var response = InferModelAction.Response.builder()
            .setId(request.getModelId())
            .addInferenceResults(List.of(new TextExpansionResults("foo", tokens, randomBoolean())))
            .build();
        @SuppressWarnings("unchecked")  // We matched the method above.
        ActionListener<InferModelAction.Response> listener = (ActionListener<InferModelAction.Response>) args[2];
        listener.onResponse(response);
        return null;
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge("_doc", new CompressedXContent(getTestSparseVectorIndexMapping()), MapperService.MergeReason.MAPPING_UPDATE);
    }

    private String getTestSparseVectorIndexMapping() {
        if (currentTestHasIndexOptions()) {
            return "{\"properties\":{\""
                + SPARSE_VECTOR_FIELD
                + "\":{\"type\":\"sparse_vector\",\"index_options\""
                + ":{\"prune\":true,\"pruning_config\":{\"tokens_freq_ratio_threshold\""
                + ":12,\"tokens_weight_threshold\":0.6}}}}}";
        }

        return Strings.toString(PutMappingRequest.simpleMapping(SPARSE_VECTOR_FIELD, "type=sparse_vector"));
    }

    private boolean currentTestHasIndexOptions() {
        if (indexVersionSupportsIndexOptions() == false) {
            return false;
        }

        Class<?> clazz = this.getClass();
        Class<InjectSparseVectorIndexOptions> injectSparseVectorIndexOptions = InjectSparseVectorIndexOptions.class;

        try {
            Method method = clazz.getMethod(this.getTestName());
            return method.isAnnotationPresent(injectSparseVectorIndexOptions);
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private boolean indexVersionSupportsIndexOptions() {
        if (indexVersionToTest.onOrAfter(IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT)) {
            return true;
        }

        if (indexVersionToTest.between(
            IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT_BACKPORT_8_X,
            IndexVersions.UPGRADE_TO_LUCENE_10_0_0
        )) {
            return true;
        }

        return false;
    }

    @Override
    protected void doAssertLuceneQuery(SparseVectorQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, instanceOf(SparseVectorQueryWrapper.class));
        var sparseQuery = (SparseVectorQueryWrapper) query;
        assertThat(sparseQuery.getTermsQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) sparseQuery.getTermsQuery();
        assertEquals(booleanQuery.getMinimumNumberShouldMatch(), 1);
        assertThat(booleanQuery.clauses(), hasSize(NUM_TOKENS));

        Class<?> featureQueryClass = FeatureField.newLinearQuery("", "", 0.5f).getClass();
        // if the weight is 1.0f a BoostQuery is returned
        Class<?> boostQueryClass = FeatureField.newLinearQuery("", "", 1.0f).getClass();

        for (var clause : booleanQuery.clauses()) {
            assertEquals(BooleanClause.Occur.SHOULD, clause.occur());
            assertThat(clause.query(), either(instanceOf(featureQueryClass)).or(instanceOf(boostQueryClass)));
        }
    }

    private void withSearchIndex(Consumer<SearchExecutionContext> consumer) throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            Document document = new Document();
            document.add(new FloatDocValuesField(SPARSE_VECTOR_FIELD, 1.0f));
            iw.addDocument(document);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                consumer.accept(context);
            }
        }
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testCacheability() throws IOException {
        withSearchIndex((context) -> {
            try {
                SparseVectorQueryBuilder queryBuilder = createTestQueryBuilder();
                QueryBuilder rewriteQuery = null;
                rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
                assertNotNull(rewriteQuery.toQuery(context));
                assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}; this query should always be rewritten
     */
    @Override
    public void testMustRewrite() throws IOException {
        withSearchIndex((context) -> {
            try {
                SparseVectorQueryBuilder queryBuilder = createTestQueryBuilder();
                queryBuilder.toQuery(context);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
    * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}; this query should always be rewritten
    */
    @Override
    public void testToQuery() throws IOException {
        withSearchIndex((context) -> {
            try {
                SparseVectorQueryBuilder queryBuilder = createTestQueryBuilder();
                if (queryBuilder.getQueryVectors() == null) {
                    QueryBuilder rewrittenQueryBuilder = rewriteAndFetch(queryBuilder, context);
                    assertTrue(rewrittenQueryBuilder instanceof SparseVectorQueryBuilder);
                    testDoToQuery((SparseVectorQueryBuilder) rewrittenQueryBuilder, context);
                } else {
                    testDoToQuery(queryBuilder, context);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void testDoToQuery(SparseVectorQueryBuilder queryBuilder, SearchExecutionContext context) throws IOException {
        Query query = queryBuilder.doToQuery(context);

        // test query builder can randomly have no vectors, which rewrites to a MatchNoneQuery - nothing more to do in this case.
        if (query instanceof MatchNoDocsQuery) {
            return;
        }

        assertTrue(query instanceof SparseVectorQueryWrapper);
        var sparseQuery = (SparseVectorQueryWrapper) query;

        // check if we have explicit pruning, or pruning via the index_options
        if (queryBuilder.shouldPruneTokens() || currentTestHasIndexOptions()) {
            // It's possible that all documents were pruned for aggressive pruning configurations
            assertTrue(sparseQuery.getTermsQuery() instanceof BooleanQuery || sparseQuery.getTermsQuery() instanceof MatchNoDocsQuery);
        } else {
            assertTrue(sparseQuery.getTermsQuery() instanceof BooleanQuery);
        }
    }

    public void testIllegalValues() {
        {
            // This will be caught and returned in the API as an IllegalArgumentException
            NullPointerException e = expectThrows(
                NullPointerException.class,
                () -> new SparseVectorQueryBuilder(null, "model text", "model id")
            );
            assertEquals("[sparse_vector] requires a [field]", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new SparseVectorQueryBuilder("field name", null, null)
            );
            assertEquals("[sparse_vector] requires one of [query_vector] or [inference_id] for sparse_vector fields", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new SparseVectorQueryBuilder("field name", "model text", null)
            );
            assertEquals("[sparse_vector] requires one of [query_vector] or [inference_id] for sparse_vector fields", e.getMessage());
        }
    }

    public void testToXContent() throws IOException {
        QueryBuilder query = new SparseVectorQueryBuilder("foo", "bar", "baz");
        checkGeneratedJson("""
            {
              "sparse_vector": {
                "field": "foo",
                "inference_id": "bar",
                "query": "baz",
                "prune": false
              }
            }""", query);
    }

    public void testToXContentWithThresholds() throws IOException {
        QueryBuilder query = new SparseVectorQueryBuilder("foo", null, "bar", "baz", true, new TokenPruningConfig(4, 0.3f, false));
        checkGeneratedJson("""
            {
              "sparse_vector": {
                "field": "foo",
                "inference_id": "bar",
                "query": "baz",
                "prune": true,
                "pruning_config": {
                  "tokens_freq_ratio_threshold": 4.0,
                  "tokens_weight_threshold": 0.3
                }
              }
            }""", query);
    }

    public void testToXContentWithThresholdsAndOnlyScorePrunedTokens() throws IOException {
        QueryBuilder query = new SparseVectorQueryBuilder("foo", null, "bar", "baz", true, new TokenPruningConfig(4, 0.3f, true));

        checkGeneratedJson("""
            {
              "sparse_vector": {
                "field": "foo",
                "inference_id": "bar",
                "query": "baz",
                "prune": true,
                "pruning_config": {
                  "tokens_freq_ratio_threshold": 4.0,
                  "tokens_weight_threshold": 0.3,
                  "only_score_pruned_tokens": true
                }
              }
            }""", query);
    }

    @Override
    protected String[] shuffleProtectedFields() {
        return new String[] { QUERY_VECTOR_FIELD.getPreferredName() };
    }

    public void testThatWeCorrectlyRewriteQueryIntoVectors() {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        TokenPruningConfig TokenPruningConfig = randomBoolean() ? new TokenPruningConfig(2, 0.3f, false) : null;

        SparseVectorQueryBuilder queryBuilder = createTestQueryBuilder(TokenPruningConfig);
        QueryBuilder rewrittenQueryBuilder = rewriteAndFetch(queryBuilder, searchExecutionContext);
        assertTrue(rewrittenQueryBuilder instanceof SparseVectorQueryBuilder);
        assertEquals(queryBuilder.shouldPruneTokens(), ((SparseVectorQueryBuilder) rewrittenQueryBuilder).shouldPruneTokens());
        assertNotNull(((SparseVectorQueryBuilder) rewrittenQueryBuilder).getQueryVectors());
    }

    @InjectSparseVectorIndexOptions
    public void testItUsesIndexOptionsDefaults() throws IOException {
        withSearchIndex((context) -> {
            try {
                SparseVectorQueryBuilder builder = createTestQueryBuilder(null);
                assertFalse(builder.shouldPruneTokens());
                testDoToQuery(builder, context);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @InjectSparseVectorIndexOptions
    public void testItOverridesIndexOptionsDefaults() throws IOException {
        withSearchIndex((context) -> {
            try {
                TokenPruningConfig pruningConfig = new TokenPruningConfig(2, 0.3f, false);
                SparseVectorQueryBuilder builder = createTestQueryBuilder(pruningConfig);
                assertTrue(builder.shouldPruneTokens());
                testDoToQuery(builder, context);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @InjectSparseVectorIndexOptions
    public void testToQueryRewriteWithIndexOptions() throws IOException {
        withSearchIndex((context) -> {
            SparseVectorQueryBuilder queryBuilder = createTestQueryBuilder(null);
            try {
                if (queryBuilder.getQueryVectors() == null) {
                    QueryBuilder rewrittenQueryBuilder = rewriteAndFetch(queryBuilder, context);
                    assertTrue(rewrittenQueryBuilder instanceof SparseVectorQueryBuilder);
                    testDoToQuery((SparseVectorQueryBuilder) rewrittenQueryBuilder, context);
                } else {
                    testDoToQuery(queryBuilder, context);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
