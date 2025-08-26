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
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.search.WeightedTokensQueryBuilder.TOKENS_FIELD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasSize;

public class WeightedTokensQueryBuilderTests extends AbstractQueryTestCase<WeightedTokensQueryBuilder> {

    private static final String RANK_FEATURES_FIELD = "rank";
    private static final List<WeightedToken> WEIGHTED_TOKENS = List.of(new WeightedToken("foo", .42f));
    private static final int NUM_TOKENS = WEIGHTED_TOKENS.size();

    @Override
    protected WeightedTokensQueryBuilder doCreateTestQueryBuilder() {
        return createTestQueryBuilder(randomBoolean());
    }

    private WeightedTokensQueryBuilder createTestQueryBuilder(boolean onlyScorePrunedTokens) {
        TokenPruningConfig tokenPruningConfig = randomBoolean()
            ? new TokenPruningConfig(randomIntBetween(1, 100), randomFloat(), onlyScorePrunedTokens)
            : null;

        var builder = new WeightedTokensQueryBuilder(RANK_FEATURES_FIELD, WEIGHTED_TOKENS, tokenPruningConfig);
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
        return List.of(XPackClientPlugin.class, MapperExtrasPlugin.class);
    }

    @Override
    protected boolean canSimulateMethod(Method method, Object[] args) throws NoSuchMethodException {
        return method.equals(Client.class.getMethod("execute", ActionType.class, ActionRequest.class, ActionListener.class))
            && (args[0] instanceof InferModelAction);
    }

    @Override
    protected Object simulateMethod(Method method, Object[] args) {
        InferModelAction.Request request = (InferModelAction.Request) args[1];
        assertEquals(InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API, request.getInferenceTimeout());
        assertEquals(TrainedModelPrefixStrings.PrefixType.SEARCH, request.getPrefixType());

        // Randomisation of tokens cannot be used here as {@code #doAssertLuceneQuery}
        // asserts that 2 rewritten queries are the same
        var response = InferModelAction.Response.builder()
            .setId(request.getId())
            .addInferenceResults(List.of(new TextExpansionResults("foo", WEIGHTED_TOKENS.stream().toList(), randomBoolean())))
            .build();
        @SuppressWarnings("unchecked")  // We matched the method above.
        ActionListener<InferModelAction.Response> listener = (ActionListener<InferModelAction.Response>) args[2];
        listener.onResponse(response);
        return null;
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(RANK_FEATURES_FIELD, "type=rank_features"))),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testToQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            // Index at least one document so we have a freq > 0
            Document document = new Document();
            document.add(new FeatureField(RANK_FEATURES_FIELD, "foo", 1.0f));
            iw.addDocument(document);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                // We need to force token pruning config here, to get repeatable lucene queries for comparison
                WeightedTokensQueryBuilder firstQuery = createTestQueryBuilder(false);
                WeightedTokensQueryBuilder controlQuery = copyQuery(firstQuery);
                QueryBuilder rewritten = rewriteQuery(firstQuery, context);
                Query firstLuceneQuery = rewritten.toQuery(context);
                assertNotNull("toQuery should not return null", firstLuceneQuery);
                assertLuceneQuery(firstQuery, firstLuceneQuery, context);
                assertEquals(
                    "query is not equal to its copy after calling toQuery, firstQuery: " + firstQuery + ", secondQuery: " + controlQuery,
                    firstQuery,
                    controlQuery
                );
                assertEquals(
                    "equals is not symmetric after calling toQuery, firstQuery: " + firstQuery + ", secondQuery: " + controlQuery,
                    controlQuery,
                    firstQuery
                );
                assertThat(
                    "query copy's hashcode is different from original hashcode after calling toQuery, firstQuery: "
                        + firstQuery
                        + ", secondQuery: "
                        + controlQuery,
                    controlQuery.hashCode(),
                    equalTo(firstQuery.hashCode())
                );
                WeightedTokensQueryBuilder secondQuery = copyQuery(firstQuery);

                // query _name never should affect the result of toQuery, we randomly set it to make sure
                if (randomBoolean()) {
                    secondQuery.queryName(
                        secondQuery.queryName() == null
                            ? randomAlphaOfLengthBetween(1, 30)
                            : secondQuery.queryName() + randomAlphaOfLengthBetween(1, 10)
                    );
                }
                context = new SearchExecutionContext(context);
                Query secondLuceneQuery = rewriteQuery(secondQuery, context).toQuery(context);
                assertNotNull("toQuery should not return null", secondLuceneQuery);
                assertLuceneQuery(secondQuery, secondLuceneQuery, context);

                if (builderGeneratesCacheableQueries()) {
                    assertEquals(
                        "two equivalent query builders lead to different lucene queries hashcode",
                        secondLuceneQuery.hashCode(),
                        firstLuceneQuery.hashCode()
                    );
                    assertEquals(
                        "two equivalent query builders lead to different lucene queries",
                        rewrite(secondLuceneQuery),
                        rewrite(firstLuceneQuery)
                    );
                }

                if (supportsBoost() && firstLuceneQuery instanceof MatchNoDocsQuery == false) {
                    secondQuery.boost(firstQuery.boost() + 1f + randomFloat());
                    Query thirdLuceneQuery = rewriteQuery(secondQuery, context).toQuery(context);
                    assertNotEquals(
                        "modifying the boost doesn't affect the corresponding lucene query",
                        rewrite(firstLuceneQuery),
                        rewrite(thirdLuceneQuery)
                    );
                }

            }
        }
    }

    @Override
    public void testFromXContent() throws IOException {
        super.testFromXContent();
        assertCriticalWarnings(WeightedTokensQueryBuilder.WEIGHTED_TOKENS_DEPRECATION_MESSAGE);
    }

    @Override
    public void testUnknownField() throws IOException {
        super.testUnknownField();
        assertCriticalWarnings(WeightedTokensQueryBuilder.WEIGHTED_TOKENS_DEPRECATION_MESSAGE);
    }

    @Override
    public void testUnknownObjectException() throws IOException {
        super.testUnknownObjectException();
        assertCriticalWarnings(WeightedTokensQueryBuilder.WEIGHTED_TOKENS_DEPRECATION_MESSAGE);
    }

    @Override
    public void testValidOutput() throws IOException {
        super.testValidOutput();
        assertCriticalWarnings(WeightedTokensQueryBuilder.WEIGHTED_TOKENS_DEPRECATION_MESSAGE);
    }

    public void testPruningIsAppliedCorrectly() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            List<Document> documents = List.of(
                createDocument(
                    List.of("the", "quick", "brown", "fox", "jumped", "over", "lazy", "dog", "me"),
                    List.of(.2f, 1.8f, 1.75f, 5.9f, 1.6f, 1.4f, .4f, 4.8f, 2.1f)
                ),
                createDocument(
                    List.of("the", "rains", "in", "spain", "fall", "mainly", "on", "plain", "me"),
                    List.of(.1f, 3.6f, .1f, 4.8f, .6f, .3f, .1f, 2.6f, 2.1f)
                ),
                createDocument(
                    List.of("betty", "bought", "butter", "but", "the", "was", "bitter", "me"),
                    List.of(6.8f, 1.4f, .5f, 3.2f, .1f, 3.2f, .6f, 2.1f)
                ),
                createDocument(
                    List.of("she", "sells", "seashells", "by", "the", "seashore", "me"),
                    List.of(.2f, 1.4f, 5.9f, .1f, .1f, 3.6f, 2.1f)
                )
            );
            iw.addDocuments(documents);

            List<WeightedToken> inputTokens = List.of(
                new WeightedToken("the", .1f),      // Will be pruned - score too low, freq too high
                new WeightedToken("black", 5.3f),   // Will be pruned - does not exist in index
                new WeightedToken("dog", 7.5f),     // Will be kept - high score and low freq
                new WeightedToken("jumped", 4.5f),  // Will be kept - high score and low freq
                new WeightedToken("on", .1f),       // Will be kept - low score but also low freq
                new WeightedToken("me", 3.8f)       // Will be kept - high freq but also high score
            );
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));

                WeightedTokensQueryBuilder noPruningQuery = new WeightedTokensQueryBuilder(RANK_FEATURES_FIELD, inputTokens, null);
                Query query = noPruningQuery.doToQuery(context);
                assertCorrectLuceneQuery("noPruningQuery", query, List.of("the", "black", "dog", "jumped", "on", "me"));

                WeightedTokensQueryBuilder queryThatShouldBePruned = new WeightedTokensQueryBuilder(
                    RANK_FEATURES_FIELD,
                    inputTokens,
                    new TokenPruningConfig(2, 0.5f, false)
                );
                query = queryThatShouldBePruned.doToQuery(context);
                assertCorrectLuceneQuery("queryThatShouldBePruned", query, List.of("dog", "jumped", "on", "me"));

                WeightedTokensQueryBuilder onlyScorePrunedTokensQuery = new WeightedTokensQueryBuilder(
                    RANK_FEATURES_FIELD,
                    inputTokens,
                    new TokenPruningConfig(2, 0.5f, true)
                );
                query = onlyScorePrunedTokensQuery.doToQuery(context);
                assertCorrectLuceneQuery("onlyScorePrunedTokensQuery", query, List.of("the", "black"));
            }
        }
    }

    private void assertCorrectLuceneQuery(String name, Query query, List<String> expectedFeatureFields) {
        assertThat(query, instanceOf(SparseVectorQueryWrapper.class));
        var sparseQuery = (SparseVectorQueryWrapper) query;
        assertThat(sparseQuery.getTermsQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) sparseQuery.getTermsQuery();
        List<BooleanClause> booleanClauses = booleanQuery.clauses();
        assertEquals(
            name + " had " + booleanClauses.size() + " clauses, expected " + expectedFeatureFields.size(),
            expectedFeatureFields.size(),
            booleanClauses.size()
        );
        for (int i = 0; i < booleanClauses.size(); i++) {
            Query clauseQuery = booleanClauses.get(i).getQuery();
            assertTrue(name + " query " + query + " expected to be a BoostQuery", clauseQuery instanceof BoostQuery);
            // FeatureQuery is not visible so we check the String representation
            assertTrue(name + " query " + query + " expected to be a FeatureQuery", clauseQuery.toString().contains("FeatureQuery"));
            assertTrue(
                name + " query " + query + " expected to have field " + expectedFeatureFields.get(i),
                clauseQuery.toString().contains("feature=" + expectedFeatureFields.get(i))
            );
        }
    }

    private Document createDocument(List<String> tokens, List<Float> weights) {
        if (tokens.size() != weights.size()) {
            throw new IllegalArgumentException(
                "tokens and weights must have the same size. Got " + tokens.size() + " and " + weights.size() + "."
            );
        }
        Document document = new Document();
        for (int i = 0; i < tokens.size(); i++) {
            document.add(new FeatureField(RANK_FEATURES_FIELD, tokens.get(i), weights.get(i)));
        }
        return document;
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testCacheability() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            Document document = new Document();
            document.add(new FloatDocValuesField(RANK_FEATURES_FIELD, 1.0f));
            iw.addDocument(document);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                WeightedTokensQueryBuilder queryBuilder = createTestQueryBuilder();
                QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));

                assertNotNull(rewriteQuery.toQuery(context));
                assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());
            }
        }
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testMustRewrite() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            Document document = new Document();
            document.add(new FloatDocValuesField(RANK_FEATURES_FIELD, 1.0f));
            iw.addDocument(document);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                context.setAllowUnmappedFields(true);
                WeightedTokensQueryBuilder queryBuilder = createTestQueryBuilder();
                queryBuilder.toQuery(context);
            }
        }
    }

    @Override
    protected void doAssertLuceneQuery(WeightedTokensQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
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
            assertEquals(BooleanClause.Occur.SHOULD, clause.getOccur());
            assertThat(clause.getQuery(), either(instanceOf(featureQueryClass)).or(instanceOf(boostQueryClass)));
        }
    }

    public void testIllegalValues() {
        List<WeightedToken> weightedTokens = List.of(new WeightedToken("foo", 1.0f));
        {
            NullPointerException e = expectThrows(
                NullPointerException.class,
                () -> new WeightedTokensQueryBuilder(null, weightedTokens, null)
            );
            assertEquals("[weighted_tokens] requires a fieldName", e.getMessage());
        }
        {
            NullPointerException e = expectThrows(
                NullPointerException.class,
                () -> new WeightedTokensQueryBuilder("field name", null, null)
            );
            assertEquals("[weighted_tokens] requires tokens", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new WeightedTokensQueryBuilder("field name", List.of(), null)
            );
            assertEquals("[weighted_tokens] requires at least one token", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new WeightedTokensQueryBuilder("field name", weightedTokens, new TokenPruningConfig(-1, 0.0f, false))
            );
            assertEquals("[tokens_freq_ratio_threshold] must be between [1] and [100], got -1.0", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new WeightedTokensQueryBuilder("field name", weightedTokens, new TokenPruningConfig(101, 0.0f, false))
            );
            assertEquals("[tokens_freq_ratio_threshold] must be between [1] and [100], got 101.0", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new WeightedTokensQueryBuilder("field name", weightedTokens, new TokenPruningConfig(5, 5f, false))
            );
            assertEquals("[tokens_weight_threshold] must be between 0 and 1", e.getMessage());
        }
    }

    public void testToXContent() throws Exception {
        QueryBuilder query = new WeightedTokensQueryBuilder("foo", WEIGHTED_TOKENS, null);
        checkGeneratedJson("""
            {
              "weighted_tokens": {
                "foo": {
                  "tokens": {
                      "foo": 0.42
                    }
                }
              }
            }""", query);
    }

    public void testToXContentWithThresholds() throws Exception {
        QueryBuilder query = new WeightedTokensQueryBuilder("foo", WEIGHTED_TOKENS, new TokenPruningConfig(4, 0.4f, false));
        checkGeneratedJson("""
            {
              "weighted_tokens": {
                "foo": {
                  "tokens": {
                      "foo": 0.42
                  },
                  "pruning_config": {
                    "tokens_freq_ratio_threshold": 4.0,
                    "tokens_weight_threshold": 0.4
                    }
                 }
              }
            }""", query);
    }

    public void testToXContentWithThresholdsAndOnlyScorePrunedTokens() throws Exception {
        QueryBuilder query = new WeightedTokensQueryBuilder("foo", WEIGHTED_TOKENS, new TokenPruningConfig(4, 0.4f, true));
        checkGeneratedJson("""
            {
              "weighted_tokens": {
                "foo": {
                  "tokens": {
                      "foo": 0.42
                  },
                  "pruning_config": {
                    "tokens_freq_ratio_threshold": 4.0,
                    "tokens_weight_threshold": 0.4,
                    "only_score_pruned_tokens": true
                    }
                 }
              }
            }""", query);
    }

    @Override
    protected String[] shuffleProtectedFields() {
        return new String[] { TOKENS_FIELD.getPreferredName() };
    }
}
