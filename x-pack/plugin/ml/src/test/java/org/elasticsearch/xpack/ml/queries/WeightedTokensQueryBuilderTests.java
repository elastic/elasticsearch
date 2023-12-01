/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults.WeightedToken;
import static org.elasticsearch.xpack.ml.queries.WeightedTokensQueryBuilder.TOKENS_FIELD;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasSize;

public class WeightedTokensQueryBuilderTests extends AbstractQueryTestCase<WeightedTokensQueryBuilder> {

    private static final String RANK_FEATURES_FIELD = "rank";

    private static final int NUM_TOKENS = 10;

    private static final Set<WeightedToken> WEIGHTED_TOKENS = Set.of(
        new TextExpansionResults.WeightedToken("foo", .42f),
        new TextExpansionResults.WeightedToken("bar", .05f),
        new TextExpansionResults.WeightedToken("baz", .74f)
    );

    @Override
    protected WeightedTokensQueryBuilder doCreateTestQueryBuilder() {
        WeightedTokensThreshold threshold = randomBoolean()
            ? new WeightedTokensThreshold(randomIntBetween(1, 100), randomFloat(), randomBoolean())
            : null;

        var builder = new WeightedTokensQueryBuilder(RANK_FEATURES_FIELD, WEIGHTED_TOKENS, threshold);
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
        return List.of(MachineLearning.class, MapperExtrasPlugin.class);
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
            Document document = new Document();
            document.add(new FloatDocValuesField(RANK_FEATURES_FIELD, 1.0f));
            iw.addDocument(document);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                WeightedTokensQueryBuilder queryBuilder = createTestQueryBuilder();
                Query query = queryBuilder.doToQuery(context);

                assertEquals(RANK_FEATURES_FIELD, queryBuilder.getFieldName());
            }
        }
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
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
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
        Set<WeightedToken> weightedTokens = Set.of(new WeightedToken("foo", 1.0f));
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
                () -> new WeightedTokensQueryBuilder("field name", Set.of(), null)
            );
            assertEquals("[weighted_tokens] requires at least one token", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new WeightedTokensQueryBuilder("field name", weightedTokens, new WeightedTokensThreshold(-1f, 0.0f, false))
            );
            assertEquals("[ratio_threshold] must be greater or equal to 1, got -1.0", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new WeightedTokensQueryBuilder("field name", weightedTokens, new WeightedTokensThreshold(5f, 5f, false))
            );
            assertEquals("[weight_threshold] must be between 0 and 1", e.getMessage());
        }
    }

    public void testToXContent() throws IOException {
        QueryBuilder query = new TextExpansionQueryBuilder("foo", "bar", "baz");
        checkGeneratedJson("""
            {
              "text_expansion": {
                "foo": {
                  "model_text": "bar",
                  "model_id": "baz"
                }
              }
            }""", query);
    }

    public void testToXContentWithThresholds() throws IOException {
        QueryBuilder query = new TextExpansionQueryBuilder("foo", "bar", "baz", new WeightedTokensThreshold(4, 0.4f, false));
        checkGeneratedJson("""
            {
              "text_expansion": {
                "foo": {
                  "model_text": "bar",
                  "model_id": "baz",
                  "tokens_threshold": {
                    "ratio_threshold": 4.0,
                    "weight_threshold": 0.4
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
