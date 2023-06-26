/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
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
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasSize;

public class TextExpansionQueryBuilderTests extends AbstractQueryTestCase<TextExpansionQueryBuilder> {

    private static final String RANK_FEATURES_FIELD = "rank";
    private static int NUM_TOKENS = 10;

    @Override
    protected TextExpansionQueryBuilder doCreateTestQueryBuilder() {
        var builder = new TextExpansionQueryBuilder(RANK_FEATURES_FIELD, randomAlphaOfLength(4), randomAlphaOfLength(4));
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
    public void testMustRewrite() {
        SearchExecutionContext context = createSearchExecutionContext();
        TextExpansionQueryBuilder builder = new TextExpansionQueryBuilder("foo", "bar", "baz");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.toQuery(context));
        assertEquals("text_expansion should have been rewritten to another query type", e.getMessage());
    }

    @Override
    protected boolean canSimulateMethod(Method method, Object[] args) throws NoSuchMethodException {
        return method.equals(Client.class.getMethod("execute", ActionType.class, ActionRequest.class, ActionListener.class))
            && (args[0] instanceof InferModelAction);
    }

    @Override
    protected Object simulateMethod(Method method, Object[] args) {
        InferModelAction.Request request = (InferModelAction.Request) args[1];

        // Randomisation cannot be used here as {@code #doAssertLuceneQuery}
        // asserts that 2 rewritten queries are the same
        var tokens = new ArrayList<TextExpansionResults.WeightedToken>();
        for (int i = 0; i < NUM_TOKENS; i++) {
            tokens.add(new TextExpansionResults.WeightedToken(Integer.toString(i), (i + 1) * 1.0f));
        }

        var response = InferModelAction.Response.builder()
            .setId(request.getId())
            .addInferenceResults(List.of(new TextExpansionResults("foo", tokens, randomBoolean())))
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

    @Override
    protected void doAssertLuceneQuery(TextExpansionQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
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
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new TextExpansionQueryBuilder(null, "model text", "model id")
            );
            assertEquals("[text_expansion] requires a fieldName", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new TextExpansionQueryBuilder("field name", null, "model id")
            );
            assertEquals("[text_expansion] requires a model_text value", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new TextExpansionQueryBuilder("field name", "model text", null)
            );
            assertEquals("[text_expansion] requires a model_id value", e.getMessage());
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
}
