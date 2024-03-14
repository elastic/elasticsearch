/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.INFERENCE_CHUNKS_RESULTS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

// TODO: Add dense vector tests

public class SemanticQueryBuilderTests extends AbstractQueryTestCase<SemanticQueryBuilder> {
    private static final String SEMANTIC_TEXT_SPARSE_FIELD = "semantic_sparse";
    private static final float TOKEN_WEIGHT = 0.5f;
    private static final int QUERY_TOKEN_LENGTH = 4;

    private Integer queryTokenCount;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        queryTokenCount = null;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InferencePlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                Strings.toString(PutMappingRequest.simpleMapping(SEMANTIC_TEXT_SPARSE_FIELD, "type=semantic_text,model_id=test_service"))
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected SemanticQueryBuilder doCreateTestQueryBuilder() {
        queryTokenCount = randomIntBetween(1, 5);
        List<String> queryTokens = new ArrayList<>(queryTokenCount);
        for (int i = 0; i < queryTokenCount; i++) {
            queryTokens.add(randomAlphaOfLength(QUERY_TOKEN_LENGTH));
        }

        SemanticQueryBuilder builder = new SemanticQueryBuilder(SEMANTIC_TEXT_SPARSE_FIELD, String.join(" ", queryTokens));
        if (randomBoolean()) {
            builder.boost((float) randomDoubleBetween(0.1, 10.0, true));
        }
        if (randomBoolean()) {
            builder.queryName(randomAlphaOfLength(4));
        }

        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(SemanticQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(queryTokenCount, notNullValue());
        assertThat(query, notNullValue());
        assertThat(query, either(instanceOf(ESToParentBlockJoinQuery.class)).or(instanceOf(BoostQuery.class)));

        if (query instanceof BoostQuery boostQuery) {
            assertThat(boostQuery.getBoost(), equalTo(queryBuilder.boost()));
            assertThat(boostQuery.getQuery(), instanceOf(ESToParentBlockJoinQuery.class));
            query = boostQuery.getQuery();
        }

        ESToParentBlockJoinQuery nestedQuery = (ESToParentBlockJoinQuery) query;
        assertThat(nestedQuery.getPath(), equalTo(SEMANTIC_TEXT_SPARSE_FIELD));
        assertThat(nestedQuery.getScoreMode(), equalTo(ScoreMode.Total));
        assertThat(nestedQuery.getChildQuery(), instanceOf(BooleanQuery.class));

        assertSparseVectorBooleanQuery((BooleanQuery) nestedQuery.getChildQuery());
    }

    private void assertSparseVectorBooleanQuery(BooleanQuery booleanQuery) {
        assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(1));
        assertThat(booleanQuery.clauses().size(), equalTo(queryTokenCount));

        for (BooleanClause booleanClause : booleanQuery.clauses()) {
            assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
            assertThat(booleanClause.getQuery(), instanceOf(BoostQuery.class));

            BoostQuery boostQuery = (BoostQuery) booleanClause.getQuery();
            assertThat(boostQuery.getBoost(), equalTo(TOKEN_WEIGHT));
            assertThat(boostQuery.getQuery(), instanceOf(TermQuery.class));

            TermQuery termQuery = (TermQuery) boostQuery.getQuery();
            assertThat(termQuery.getTerm().field(), equalTo(SEMANTIC_TEXT_SPARSE_FIELD + "." + INFERENCE_CHUNKS_RESULTS));
            assertThat(termQuery.getTerm().text().length(), equalTo(QUERY_TOKEN_LENGTH));
        }
    }

    @Override
    protected boolean canSimulateMethod(Method method, Object[] args) throws NoSuchMethodException {
        return method.equals(Client.class.getMethod("execute", ActionType.class, ActionRequest.class, ActionListener.class))
            && (args[0] instanceof InferenceAction);
    }

    @Override
    protected Object simulateMethod(Method method, Object[] args) {
        InferenceAction.Request request = (InferenceAction.Request) args[1];
        assertThat(request.getTaskType(), equalTo(TaskType.ANY));
        assertThat(request.getInputType(), equalTo(InputType.SEARCH));

        List<String> input = request.getInput();
        assertThat(input.size(), equalTo(1));

        List<TextExpansionResults.WeightedToken> weightedTokens = Arrays.stream(input.get(0).split("\\s+"))
            .map(s -> new TextExpansionResults.WeightedToken(s, TOKEN_WEIGHT))
            .toList();
        TextExpansionResults textExpansionResults = new TextExpansionResults(DEFAULT_RESULTS_FIELD, weightedTokens, false);
        InferenceAction.Response response = new InferenceAction.Response(SparseEmbeddingResults.of(List.of(textExpansionResults)));

        @SuppressWarnings("unchecked")  // We matched the method above.
        ActionListener<InferenceAction.Response> listener = (ActionListener<InferenceAction.Response>) args[2];
        listener.onResponse(response);

        return null;
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        SemanticQueryBuilder builder = new SemanticQueryBuilder("foo", "bar");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.toQuery(context));
        assertThat(e.getMessage(), equalTo("Query builder must be rewritten first"));
    }

    public void testIllegalValues() {
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SemanticQueryBuilder(null, "query"));
            assertThat(e.getMessage(), equalTo("[semantic_query] requires a fieldName"));
        }
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SemanticQueryBuilder("fieldName", null));
            assertThat(e.getMessage(), equalTo("[semantic_query] requires a query value"));
        }
    }

    public void testToXContent() throws IOException {
        QueryBuilder queryBuilder = new SemanticQueryBuilder("foo", "bar");
        checkGeneratedJson("""
            {
              "semantic_query": {
                "foo": {
                  "query": "bar"
                }
              }
            }""", queryBuilder);
    }
}
