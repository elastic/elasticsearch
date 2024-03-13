/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
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
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

// TODO: Add dense vector tests

public class SemanticQueryBuilderTests extends AbstractQueryTestCase<SemanticQueryBuilder> {

    private static final String SEMANTIC_TEXT_SPARSE_FIELD = "semantic_sparse";

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
        SemanticQueryBuilder builder = new SemanticQueryBuilder(SEMANTIC_TEXT_SPARSE_FIELD, randomAlphaOfLength(4));
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
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(ESToParentBlockJoinQuery.class));

        // TODO: Extend assertion
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
            .map(s -> new TextExpansionResults.WeightedToken(s, randomFloat()))
            .toList();
        TextExpansionResults textExpansionResults = new TextExpansionResults(DEFAULT_RESULTS_FIELD, weightedTokens, false);
        InferenceAction.Response response = new InferenceAction.Response(SparseEmbeddingResults.of(List.of(textExpansionResults)));

        @SuppressWarnings("unchecked")  // We matched the method above.
        ActionListener<InferenceAction.Response> listener = (ActionListener<InferenceAction.Response>) args[2];
        listener.onResponse(response);

        return null;
    }
}
