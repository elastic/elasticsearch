/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.SlimResults;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TextExpansionQueryBuilderTests extends AbstractQueryTestCase<TextExpansionQueryBuilder> {

    @Override
    protected TextExpansionQueryBuilder doCreateTestQueryBuilder() {
        var builder = new TextExpansionQueryBuilder(
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(4)
        );
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
        return List.of(MachineLearning.class);
    }

    @Override
    public void testMustRewrite() {
        SearchExecutionContext context = createSearchExecutionContext();
        TextExpansionQueryBuilder builder = new TextExpansionQueryBuilder("foo", "bar", "baz");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.toQuery(context));
        assertEquals("terms query must not be null, missing rewrite?", e.getMessage());
    }

    @Override
    protected boolean canSimulateMethod(Method method, Object[] args) throws NoSuchMethodException {
        return method.equals(Client.class.getMethod("execute", ActionType.class, ActionRequest.class, ActionListener.class))
            && (args[0] instanceof InferModelAction);
    }

    @Override
    protected Object simulateRequest(Method method, Object[] args) {
        InferModelAction.Request request = (InferModelAction.Request) args[1];

        var tokens = new ArrayList<SlimResults.WeightedToken>();
        int numTokens = randomIntBetween(1, 20);
        for (int i = 0; i < numTokens; i++) {
            tokens.add(new SlimResults.WeightedToken(i, randomFloat()));
        }

        var response = InferModelAction.Response.builder()
            .setModelId(request.getModelId())
            .addInferenceResults(List.of(new SlimResults("foo", tokens, randomBoolean())))
            .build();
        @SuppressWarnings("unchecked")  // We matched the method above.
        ActionListener<InferModelAction.Response> listener = (ActionListener<InferModelAction.Response>) args[2];
        listener.onResponse(response);
        return null;
    }

    @Override
    protected void doAssertLuceneQuery(TextExpansionQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        fail("what should be asserted here?");
    }

    public void testIllegalValues() {
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new TextExpansionQueryBuilder(null, "model text", null)
            );
            assertEquals("[text_expansion] requires a fieldName", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new TextExpansionQueryBuilder("field name", null, null)
            );
            assertEquals("[text_expansion] requires a model_text value", e.getMessage());
        }
    }

    public void testToXContentWithDefaults() throws IOException {
        QueryBuilder query = new TextExpansionQueryBuilder("foo", "bar", null);
        checkGeneratedJson("""
            {
              "text_expansion": {
                "foo": {
                  "model_text": "bar",
                  "model_id": "slim"
                }
              }
            }""", query);
    }
}
