/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.utils.SemanticTextInferenceUtils;
import org.elasticsearch.xpack.esql.analysis.InferenceContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public final class InferenceUtils {

    private InferenceUtils() {}

    public static void setInferenceResults(
        LogicalPlan plan,
        Client client,
        InferenceContext inferenceContext,
        ActionListener<Result> listener,
        BiConsumer<LogicalPlan, ActionListener<Result>> callback
    ) {
        Set<SemanticQuery> semanticQueries = semanticQueries(plan);

        if (semanticQueries.isEmpty()) {
            callback.accept(plan, listener);
            return;
        }

        GroupedActionListener<InferenceAction.Response> actionListener = new GroupedActionListener<>(
            semanticQueries.size(),
            new ActionListener<Collection<InferenceAction.Response>>() {
                @Override
                public void onResponse(Collection<InferenceAction.Response> ignored) {
                    try {
                        callback.accept(plan, listener);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
        for (SemanticQuery semanticQuery : semanticQueries) {
            InferenceAction.Request inferenceRequest = new InferenceAction.Request(
                TaskType.ANY,
                inferenceContext.semanticTextInferenceId(semanticQuery.fieldName()),
                null,
                List.of(semanticQuery.queryString()),
                Map.of(),
                InputType.SEARCH,
                InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API,
                false
            );

            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                InferenceAction.INSTANCE,
                inferenceRequest,
                actionListener.delegateFailureAndWrap((next, inferenceResponse) -> {
                    InferenceResults inferenceResults = SemanticTextInferenceUtils.validateAndConvertInferenceResults(
                        inferenceResponse.getResults(),
                        semanticQuery.fieldName()
                    );
                    setInferenceResult(plan, semanticQuery.fieldName(), semanticQuery.queryString(), inferenceResults);
                    next.onResponse(inferenceResponse);
                })
            );
        }
    }

    private record SemanticQuery(String fieldName, String queryString) {};

    private static Set<SemanticQuery> semanticQueries(LogicalPlan analyzedPlan) {
        Set<SemanticQuery> result = new HashSet<>();

        analyzedPlan.forEachDown(plan -> {
            if (plan instanceof Filter) {
                ((Filter) plan).condition().forEachDown(expression -> {
                    if (expression instanceof Match && ((Match) expression).field().dataType() == DataType.SEMANTIC_TEXT) {
                        result.add(new SemanticQuery(((Match) expression).field().sourceText(), ((Match) expression).query().sourceText()));
                    }
                });
            }
        });

        return result;
    }

    private static void setInferenceResult(LogicalPlan analyzedPlan, String fieldName, String query, InferenceResults inferenceResults) {
        analyzedPlan.forEachDown(plan -> {
            if (plan instanceof Filter) {
                Filter filter = (Filter) plan;
                filter.condition().forEachDown(expression -> {
                    if (expression instanceof Match) {
                        Match match = (Match) expression;
                        if (match.field().sourceText().equals(fieldName) && match.query().sourceText().equals(query)) {
                            match.setInferenceResults(inferenceResults);
                        }
                    }
                });
            }
        });
    }
}
