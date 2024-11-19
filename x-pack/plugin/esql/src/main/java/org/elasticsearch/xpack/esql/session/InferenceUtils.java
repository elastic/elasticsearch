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
import org.elasticsearch.core.Tuple;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceUtils {

    private InferenceUtils() {}

    public static List<Tuple<String, String>> semanticQueries(LogicalPlan analyzedPlan) {
        List<Tuple<String, String>> result = new ArrayList<>();

        analyzedPlan.forEachDown(plan -> {
            if (plan instanceof Filter) {
                ((Filter) plan).condition().forEachDown(expression -> {
                    if (expression instanceof Match && ((Match) expression).field().dataType() == DataType.SEMANTIC_TEXT) {
                        result.add(Tuple.tuple(((Match) expression).field().sourceText(), ((Match) expression).query().sourceText()));
                    }
                });
            }
        });

        return result;
    }

    public static void setInferenceResult(LogicalPlan analyzedPlan, String fieldName, String query, InferenceResults inferenceResults) {
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

    public static void setInferenceResults(
        LogicalPlan plan,
        Client client,
        InferenceContext inferenceContext,
        ActionListener<Result> listener,
        BiConsumer<LogicalPlan, ActionListener<Result>> callback
    ) {
        List<Tuple<String, String>> semanticQueries = semanticQueries(plan);

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
        for (Tuple<String, String> semanticQuery : semanticQueries) {
            InferenceAction.Request inferenceRequest = new InferenceAction.Request(
                TaskType.ANY,
                inferenceContext.semanticTextInferenceId(semanticQuery.v1()),
                null,
                List.of(semanticQuery.v2()),
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
                        semanticQuery.v1()
                    );
                    setInferenceResult(plan, semanticQuery.v1(), semanticQuery.v2(), inferenceResults);
                    next.onResponse(inferenceResponse);
                })
            );
        }
    }
}
