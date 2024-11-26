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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.SemanticTextEsField;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceResolver {
    private final Client client;

    public InferenceResolver(Client client) {
        this.client = client;
    }

    public void setInferenceResults(
        LogicalPlan plan,
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
                semanticQuery.inferenceId(),
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

    private record SemanticQuery(String fieldName, String queryString, String inferenceId) {};

    private static Set<SemanticQuery> semanticQueries(LogicalPlan analyzedPlan) {
        Set<SemanticQuery> result = new HashSet<>();

        analyzedPlan.forEachExpressionDown(Match.class, match -> {
            if (match.field().dataType() == DataType.SEMANTIC_TEXT && match.field() instanceof FieldAttribute field) {
                SemanticTextEsField esField = (SemanticTextEsField) field.field();
                if (esField.inferenceIds().size() == 1) {
                    result.add(new SemanticQuery(field.sourceText(), match.query().sourceText(), esField.inferenceIds().iterator().next()));
                }
            }
        });

        return result;
    }

    private void setInferenceResult(LogicalPlan analyzedPlan, String fieldName, String query, InferenceResults inferenceResults) {
        analyzedPlan.forEachExpressionDown(Match.class, match -> {
            if (match.field().sourceText().equals(fieldName) && match.query().sourceText().equals(query)) {
                match.setInferenceResults(inferenceResults);
            }
        });
    }
}
