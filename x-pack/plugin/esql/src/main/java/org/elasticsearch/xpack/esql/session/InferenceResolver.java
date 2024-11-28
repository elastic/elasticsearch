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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.utils.SemanticTextInferenceUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.SemanticQuery;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceResolver {
    private final Client client;
    private final ClusterService clusterService;

    public InferenceResolver(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public void setInferenceResults(
        LogicalPlan plan,
        EsqlExecutionInfo executionInfo,
        ActionListener<Result> listener,
        BiConsumer<LogicalPlan, ActionListener<Result>> callback
    ) {
        if (EsqlCapabilities.Cap.SEMANTIC_TEXT_TYPE.isEnabled() == false) {
            callback.accept(plan, listener);
            return;
        }

        Set<Failure> failures = new LinkedHashSet<>();
        Set<SemanticQuery> semanticQueries = semanticQueries(plan, failures, executionInfo.isCrossClusterSearch());

        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }

        if (semanticQueries.isEmpty()) {
            callback.accept(plan, listener);
            return;
        }

        ConcurrentMap<SemanticQuery, InferenceResults> inferenceResultsMap = new ConcurrentHashMap<>();

        GroupedActionListener<InferenceAction.Response> actionListener = new GroupedActionListener<>(
            semanticQueries.size(),
            new ActionListener<Collection<InferenceAction.Response>>() {
                @Override
                public void onResponse(Collection<InferenceAction.Response> ignored) {
                    try {
                        LogicalPlan newPlan = updatedPlan(plan, inferenceResultsMap);
                        newPlan.setAnalyzed();
                        callback.accept(newPlan, listener);
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
                    inferenceResultsMap.put(semanticQuery, inferenceResults);
                    next.onResponse(inferenceResponse);
                })
            );
        }
    }

    private record SemanticQuery(String fieldName, String queryString, String inferenceId) {};

    private Set<SemanticQuery> semanticQueries(LogicalPlan analyzedPlan, Set<Failure> failures, boolean isCrossClusterSearch) {
        Set<SemanticQuery> result = new HashSet<>();

        Set<String> indexNames = indexNames(analyzedPlan);

        analyzedPlan.forEachExpressionDown(Match.class, match -> {
            if (match.field().dataType() == DataType.SEMANTIC_TEXT && match.field() instanceof FieldAttribute field) {
                if (isCrossClusterSearch) {
                    failures.add(
                        Failure.fail(
                            match.field(),
                            "[{}] {} does not allow semantic_text fields with cross cluster queries.",
                            match.functionName(),
                            match.functionType()
                        )
                    );
                }

                EsField esField = field.field();
                Set<String> inferenceIds = inferenceIdsForField(esField.getName(), indexNames);
                if (inferenceIds.size() == 1) {
                    result.add(new SemanticQuery(field.sourceText(), match.query().sourceText(), inferenceIds.iterator().next()));
                } else {
                    assert inferenceIds.size() == 0 : "Should never have a semantic_text field with no inference ID attached";

                    failures.add(
                        Failure.fail(
                            match.field(),
                            "[{}] {} cannot operate on [{}] because it is configured with multiple inference IDs.",
                            match.functionName(),
                            match.functionType(),
                            match.field().sourceText()
                        )
                    );
                }
            }
        });

        return result;
    }

    private LogicalPlan updatedPlan(LogicalPlan plan, Map<SemanticQuery, InferenceResults> inferenceResultsMap) {
        return plan.transformExpressionsDown(Match.class, match -> {
            for (SemanticQuery semanticQuery : inferenceResultsMap.keySet()) {
                if (match.field().sourceText().equals(semanticQuery.fieldName())
                    && match.query().sourceText().equals(semanticQuery.queryString())) {
                    return Match.newWithInferenceResults(match, inferenceResultsMap.get(semanticQuery));
                }
            }
            return match;
        });
    }

    public Set<String> inferenceIdsForField(String name, Set<String> indexNames) {
        Set<String> inferenceIds = new HashSet<>();
        Map<String, IndexMetadata> indexMetadata = clusterService.state().getMetadata().getIndices();

        for (String indexName : indexNames) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.get(indexName).getInferenceFields().get(name);
            if (inferenceFieldMetadata != null) {
                inferenceIds.add(inferenceFieldMetadata.getInferenceId());
            }
        }
        return inferenceIds;
    }

    public Set<String> indexNames(LogicalPlan analyzedPlan) {
        AtomicReference<Set<String>> indexNames = new AtomicReference<>();
        analyzedPlan.forEachDown(EsRelation.class, esRelation -> { indexNames.set(esRelation.index().concreteIndices()); });

        return indexNames.get();
    }
}
