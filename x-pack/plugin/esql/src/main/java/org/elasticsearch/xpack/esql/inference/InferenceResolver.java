/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Collects and resolves inference deployments inference IDs from ES|QL logical plans.
 */
public class InferenceResolver {

    private final Client client;
    private final EsqlFunctionRegistry functionRegistry;
    private final ThreadPool threadPool;

    /**
     * Constructs a new {@code InferenceResolver}.
     *
     * @param client The Elasticsearch client for executing inference deployment lookups
     */
    public InferenceResolver(Client client, EsqlFunctionRegistry functionRegistry, ThreadPool threadPool) {
        this.client = client;
        this.functionRegistry = functionRegistry;
        this.threadPool = threadPool;
    }

    /**
     * Resolves inference IDs from the given logical plan.
     * <p>
     * This method traverses the logical plan tree and identifies all inference operations,
     * extracting their deployment IDs for subsequent validation. Currently, supports:
     * <ul>
     *   <li>{@link InferencePlan} objects (Completion, etc.)</li>
     * </ul>
     *
     * @param plan     The logical plan to scan for inference operations
     * @param listener Callback to receive the resolution results
     */
    public void resolveInferenceIds(LogicalPlan plan, ActionListener<InferenceResolution> listener) {
        resolveInferenceIds(collectInferenceIds(plan), listener);
    }

    /**
     * Collects all inference IDs from the given logical plan.
     * <p>
     * This method traverses the logical plan tree and identifies all inference operations,
     * extracting their deployment IDs for subsequent validation. Currently, supports:
     * <ul>
     *   <li>{@link InferencePlan} objects (Completion, etc.)</li>
     *   <li>{@link InferenceFunction} objects (TextEmbedding, etc.)</li>
     * </ul>
     *
     * @param plan The logical plan to scan for inference operations
     */
    List<String> collectInferenceIds(LogicalPlan plan) {
        List<String> inferenceIds = new ArrayList<>();
        collectInferenceIdsFromInferencePlans(plan, inferenceIds::add);
        collectInferenceIdsFromInferenceFunctions(plan, inferenceIds::add);

        return inferenceIds;
    }

    /**
     * Resolves a list of inference deployment IDs to their metadata.
     * <p>
     * For each inference ID, this method:
     * <ol>
     *   <li>Queries the inference service to verify the deployment exists</li>
     *   <li>Retrieves the deployment's task type and configuration</li>
     *   <li>Builds an {@link InferenceResolution} containing resolved metadata or errors</li>
     * </ol>
     *
     * @param inferenceIds List of inference deployment IDs to resolve
     * @param listener     Callback to receive the resolution results
     */
    void resolveInferenceIds(List<String> inferenceIds, ActionListener<InferenceResolution> listener) {
        resolveInferenceIds(Set.copyOf(inferenceIds), listener);
    }

    void resolveInferenceIds(Set<String> inferenceIds, ActionListener<InferenceResolution> listener) {

        if (inferenceIds.isEmpty()) {
            listener.onResponse(InferenceResolution.EMPTY);
            return;
        }

        final InferenceResolution.Builder inferenceResolutionBuilder = InferenceResolution.builder();

        final CountDownActionListener countdownListener = new CountDownActionListener(
            inferenceIds.size(),
            listener.delegateFailureIgnoreResponseAndWrap(l -> l.onResponse(inferenceResolutionBuilder.build()))
        );

        for (var inferenceId : inferenceIds) {
            client.execute(
                GetInferenceModelAction.INSTANCE,
                new GetInferenceModelAction.Request(inferenceId, TaskType.ANY),
                new ThreadedActionListener<>(threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION), ActionListener.wrap(r -> {
                    ResolvedInference resolvedInference = new ResolvedInference(inferenceId, r.getEndpoints().getFirst().getTaskType());
                    inferenceResolutionBuilder.withResolvedInference(resolvedInference);
                    countdownListener.onResponse(null);
                }, e -> {
                    inferenceResolutionBuilder.withError(inferenceId, e.getMessage());
                    countdownListener.onResponse(null);
                }))
            );
        }
    }

    /**
     * Collects inference IDs from InferencePlan objects within the logical plan.
     *
     * @param plan The logical plan to scan for InferencePlan objects
     * @param c    Consumer function to receive each discovered inference ID
     */
    private void collectInferenceIdsFromInferencePlans(LogicalPlan plan, Consumer<String> c) {
        plan.forEachUp(InferencePlan.class, inferencePlan -> c.accept(inferenceId(inferencePlan)));
    }

    /**
     * Collects inference IDs from function expressions within the logical plan.
     *
     * @param plan The logical plan to scan for function expressions
     * @param c    Consumer function to receive each discovered inference ID
     */
    private void collectInferenceIdsFromInferenceFunctions(LogicalPlan plan, Consumer<String> c) {
        EsqlFunctionRegistry snapshotRegistry = functionRegistry.snapshotRegistry();
        plan.forEachExpressionUp(UnresolvedFunction.class, f -> {
            String functionName = snapshotRegistry.resolveAlias(f.name());
            if (snapshotRegistry.functionExists(functionName)) {
                FunctionDefinition def = snapshotRegistry.resolveFunction(functionName);
                if (InferenceFunction.class.isAssignableFrom(def.clazz())) {
                    String inferenceId = inferenceId(f, def);
                    if (inferenceId != null) {
                        c.accept(inferenceId);
                    }
                }
            }
        });
    }

    /**
     * Extracts the inference ID from an InferencePlan object.
     *
     * @param plan The InferencePlan object to extract the ID from
     * @return The inference ID as a string
     */
    private static String inferenceId(InferencePlan<?> plan) {
        return BytesRefs.toString(plan.inferenceId().fold(FoldContext.small()));
    }

    /**
     * Extracts the inference ID from an InferenceFunction expression that is not yet resolved.
     *
     * @param f   The UnresolvedFunction expression representing the inference function
     * @param def The FunctionDefinition of the inference function
     * @return The inference ID as a string, or null if not found or invalid
     */
    private static String inferenceId(UnresolvedFunction f, FunctionDefinition def) {
        EsqlFunctionRegistry.FunctionDescription functionDescription = EsqlFunctionRegistry.description(def);

        for (int i = 0; i < functionDescription.args().size(); i++) {
            EsqlFunctionRegistry.ArgSignature arg = functionDescription.args().get(i);
            if (i >= f.arguments().size()) {
                // Argument is missing. We will fail later during verifier, so just return null here.
                return null;
            }

            if (arg.name().equals(InferenceFunction.INFERENCE_ID_PARAMETER_NAME)) {
                Expression inferenceId = f.arguments().get(i);
                if (inferenceId != null && inferenceId.foldable() && DataType.isString(inferenceId.dataType())) {
                    return BytesRefs.toString(inferenceId.fold(FoldContext.small()));
                }
            }
        }

        return null;
    }

    public static Factory factory(Client client) {
        return new Factory(client, client.threadPool());
    }

    public static class Factory {
        private final Client client;
        private final ThreadPool threadPool;

        private Factory(Client client, ThreadPool threadPool) {
            this.client = client;
            this.threadPool = threadPool;
        }

        public InferenceResolver create(EsqlFunctionRegistry functionRegistry) {
            return new InferenceResolver(client, functionRegistry, threadPool);
        }
    }
}
