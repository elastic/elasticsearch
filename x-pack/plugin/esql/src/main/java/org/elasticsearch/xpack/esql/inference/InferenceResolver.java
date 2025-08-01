/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
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

    /**
     * Constructs a new {@code InferenceResolver}.
     *
     * @param client The Elasticsearch client for executing inference deployment lookups
     */
    public InferenceResolver(Client client, EsqlFunctionRegistry functionRegistry) {
        this.client = client;
        this.functionRegistry = functionRegistry;
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
        List<String> inferenceIds = new ArrayList<>();
        collectInferenceIds(plan, inferenceIds::add);
        resolveInferenceIds(inferenceIds, listener);
    }

    /**
     * Collects all inference IDs from the given logical plan.
     * <p>
     * This method traverses the logical plan tree and identifies all inference operations,
     * extracting their deployment IDs for subsequent validation. Currently, supports:
     * <ul>
     *   <li>{@link InferencePlan} objects (Completion, etc.)</li>
     * </ul>
     *
     * @param plan The logical plan to scan for inference operations
     * @param c    Consumer function to receive each discovered inference ID
     */
    void collectInferenceIds(LogicalPlan plan, Consumer<String> c) {
        collectInferenceIdsFromInferencePlans(plan, c);
        collectInferenceIdsFromInferenceFunctions(plan, c);
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
            ActionListener.wrap(_r -> listener.onResponse(inferenceResolutionBuilder.build()), listener::onFailure)
        );

        for (var inferenceId : inferenceIds) {
            client.execute(
                GetInferenceModelAction.INSTANCE,
                new GetInferenceModelAction.Request(inferenceId, TaskType.ANY),
                ActionListener.wrap(r -> {
                    ResolvedInference resolvedInference = new ResolvedInference(inferenceId, r.getEndpoints().getFirst().getTaskType());
                    inferenceResolutionBuilder.withResolvedInference(resolvedInference);
                    countdownListener.onResponse(null);
                }, e -> {
                    inferenceResolutionBuilder.withError(inferenceId, e.getMessage());
                    countdownListener.onResponse(null);
                })
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
     * Collects inference IDs from inference function calls within the logical plan.
     * <p>
     * This method scans the logical plan for {@link UnresolvedFunction} instances that represent
     * inference functions (e.g., TEXT_EMBEDDING). For each inference function found:
     * <ol>
     *   <li>Resolves the function definition through the registry and checks if the function implements {@link InferenceFunction}</li>
     *   <li>Extracts the inference deployment ID from the function arguments</li>
     * </ol>
     * <p>
     * This operates during pre-analysis when functions are still unresolved, allowing early
     * validation of inference deployments before query optimization.
     *
     * @param plan The logical plan to scan for inference function calls
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
        return inferenceId(plan.inferenceId());
    }

    /**
     * Extracts the inference ID from an Expression (expect the expression to be a constant).
     */
    private static String inferenceId(Expression e) {
        return BytesRefs.toString(e.fold(FoldContext.small()));
    }

    /**
     * Extracts the inference ID from an {@link UnresolvedFunction} instance.
     * <p>
     * This method inspects the function's arguments to find the inference ID.
     * Currently, it only supports positional parameters named "inference_id".
     *
     * @param f   The unresolved function to extract the ID from
     * @param def The function definition
     * @return The inference ID as a string, or null if not found
     */
    public String inferenceId(UnresolvedFunction f, FunctionDefinition def) {
        EsqlFunctionRegistry.FunctionDescription functionDescription = EsqlFunctionRegistry.description(def);

        for (int i = 0; i < functionDescription.args().size(); i++) {
            EsqlFunctionRegistry.ArgSignature arg = functionDescription.args().get(i);

            if (arg.name().equals(InferenceFunction.INFERENCE_ID_PARAMETER_NAME)) {
                // Found a positional parameter named "inference_id", so use its value
                Expression argValue = f.arguments().get(i);
                if (argValue != null && argValue.foldable()) {
                    return inferenceId(argValue);
                }
            }

            // TODO: support inference ID as an optional named parameter
        }

        return null;
    }

    public static Factory factory(Client client) {
        return new Factory(client);
    }

    public static class Factory {
        private final Client client;

        private Factory(Client client) {
            this.client = client;
        }

        public InferenceResolver create(EsqlFunctionRegistry functionRegistry) {
            return new InferenceResolver(client, functionRegistry);
        }
    }
}
