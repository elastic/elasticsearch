/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.FilterOperator.FilterOperatorFactory;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ProjectOperator.ProjectOperatorFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RemoteFetchSource;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates and builds operators for the deliberately small post-fetch pushdown fragment supported by remote fetch.
 * <p>
 * At runtime, this class translates a supported pushdown {@link FragmentExec} into an operator pipeline that is
 * appended to the exchange server's data-node driver pipeline.
 * <p>
 * The source fragment's last output attribute is the synthetic position-mapping attribute. It corresponds to the final
 * {@link IntBlock} in the data-node pipeline and must remain the last output block after pushdown execution.
 */
final class RemoteFetchPushdownOperatorBuilder {
    private record PushdownPipeline(Layout layout, NameId positionAttributeId) {}

    /**
     * Validates the remote-fetch pushdown shape used on the wire.
     * <p>
     * Accepted forms are {@link FragmentExec} wrapping logical nodes from the constrained
     * {@link RemoteFetchSource}/{@link Eval}/{@link Filter}/{@link Project} family.
     */
    static void validateSupportedPlan(PhysicalPlan plan) {
        if (plan == null) {
            return;
        }
        if (plan instanceof FragmentExec fragmentExec) {
            validateSupportedFragment(fragmentExec.fragment());
            return;
        }
        throw new IllegalArgumentException(unsupportedPlanMessage(plan));
    }

    private static void validateSupportedFragment(LogicalPlan plan) {
        plan.forEachDown(node -> {
            if (node instanceof RemoteFetchSource == false
                && node instanceof Eval == false
                && node instanceof Filter == false
                && node instanceof Project == false) {
                throw new IllegalArgumentException(unsupportedPlanMessage(node));
            }
        });
    }

    private static String unsupportedPlanMessage(Object plan) {
        return "unsupported remote fetch pushdown plan [" + plan.getClass().getSimpleName() + "]";
    }

    /**
     * Builds runtime operators from a supported, non-null pushdown plan.
     * <p>
     * Callers should validate request-time payloads with {@link #validateSupportedPlan(PhysicalPlan)} before invoking
     * this method, and skip the call entirely when the request carries no pushdown plan.
     */
    List<Operator> buildOperators(
        PhysicalPlan pushdownPlan,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        FoldContext foldContext,
        DriverContext driverContext
    ) {
        if (pushdownPlan == null) {
            throw new IllegalArgumentException("remote fetch pushdown plan must not be null");
        }
        List<Operator.OperatorFactory> factories = new ArrayList<>();
        PushdownPipeline pipeline = buildPipeline(pushdownPlan, factories, shardContexts, foldContext);
        List<Operator> operators = new ArrayList<>(factories.size() + 1);
        boolean success = false;
        try {
            for (Operator.OperatorFactory factory : factories) {
                operators.add(factory.get(driverContext));
            }
            appendPositionFinalizerIfNeeded(operators, pipeline, driverContext);
            success = true;
            return operators;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(operators));
            }
        }
    }

    private PushdownPipeline buildPipeline(
        PhysicalPlan plan,
        List<Operator.OperatorFactory> factories,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        FoldContext foldContext
    ) {
        if (plan instanceof FragmentExec fragmentExec) {
            return buildFragmentPipeline(fragmentExec.fragment(), factories, shardContexts, foldContext);
        }
        throw new IllegalStateException(unsupportedPlanMessage(plan));
    }

    private void appendPositionFinalizerIfNeeded(List<Operator> operators, PushdownPipeline pipeline, DriverContext driverContext) {
        NameId positionAttributeId = pipeline.positionAttributeId();
        if (positionAttributeId == null) {
            return;
        }
        Layout.ChannelAndType position = pipeline.layout().get(positionAttributeId);
        if (position == null) {
            throw new IllegalStateException("remote fetch pushdown lost position-mapping attribute");
        }
        int positionChannel = position.channel();
        int totalChannels = pipeline.layout().numberOfChannels();
        if (positionChannel == totalChannels - 1) {
            return;
        }
        List<Integer> projection = new ArrayList<>(totalChannels);
        for (int channel = 0; channel < totalChannels; channel++) {
            if (channel != positionChannel) {
                projection.add(channel);
            }
        }
        projection.add(positionChannel);
        operators.add(new ProjectOperatorFactory(projection).get(driverContext));
    }

    private PushdownPipeline buildFragmentPipeline(
        LogicalPlan plan,
        List<Operator.OperatorFactory> factories,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        FoldContext foldContext
    ) {
        if (plan instanceof RemoteFetchSource sourcePlan) {
            Layout.Builder builder = new Layout.Builder();
            builder.append(sourcePlan.output());
            List<Attribute> output = sourcePlan.output();
            // The position-mapping attribute is always the trailing attribute in the source output; verify by name
            // instead of trusting the position alone so a malformed fragment fails fast rather than mapping the
            // wrong channel.
            Attribute positionAttribute = output.isEmpty() ? null : output.getLast();
            if (positionAttribute == null || positionAttribute.name().equals(RemoteFetchSource.POSITION_ATTRIBUTE_NAME) == false) {
                throw new IllegalStateException(
                    "remote fetch pushdown source must end with the position-mapping attribute ["
                        + RemoteFetchSource.POSITION_ATTRIBUTE_NAME
                        + "] but ends with ["
                        + (positionAttribute == null ? "nothing" : positionAttribute.name())
                        + "]"
                );
            }
            return new PushdownPipeline(builder.build(), positionAttribute.id());
        }
        if (plan instanceof Eval evalPlan) {
            PushdownPipeline child = buildFragmentPipeline(evalPlan.child(), factories, shardContexts, foldContext);
            Layout childLayout = child.layout();
            Layout.Builder builder = childLayout.builder();
            for (Alias field : evalPlan.fields()) {
                factories.add(new EvalOperatorFactory(EvalMapper.toEvaluator(foldContext, field.child(), childLayout, shardContexts)));
                builder.append(field.toAttribute());
            }
            return new PushdownPipeline(builder.build(), child.positionAttributeId());
        }
        if (plan instanceof Filter filterPlan) {
            PushdownPipeline child = buildFragmentPipeline(filterPlan.child(), factories, shardContexts, foldContext);
            Layout childLayout = child.layout();
            factories.add(
                new FilterOperatorFactory(EvalMapper.toEvaluator(foldContext, filterPlan.condition(), childLayout, shardContexts))
            );
            return child;
        }
        if (plan instanceof Project projectPlan) {
            PushdownPipeline child = buildFragmentPipeline(projectPlan.child(), factories, shardContexts, foldContext);
            Layout childLayout = child.layout();
            List<Integer> projectionList = new ArrayList<>(projectPlan.projections().size() + 1);
            Layout.Builder builder = new Layout.Builder();
            for (NamedExpression projection : projectPlan.projections()) {
                NameId inputId = projectionInputId(projection);
                if (inputId.equals(child.positionAttributeId())) {
                    continue;
                }
                Layout.ChannelAndType input = childLayout.get(inputId);
                if (input == null) {
                    throw new IllegalStateException("can't find pushdown input for [" + projection + "]");
                }
                projectionList.add(input.channel());
                builder.append(projection);
            }
            if (child.positionAttributeId() != null) {
                Layout.ChannelAndType positionInput = childLayout.get(child.positionAttributeId());
                if (positionInput == null) {
                    throw new IllegalStateException("remote fetch pushdown lost position-mapping attribute");
                }
                projectionList.add(positionInput.channel());
                builder.append(childLayout.inverse().get(positionInput.channel()));
            }
            factories.add(new ProjectOperatorFactory(projectionList));
            return new PushdownPipeline(builder.build(), child.positionAttributeId());
        }
        throw new IllegalStateException(unsupportedPlanMessage(plan));
    }

    private NameId projectionInputId(NamedExpression projection) {
        if (projection instanceof Alias alias) {
            if (alias.child() instanceof NamedExpression child) {
                return child.id();
            }
            throw new IllegalStateException(
                "remote fetch pushdown project aliases must reference existing named expressions; use EvalExec for scalar alias ["
                    + projection
                    + "]"
            );
        }
        return projection.id();
    }
}
