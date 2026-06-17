/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
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
 * Executes the deliberately small post-fetch pushdown fragment supported by remote fetch.
 * <p>
 * At runtime, this class translates the validated pushdown {@link FragmentExec} into a local operator pipeline and
 * runs that pipeline against fetched pages on the target data node.
 * <p>
 * The source fragment's last output attribute is the synthetic position-mapping attribute. It corresponds to the final
 * {@link IntBlock} appended by this executor and must remain the last output block after pushdown execution.
 */
final class RemoteFetchPushdownPlanExecutor {
    private final BigArrays bigArrays;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;

    private record PushdownPipeline(Layout layout, NameId positionAttributeId) {}

    RemoteFetchPushdownPlanExecutor(BigArrays bigArrays, LocalCircuitBreaker.SizeSettings localBreakerSettings) {
        this.bigArrays = bigArrays;
        this.localBreakerSettings = localBreakerSettings;
    }

    List<Page> execute(
        List<Page> pages,
        PhysicalPlan pushdownPlan,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        BlockFactory executionBlockFactory,
        FoldContext foldContext
    ) {
        if (pushdownPlan == null || pages.isEmpty()) {
            return pages;
        }
        List<Page> current = appendPositionColumn(pages, executionBlockFactory);
        List<Operator.OperatorFactory> factories = new ArrayList<>();
        boolean success = false;
        try {
            PushdownPipeline pipeline = buildPipeline(pushdownPlan, factories, shardContexts, foldContext);
            for (Operator.OperatorFactory factory : factories) {
                List<Page> next = new ArrayList<>();
                try (
                    Operator operator = factory.get(
                        new DriverContext(bigArrays, executionBlockFactory, localBreakerSettings, "remote_fetch_pushdown")
                    )
                ) {
                    for (Page page : current) {
                        operator.addInput(page);
                        Page output;
                        while ((output = operator.getOutput()) != null) {
                            output.allowPassingToDifferentDriver();
                            next.add(output);
                        }
                    }
                    operator.finish();
                    Page output;
                    while ((output = operator.getOutput()) != null) {
                        output.allowPassingToDifferentDriver();
                        next.add(output);
                    }
                }
                current = next;
            }
            List<Page> reordered = movePositionColumnToEnd(current, pipeline);
            if (reordered != current) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(current.iterator(), page -> page::releaseBlocks)));
                current = reordered;
            }
            success = true;
            return current;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(current.iterator(), page -> page::releaseBlocks)));
            }
        }
    }

    private List<Page> appendPositionColumn(List<Page> pages, BlockFactory executionBlockFactory) {
        List<Page> withPosition = new ArrayList<>(pages.size());
        int position = 0;
        for (Page page : pages) {
            try (IntBlock.Builder builder = executionBlockFactory.newIntBlockBuilder(page.getPositionCount())) {
                for (int row = 0; row < page.getPositionCount(); row++) {
                    builder.appendInt(position++);
                }
                Block positionBlock = builder.build();
                Block[] blocks = new Block[page.getBlockCount() + 1];
                for (int i = 0; i < page.getBlockCount(); i++) {
                    blocks[i] = page.getBlock(i);
                    blocks[i].incRef();
                }
                blocks[page.getBlockCount()] = positionBlock;
                withPosition.add(new Page(page.getPositionCount(), blocks));
            } finally {
                page.releaseBlocks();
            }
        }
        return withPosition;
    }

    private List<Page> movePositionColumnToEnd(List<Page> pages, PushdownPipeline pipeline) {
        NameId positionAttributeId = pipeline.positionAttributeId();
        if (positionAttributeId == null) {
            return pages;
        }
        Layout.ChannelAndType position = pipeline.layout().get(positionAttributeId);
        if (position == null) {
            throw new IllegalStateException("remote fetch pushdown lost position-mapping attribute");
        }
        int positionChannel = position.channel();
        boolean alreadyLast = true;
        for (Page page : pages) {
            if (positionChannel != page.getBlockCount() - 1) {
                alreadyLast = false;
                break;
            }
        }
        if (alreadyLast) {
            return pages;
        }
        List<Page> reordered = new ArrayList<>(pages.size());
        boolean success = false;
        try {
            for (Page page : pages) {
                Block[] blocks = new Block[page.getBlockCount()];
                int output = 0;
                for (int channel = 0; channel < page.getBlockCount(); channel++) {
                    if (channel != positionChannel) {
                        blocks[output++] = page.getBlock(channel);
                        blocks[output - 1].incRef();
                    }
                }
                blocks[blocks.length - 1] = page.getBlock(positionChannel);
                blocks[blocks.length - 1].incRef();
                reordered.add(new Page(page.getPositionCount(), blocks));
            }
            success = true;
            return reordered;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(reordered.iterator(), page -> page::releaseBlocks)));
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
        throw new IllegalStateException("unsupported remote fetch pushdown plan [" + plan.getClass().getSimpleName() + "]");
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
            NameId positionAttributeId = output.isEmpty() ? null : output.getLast().id();
            return new PushdownPipeline(builder.build(), positionAttributeId);
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
        throw new IllegalStateException("unsupported remote fetch pushdown plan [" + plan.getClass().getSimpleName() + "]");
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
