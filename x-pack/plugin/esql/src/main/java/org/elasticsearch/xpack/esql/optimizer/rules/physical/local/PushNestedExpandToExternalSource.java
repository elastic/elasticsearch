/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection;
import org.elasticsearch.xpack.esql.plan.logical.DatasetNestedExpand;
import org.elasticsearch.xpack.esql.plan.physical.AbstractExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExpandingExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.ExpandingExternalSourceExec.Expansion;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Turns a {@code NESTED_EXPAND} over a dataset into an {@link ExpandingExternalSourceExec}: a dataset has no child
 * documents to find the elements through later, so the format reader reads the collection at element grain instead, in
 * the one pass over the file. The {@link NestedExpandExec} is removed and the scan's output gains the expanded columns.
 * <p>
 * <b>What the expansion moves below.</b> Every node between the expansion and the scan now sees element rows rather than
 * parent rows, so only a node that computes each row from that row alone may sit there. Which nodes those are is decided
 * once, on the coordinator over the logical plan, by {@link DatasetNestedExpand}, which refuses any other node below the
 * expansion, a name it would shadow, a rename of the collection, a record reference or a second expansion, before the
 * plan reaches a data node. This rule keeps no list of its own: it moves the expansion below any {@link UnaryExec} and
 * down the left side of a {@link LookupJoinExec}, rebuilding each node over the expanding scan, so those nodes run once
 * per element rather than once per parent. It asserts again, as {@link IllegalStateException}s, only what it can check
 * with the predicates {@link DatasetNestedExpand} shares.
 * <p>
 * <b>Limits.</b> A {@link LimitExec} under the expansion is a copy of the limit above it, which
 * {@code PushDownAndCombineLimits} puts under a {@code NestedExpand}. It becomes the scan's parent limit, and leaves the
 * chain, when nothing between it and the scan drops rows ({@link ProjectExec}, a deterministic {@link EvalExec}, another
 * such limit): a limit on parents applied before a filter that stays above the scan would lose rows. Any other limit
 * stays where it is, so no limit this rule did not fold is lost. A limit {@code PushLimitToExternalSource} already pushed
 * into the scan counted the scan's rows, the parents, so it becomes part of the parent limit too.
 */
public final class PushNestedExpandToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    NestedExpandExec,
    LocalPhysicalOptimizerContext> {

    public PushNestedExpandToExternalSource() {
        // Bottom-up, so the lower of two expansions folds first and the upper one meets an expanding scan
        super(TransformDirection.UP);
    }

    @Override
    protected PhysicalPlan rule(NestedExpandExec expand, LocalPhysicalOptimizerContext context) {
        Chain chain = chainToScan(expand);
        if (chain == null) {
            // Not over a dataset scan: an index's expansion runs in NestedExpandOperator
            return expand;
        }
        ExternalSourceExec scan = checkInvariants(expand, chain);
        ParentLimit parentLimit = parentLimit(chain, context);
        Expansion expansion = new Expansion(expand.path(), expand.paths(), parentLimit.value());
        ExpandingExternalSourceExec expanding = scan.withPushedLimit(FormatReader.NO_LIMIT).withExpansion(expand.expanded(), expansion);
        return rebuild(expand, chain, parentLimit.foldedFrom(), expanding);
    }

    /**
     * The nodes between an expansion and the dataset scan it reads, top down, and the scan.
     *
     * @param between every node from the expansion's child down to, not including, the scan
     */
    private record Chain(List<PhysicalPlan> between, AbstractExternalSourceExec<?> scan) {}

    /** The chain from {@code expand} down to a dataset scan, or {@code null} when it does not end in one. */
    private static Chain chainToScan(NestedExpandExec expand) {
        List<PhysicalPlan> between = new ArrayList<>();
        PhysicalPlan node = expand.child();
        while (node instanceof AbstractExternalSourceExec<?> == false) {
            between.add(node);
            if (node instanceof UnaryExec unary) {
                node = unary.child();
            } else if (node instanceof LookupJoinExec join) {
                node = join.left();
            } else {
                return null;
            }
        }
        return new Chain(between, (AbstractExternalSourceExec<?>) node);
    }

    /**
     * What {@link DatasetNestedExpand#refuseUnsupported} refused on the coordinator, over the logical plan this one was
     * mapped from: a plan that reaches here otherwise is a planner bug, not a query's error. Returns the scan, which
     * reads file rows once a second expansion is ruled out.
     */
    private static ExternalSourceExec checkInvariants(NestedExpandExec expand, Chain chain) {
        if (chain.scan() instanceof ExpandingExternalSourceExec expanding) {
            throw invariant(expand, "a second expansion over [" + expanding.expansion().path() + "]");
        }
        ExternalSourceExec scan = (ExternalSourceExec) chain.scan();
        if (DatasetNestedExpand.readsRecordRef(scan.attributes())) {
            throw invariant(expand, "a record reference");
        }
        if (DatasetNestedExpand.renameOf(scan.declaredReadSpec(), expand.path()) != null) {
            throw invariant(expand, "a rename of the collection");
        }
        Set<String> expandedNames = DatasetNestedExpand.names(expand.expanded());
        if (DatasetNestedExpand.clashingName(scan.attributes(), expandedNames) != null) {
            throw invariant(expand, "a dataset column named like an expanded one");
        }
        for (PhysicalPlan between : chain.between()) {
            if (DatasetNestedExpand.clashingName(between.output(), expandedNames) != null) {
                throw invariant(expand, "[" + between.nodeName() + "] defining a column named like an expanded one");
            }
        }
        return scan;
    }

    /**
     * The most parents the read needs, or {@link FormatReader#NO_LIMIT}, and where in the chain it was folded from.
     *
     * @param foldedFrom the index in {@link Chain#between()} from which down every limit is part of {@code value}
     */
    private record ParentLimit(int value, int foldedFrom) {}

    /**
     * The scan's own pushed limit and every limit in the chain below the lowest node that can drop rows (see the class
     * Javadoc), folded into one parent limit.
     */
    private static ParentLimit parentLimit(Chain chain, LocalPhysicalOptimizerContext context) {
        int parentLimit = chain.scan().pushedLimit();
        List<PhysicalPlan> between = chain.between();
        int i = between.size() - 1;
        for (; i >= 0; i--) {
            PhysicalPlan node = between.get(i);
            if (node instanceof LimitExec limit) {
                int value = (int) limit.limit().fold(context.foldCtx());
                parentLimit = parentLimit == FormatReader.NO_LIMIT ? value : Math.min(parentLimit, value);
            } else if (keepsEveryRow(node) == false) {
                break;
            }
        }
        return new ParentLimit(parentLimit, i + 1);
    }

    /**
     * The chain over {@code expanding}, which now carries the expanded columns, without the limits folded into its parent
     * limit, those at or below {@code foldedFrom}; every other node, any other limit included, is rebuilt where it was. A
     * projection keeps the expanded columns, as it would have had it sat above the expansion, and the result is projected
     * to the expansion's output when their order differs.
     */
    private static PhysicalPlan rebuild(NestedExpandExec expand, Chain chain, int foldedFrom, ExpandingExternalSourceExec expanding) {
        PhysicalPlan rebuilt = expanding;
        List<PhysicalPlan> between = chain.between();
        for (int i = between.size() - 1; i >= 0; i--) {
            PhysicalPlan node = between.get(i);
            if (node instanceof ProjectExec project) {
                List<NamedExpression> projections = new ArrayList<>(project.projections());
                projections.addAll(expand.expanded());
                rebuilt = new ProjectExec(project.source(), rebuilt, projections);
            } else if (node instanceof LookupJoinExec join) {
                rebuilt = join.replaceChildren(rebuilt, join.right());
            } else if (node instanceof LimitExec == false || i < foldedFrom) {
                rebuilt = ((UnaryExec) node).replaceChild(rebuilt);
            }
        }
        if (rebuilt.output().equals(expand.output())) {
            return rebuilt;
        }
        return new ProjectExec(expand.source(), rebuilt, new ArrayList<NamedExpression>(expand.output()));
    }

    private static IllegalStateException invariant(NestedExpandExec expand, String what) {
        return new IllegalStateException("[" + expand.sourceText() + "] over a dataset reached the data node with " + what);
    }

    /** Whether {@code node} emits each input row exactly once, so a limit on the rows below it bounds the rows above. */
    private static boolean keepsEveryRow(PhysicalPlan node) {
        return node instanceof ProjectExec || (node instanceof EvalExec eval && DatasetNestedExpand.deterministic(eval.fields()));
    }
}
