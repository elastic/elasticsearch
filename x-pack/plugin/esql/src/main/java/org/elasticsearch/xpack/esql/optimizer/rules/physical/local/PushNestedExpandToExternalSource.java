/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.NonDeterministic;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.physical.CompoundOutputEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns a {@code NESTED_EXPAND} over a dataset into the scan's own {@link ExternalSourceExec.Expansion}: a dataset has no
 * child documents to find the elements through later, so the format reader reads the collection at element grain
 * instead, in the one pass over the file. The {@link NestedExpandExec} is removed and the scan's output gains the
 * expanded columns.
 * <p>
 * <b>What the expansion moves below.</b> Every node between the expansion and the scan now sees element rows rather than
 * parent rows, so only a node that computes each row from that row alone may sit there: {@link ProjectExec}, a
 * deterministic {@link EvalExec} or {@link FilterExec}, {@link DissectExec}, {@link GrokExec}, the per-row enrichments
 * ({@link CompoundOutputEvalExec}), {@link EnrichExec}, the left side of a {@link LookupJoinExec}, and
 * {@link MvExpandExec}. Any other node fails the query by name: sampling parents is not sampling elements, an inference
 * command would run once per element, a change point reads its neighbours, and a non-deterministic expression evaluated
 * per element is no longer evaluated per parent.
 * <p>
 * <b>Limits.</b> A {@link LimitExec} under the expansion is always a copy of the limit above it:
 * {@code PushDownAndCombineLimits} puts a local copy under a {@code NestedExpand}, and keeps the smaller of two limits
 * there, while a non-local {@code LIMIT} under it would have pulled the expansion above the exchange, which the
 * {@code Mapper} refuses. So each such limit can be removed, the one above still bounding the result. It is kept as
 * the scan's parent limit only when nothing between it and the scan drops rows ({@link ProjectExec}, a deterministic
 * {@link EvalExec}, another such limit): a limit on parents applied before a filter that stays above the scan would
 * lose rows. A limit {@code PushLimitToExternalSource} already pushed into the scan counted the scan's rows, the
 * parents, so it becomes part of the parent limit too.
 * <p>
 * <b>One collection.</b> A read expands a single collection ({@code ExpandRead} names one path), so a second
 * {@code NESTED_EXPAND} over the same scan is refused.
 * <p>
 * The expansion's attributes sit inside a record that attribute-rewriting rules do not descend into, so this rule runs
 * after all of them.
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
        List<PhysicalPlan> chain = new ArrayList<>();
        PhysicalPlan node = expand.child();
        while (node instanceof ExternalSourceExec == false) {
            chain.add(node);
            if (node instanceof UnaryExec unary) {
                node = unary.child();
            } else if (node instanceof LookupJoinExec join) {
                node = join.left();
            } else {
                // Not over a dataset scan: an index's expansion runs in NestedExpandOperator
                return expand;
            }
        }
        ExternalSourceExec scan = (ExternalSourceExec) node;
        if (scan.expansion() != null) {
            throw new VerificationException(
                "NESTED_EXPAND over a dataset reads one collection per query, so [{}] cannot also expand [{}]",
                expand.sourceText(),
                scan.expansion().path()
            );
        }
        for (Attribute attribute : scan.attributes()) {
            if (FileMetadataColumns.RECORD_REF.equals(attribute.name())) {
                // A record reference addresses a file row, and the rows of an expanding read are elements
                throw new VerificationException(
                    "NESTED_EXPAND over a dataset cannot read [{}]: a record reference addresses a file row",
                    FileMetadataColumns.RECORD_REF
                );
            }
        }
        refuseRenamedCollection(expand, scan);
        for (PhysicalPlan between : chain) {
            if (pushesThrough(between) == false) {
                throw new VerificationException("NESTED_EXPAND over a dataset cannot follow [{}]", between.sourceText());
            }
        }

        // The carrier the analyzer added for the collection, read by nothing once the scan expands it
        Attribute carrier = NestedExpand.carrier(scan.output(), expand.path());
        int parentLimit = scan.pushedLimit();
        boolean limitBounds = true;
        for (int i = chain.size() - 1; i >= 0; i--) {
            PhysicalPlan between = chain.get(i);
            if (between instanceof LimitExec limit && limitBounds) {
                int value = (int) limit.limit().fold(context.foldCtx());
                parentLimit = parentLimit == FormatReader.NO_LIMIT ? value : Math.min(parentLimit, value);
            } else if (keepsEveryRow(between) == false) {
                limitBounds = false;
            }
        }

        List<Attribute> parentAttributes = carrier == null
            ? scan.attributes()
            : scan.attributes().stream().filter(a -> a != carrier).toList();
        PhysicalPlan rebuilt = scan.withAttributes(parentAttributes)
            .withPushedLimit(FormatReader.NO_LIMIT)
            .withExpansion(new ExternalSourceExec.Expansion(expand.path(), expand.expanded(), expand.paths(), parentLimit));
        for (int i = chain.size() - 1; i >= 0; i--) {
            PhysicalPlan between = chain.get(i);
            if (between instanceof LimitExec) {
                // A copy of the limit above the expansion, which still bounds the result
                continue;
            }
            rebuilt = rebuild(between, rebuilt, expand.expanded(), carrier);
        }
        if (rebuilt.output().equals(expand.output())) {
            return rebuilt;
        }
        // The expanded columns now come from the scan, so their place in the output follows the nodes above it
        return new ProjectExec(expand.source(), rebuilt, new ArrayList<NamedExpression>(expand.output()));
    }

    /**
     * The expansion's path is the collection's name in the files, which a declared rename of the dataset's columns does
     * not reach. A rename to or from that name would make the dataset's column and the collection two different things
     * under one name, so the expansion is refused rather than guessing which one the query means.
     */
    private static void refuseRenamedCollection(NestedExpandExec expand, ExternalSourceExec scan) {
        String path = expand.path();
        scan.declaredReadSpec().renames().forEach((logical, physical) -> {
            if (logical.equals(path) || physical.equals(path)) {
                throw new VerificationException(
                    "NESTED_EXPAND over a dataset cannot expand [{}]: the dataset declares a rename of [{}] to [{}]",
                    path,
                    logical,
                    physical
                );
            }
        });
    }

    /** Whether the expansion may move below {@code node}: it computes each row from that row alone. */
    private static boolean pushesThrough(PhysicalPlan node) {
        return switch (node) {
            case ProjectExec project -> true;
            case EvalExec eval -> deterministic(eval.fields());
            case FilterExec filter -> deterministic(List.of(filter.condition()));
            case DissectExec dissect -> true;
            case GrokExec grok -> true;
            case CompoundOutputEvalExec enrichment -> true;
            case EnrichExec enrich -> true;
            case LookupJoinExec join -> true;
            case MvExpandExec mvExpand -> true;
            case LimitExec limit -> true;
            default -> false;
        };
    }

    /** Whether {@code node} emits each input row exactly once, so a limit on the rows below it bounds the rows above. */
    private static boolean keepsEveryRow(PhysicalPlan node) {
        return node instanceof ProjectExec || (node instanceof EvalExec eval && deterministic(eval.fields()));
    }

    private static boolean deterministic(List<? extends Expression> expressions) {
        for (Expression expression : expressions) {
            if (expression.anyMatch(e -> e instanceof NonDeterministic)) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@code node} over {@code child}, which now carries the expanded columns. A projection keeps them, as it would have
     * had it sat above the expansion, and drops the collection's carrier.
     */
    private static PhysicalPlan rebuild(PhysicalPlan node, PhysicalPlan child, List<Attribute> expanded, Attribute carrier) {
        if (node instanceof ProjectExec project) {
            List<NamedExpression> projections = new ArrayList<>();
            for (NamedExpression projection : project.projections()) {
                if (carrier == null || projection.toAttribute().equals(carrier) == false) {
                    projections.add(projection);
                }
            }
            projections.addAll(expanded);
            return new ProjectExec(project.source(), child, projections);
        }
        if (node instanceof UnaryExec unary) {
            return unary.replaceChild(child);
        }
        LookupJoinExec join = (LookupJoinExec) node;
        return join.replaceChildren(child, join.right());
    }
}
