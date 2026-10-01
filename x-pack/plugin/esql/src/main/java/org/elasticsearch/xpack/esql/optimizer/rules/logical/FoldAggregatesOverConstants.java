/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.RuleUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.ConstantAggregation;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Folds aggregations whose result doesn't depend on the values of the rows they see. What an aggregation returns for no rows,
 * one row or repeated rows is asked from its own compute aggregator (see {@link ConstantAggregation}), never restated here.
 * <p>
 * A false or null filter, or an input the aggregator ignores (e.g. {@code null}), folds to the no-rows result:
 * <pre>
 *     ... | STATS x = someAgg(y) WHERE FALSE {BY z} | ...
 *     =>
 *     ... | EVAL x = no-rows result | KEEP x{, z} | ...
 * </pre>
 * A constant input whose repetitions don't change the aggregator's state folds to the one-row result, if there are rows:
 * <pre>
 *     ... | EVAL c = 1 | STATS x = MAX(c) WHERE f | ...
 *     =>
 *     ... | STATS $$count = COUNT(*) WHERE f | EVAL x = CASE($$count > 0, 1, null) | KEEP x | ...
 * </pre>
 * Unfiltered and with groupings, the row count check is dropped since every group has at least one row.
 * <p>
 * This rule is applied to both STATS' {@link Aggregate} and {@link InlineJoin} right-hand side {@link Aggregate} plans.
 * Skipped in local optimizer: once a fragment contains an Agg, this can no longer be pruned, which the rule can do.
 */
public final class FoldAggregatesOverConstants extends OptimizerRules.ParameterizedOptimizerRule<LogicalPlan, LogicalOptimizerContext>
    implements
        OptimizerRules.CoordinatorOnly {

    /**
     * Types whose literal values can be turned into blocks unambiguously. Spatial types, for one, depend on the extraction
     * preference.
     */
    private static final Set<DataType> PROBED_TYPES = Set.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.UNSIGNED_LONG,
        DataType.DOUBLE,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.IP,
        DataType.VERSION
    );

    /**
     * Aggregator suppliers are picked by the input type, so a {@code null} input is given one of these to find a supplier.
     * The no-rows result doesn't depend on which.
     */
    private static final List<DataType> NULL_STAND_INS = List.of(
        DataType.KEYWORD,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.INTEGER,
        DataType.BOOLEAN,
        DataType.DATETIME
    );

    public FoldAggregatesOverConstants() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan, LogicalOptimizerContext ctx) {
        Aggregate aggregate;
        InlineJoin ij = null;
        if (plan instanceof Aggregate a) {
            aggregate = a;
        } else if (plan instanceof InlineJoin inlineJoin) {
            ij = inlineJoin;
            Holder<Aggregate> aggHolder = new Holder<>();
            inlineJoin.right().forEachDown(Aggregate.class, aggHolder::setIfAbsent);
            aggregate = aggHolder.get();
        } else {
            return plan; // not an Aggregate or InlineJoin, nothing to do
        }

        if (aggregate != null) {
            int oldAggSize = aggregate.aggregates().size();
            List<NamedExpression> newAggs = new ArrayList<>(oldAggSize);
            List<Alias> newEvals = new ArrayList<>(oldAggSize);
            List<NamedExpression> newProjections = new ArrayList<>(oldAggSize);
            Folding folding = new Folding(aggregate, ij != null && seesAllJoinedRows(aggregate), ctx);

            for (var ne : aggregate.aggregates()) {
                Expression folded = ne instanceof Alias alias && alias.child() instanceof AggregateFunction af ? folding.fold(af) : null;
                if (folded != null) {
                    Alias newAlias = ((Alias) ne).replaceChild(folded);
                    newEvals.add(newAlias);
                    newProjections.add(newAlias.toAttribute());
                } else {
                    newAggs.add(ne); // agg function unchanged or grouping key
                    newProjections.add(ne.toAttribute());
                }
            }
            // the grouping keys stay last
            int countsAt = 0;
            for (int i = 0; i < newAggs.size(); i++) {
                if (Alias.unwrap(newAggs.get(i)) instanceof AggregateFunction) {
                    countsAt = i + 1;
                }
            }
            newAggs.addAll(countsAt, folding.counts);
            if (newAggs.isEmpty() && ij == null && aggregate.groupings().isEmpty() == false) {
                // Aggs cannot produce pages with 0 columns, so retain one grouping
                newAggs.add(Expressions.attribute(aggregate.groupings().getFirst()));
            }

            if (newEvals.isEmpty() == false) {
                if (newAggs.isEmpty()) { // the Aggregate node is pruned
                    if (ij != null) { // this is an Aggregate part of right-hand side of an InlineJoin
                        final LogicalPlan leftHandSide = ij.left(); // final so we can use it in the lambda below
                        // the aggregate becomes a simple Eval since it's not needed anymore (it was replaced with Literals)
                        var newRight = ij.right()
                            .transformDown(
                                Aggregate.class,
                                agg -> agg == aggregate ? new Eval(aggregate.source(), aggregate.child(), newEvals) : agg
                            );
                        // Remove the StubRelation since the right-hand side of the join is now part of the main plan
                        // and it won't be executed separately by the EsqlSession INLINE STATS planning.
                        newRight = InlineJoin.replaceStub(leftHandSide, newRight);

                        // project the correct output (the one of the former inlinejoin) and remove the InlineJoin altogether,
                        // replacing it with its right-hand side followed by its left-hand side
                        plan = new Project(ij.source(), newRight, ij.output());
                    } else { // this is a standalone Aggregate
                        plan = localRelation(aggregate.source(), newEvals);
                    }
                } else {
                    if (ij != null) { // this is an Aggregate part of right-hand side of an InlineJoin
                        plan = ij.replaceRight(
                            ij.right()
                                .transformUp(
                                    Aggregate.class,
                                    agg -> agg == aggregate ? updateAggregate(agg, newAggs, newEvals, newProjections) : agg
                                )
                        );
                    } else { // this is a standalone Aggregate
                        plan = updateAggregate(aggregate, newAggs, newEvals, newProjections);
                    }
                }
            }
        }
        return plan;
    }

    /**
     * The literal {@code aggregation} evaluates to when it sees no rows, or only rows whose input it ignores, like
     * {@code null}. Returns {@code null} if that can't be computed at plan time.
     */
    @Nullable
    public static Literal noRowsResult(AggregateFunction aggregation, FoldContext foldContext) {
        // the partial state is irrelevant: the FromPartial consuming it folds to the actual result
        if (aggregation.dataType() == DataType.NULL || aggregation instanceof ToPartial) {
            return Literal.of(aggregation, null);
        }
        AggregateFunction effective = unwrapFromPartial(aggregation);
        // their suppliers may throw on parameters the post-optimization verification is to reject
        if (effective instanceof PostOptimizationVerificationAware) {
            return null;
        }
        if (effective instanceof TimeSeriesAggregateFunction ts) {
            effective = ts.perTimeSeriesAggregation();
        }
        if (effective instanceof ToAggregator && effective instanceof TimeSeriesAggregateFunction == false) {
            AggregateFunction typed = withTypedNullFields(effective);
            if (typed != null) {
                Object value = ConstantAggregation.empty((ToAggregator) typed, typed.fields().size(), typed.source(), foldContext);
                return Literal.of(aggregation, value);
            }
        }
        return effective instanceof AnyNullIsNull ? Literal.of(aggregation, null) : null;
    }

    /**
     * Whether {@code aggregation} gets an input that it ignores, making it fold to {@link #noRowsResult}.
     */
    public static boolean ignoresInput(AggregateFunction aggregation) {
        AggregateFunction inner = unwrapToPartial(unwrapFromPartial(aggregation));
        if (inner instanceof AnyNullIsNull) {
            return inner.fields().stream().anyMatch(field -> DataType.isNull(field.dataType()));
        }
        return inner.fields().isEmpty() == false && DataType.isNull(inner.fields().getFirst().dataType());
    }

    private static AggregateFunction unwrapFromPartial(AggregateFunction aggregation) {
        return aggregation instanceof FromPartial fromPartial && fromPartial.function() instanceof AggregateFunction inner
            ? inner
            : aggregation;
    }

    private static AggregateFunction unwrapToPartial(AggregateFunction aggregation) {
        return aggregation instanceof ToPartial toPartial && toPartial.function() instanceof AggregateFunction inner ? inner : aggregation;
    }

    @Nullable
    private static AggregateFunction withTypedNullFields(AggregateFunction aggregation) {
        if (aggregation.fields().stream().noneMatch(field -> DataType.isNull(field.dataType()))) {
            return aggregation;
        }
        for (DataType standIn : NULL_STAND_INS) {
            List<Expression> fields = new ArrayList<>(aggregation.fields().size());
            for (Expression field : aggregation.fields()) {
                fields.add(DataType.isNull(field.dataType()) ? new Literal(field.source(), null, standIn) : field);
            }
            AggregateFunction typed = aggregation.withFields(fields);
            if (typed.typeResolved().resolved()) {
                return typed;
            }
        }
        return null;
    }

    /**
     * Folds the aggregations of one {@link Aggregate}, collecting the row counts the folded expressions depend on.
     */
    private static final class Folding {
        private final Aggregate aggregate;
        private final boolean inlineJoin;
        private final LogicalOptimizerContext ctx;
        private final AttributeSet groupings;
        private final List<Alias> counts = new ArrayList<>();
        private AttributeMap<Expression> foldables;

        Folding(Aggregate aggregate, boolean inlineJoin, LogicalOptimizerContext ctx) {
            this.aggregate = aggregate;
            this.inlineJoin = inlineJoin;
            this.ctx = ctx;
            this.groupings = Expressions.references(aggregate.groupings());
        }

        @Nullable
        Expression fold(AggregateFunction af) {
            // not hasFilter(), which reports a foldable filter that isn't a literal yet as no filter
            boolean filtered = af.filter() != null && Literal.TRUE.equals(af.filter()) == false;
            if (filtered) {
                Expression filter = resolve(af.filter());
                if (filter.foldable()) {
                    if (Boolean.TRUE.equals(filter.fold(ctx.foldCtx())) == false) {
                        return noRowsResult(af, ctx.foldCtx());
                    }
                    filtered = false;
                }
            }
            if (ignoresInput(af)) {
                return noRowsResult(af, ctx.foldCtx());
            }
            if (probeable(af) == false) {
                return null;
            }
            List<Object> inputs = constantInputs(af);
            if (inputs == null) {
                return null;
            }
            var probe = ConstantAggregation.probe((ToAggregator) af, inputs, af.source(), ctx.foldCtx());
            if (probe == null) {
                return null;
            }
            Literal noRows = Literal.of(af, probe.empty());
            if (probe.inputIgnored()) {
                return noRows;
            }
            if (probe.idempotent() == false) {
                return null;
            }
            Literal oneRow = Literal.of(af, probe.single());
            // every group, and every row INLINE STATS joins the result to, has seen at least one row
            if (filtered == false && (aggregate.groupings().isEmpty() == false || inlineJoin)) {
                return oneRow;
            }
            Source source = af.source();
            Expression hasRows = new GreaterThan(source, rowCount(af, filtered), new Literal(source, 0L, DataType.LONG));
            return new Case(source, hasRows, List.of(oneRow, noRows));
        }

        private boolean probeable(AggregateFunction af) {
            return af instanceof ToAggregator
                // the rows counting aggregations are what folding relies on
                && af instanceof Count == false
                && af instanceof CountApproximate == false
                && af instanceof ToPartial == false
                && af instanceof FromPartial == false
                && af instanceof TimeSeriesAggregateFunction == false
                && af instanceof TimestampAware == false
                // folding would skip the checks run after optimization
                && af instanceof PostOptimizationVerificationAware == false
                && af.hasWindow() == false
                && aggregate instanceof TimeSeriesAggregate == false
                // the row count of a sampled aggregate is an estimate
                && aggregate instanceof SampledAggregate == false;
        }

        @Nullable
        private List<Object> constantInputs(AggregateFunction af) {
            List<Object> inputs = new ArrayList<>(af.fields().size());
            for (Expression field : af.fields()) {
                // a multivalued grouping is expanded: each group sees one of its values, not the literal
                if (PROBED_TYPES.contains(field.dataType()) == false || field.references().stream().anyMatch(groupings::contains)) {
                    return null;
                }
                Expression resolved = resolve(field);
                if (resolved.foldable() == false) {
                    return null;
                }
                inputs.add(Literal.of(ctx.foldCtx(), resolved).value());
            }
            return inputs;
        }

        private Expression resolve(Expression expression) {
            if (expression.foldable()) {
                return expression;
            }
            if (foldables == null) {
                foldables = RuleUtils.foldableReferencesSkipMVGroupings(aggregate.child(), ctx);
            }
            return expression.transformUp(ReferenceAttribute.class, r -> foldables.resolve(r, r));
        }

        private Attribute rowCount(AggregateFunction af, boolean filtered) {
            Source source = af.source();
            Count count = new Count(
                source,
                Literal.keyword(source, StringUtils.WILDCARD),
                filtered ? af.filter() : Literal.TRUE,
                af.window()
            );
            for (NamedExpression ne : aggregate.aggregates()) {
                if (ne instanceof Alias alias && alias.child().semanticEquals(count)) {
                    return alias.toAttribute();
                }
            }
            for (Alias alias : counts) {
                if (alias.child().semanticEquals(count)) {
                    return alias.toAttribute();
                }
            }
            Alias alias = new Alias(source, TemporaryNameGenerator.temporaryName(count, af, counts.size()), count, null, true);
            counts.add(alias);
            return alias.toAttribute();
        }
    }

    /**
     * Whether the right-hand side {@code aggregate} of an {@link InlineJoin} sees exactly the rows its result is joined to.
     */
    private static boolean seesAllJoinedRows(Aggregate aggregate) {
        LogicalPlan child = aggregate.child();
        while (child instanceof Eval || child instanceof Project) {
            child = ((UnaryPlan) child).child();
        }
        return child instanceof StubRelation;
    }

    private static LogicalPlan updateAggregate(
        Aggregate agg,
        List<NamedExpression> newAggs,
        List<Alias> newEvals,
        List<NamedExpression> newProjections
    ) {
        // only update the Aggregate and add an Eval for the removed aggregations
        LogicalPlan newAgg = agg.with(agg.child(), agg.groupings(), newAggs);
        newAgg = new Eval(agg.source(), newAgg, newEvals);
        newAgg = new Project(agg.source(), newAgg, newProjections);
        return newAgg;
    }

    private static LocalRelation localRelation(Source source, List<Alias> newEvals) {
        Block[] blocks = new Block[newEvals.size()];
        List<Attribute> attributes = new ArrayList<>(newEvals.size());
        for (int i = 0; i < newEvals.size(); i++) {
            Alias alias = newEvals.get(i);
            attributes.add(alias.toAttribute());
            blocks[i] = BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, ((Literal) alias.child()).value(), 1);
        }
        return new LocalRelation(source, attributes, LocalSupplier.of(new Page(blocks)));
    }
}
