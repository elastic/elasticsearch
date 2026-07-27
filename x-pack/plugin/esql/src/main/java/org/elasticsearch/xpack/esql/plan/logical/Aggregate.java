/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sparkline;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.PARTIAL_AGG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TDIGEST;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;
import static org.elasticsearch.xpack.esql.plan.logical.Filter.checkFilterConditionDataType;

public class Aggregate extends UnaryPlan
    implements
        PostAnalysisVerificationAware,
        TelemetryAware,
        SortAgnostic,
        PipelineBreaker,
        ExecutesOn.Coordinator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Aggregate",
        Aggregate::new
    );
    private static final TransportVersion ESQL_REMOVE_AGGREGATE_TYPE = TransportVersion.fromName("esql_remove_aggregate_type");

    protected final List<Expression> groupings;
    protected final List<? extends NamedExpression> aggregates;

    protected List<Attribute> lazyOutput;

    public Aggregate(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    public Aggregate(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class));
        if (in.getTransportVersion().supports(ESQL_REMOVE_AGGREGATE_TYPE) == false) {
            in.readString();
        }
        this.groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        this.aggregates = in.readNamedWriteableCollectionAsList(NamedExpression.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        if (out.getTransportVersion().supports(ESQL_REMOVE_AGGREGATE_TYPE) == false) {
            out.writeString("STANDARD");
        }
        out.writeNamedWriteableCollection(groupings);
        out.writeNamedWriteableCollection(aggregates());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Aggregate> info() {
        return NodeInfo.create(this, Aggregate::new, child(), groupings, aggregates);
    }

    @Override
    public Aggregate replaceChild(LogicalPlan newChild) {
        return new Aggregate(source(), newChild, groupings, aggregates);
    }

    public Aggregate with(List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return with(child(), newGroupings, newAggregates);
    }

    public Aggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new Aggregate(source(), child, newGroupings, newAggregates);
    }

    /**
     * What this aggregation is grouped by. Generally, this corresponds to the {@code BY} clause, even though this command will not output
     * those values unless they are also part of the {@link Aggregate#aggregates()}. This enables grouping without outputting the grouping
     * keys, and makes it so that an {@link Aggregate}s also acts as a projection.
     * <p>
     * The actual grouping keys will be extracted from multivalues, so that if the grouping is on {@code mv_field}, and the document has
     * {@code mv_field: [1, 2, 2]}, then the document will be part of the groups for both {@code mv_field=1} and {@code mv_field=2} (and
     * counted only once in each group).
     */
    public List<Expression> groupings() {
        return groupings;
    }

    /**
     * The actual aggregates to compute. This includes the grouping keys if they are to be output.
     * <p>
     * Multivalued grouping keys will be extracted into single values, so that if the grouping is on {@code mv_field}, and the document has
     * {@code mv_field: [1, 2, 2]}, then the output will have two corresponding rows, one with {@code mv_field=1} and one with
     * {@code mv_field=2}.
     */
    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    @Override
    public String telemetryLabel() {
        return "STATS";
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && Resolvables.resolved(aggregates);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = output(aggregates);
        }
        return lazyOutput;
    }

    public static List<Attribute> output(List<? extends NamedExpression> aggregates) {
        return mergeOutputAttributes(Expressions.asAttributes(aggregates), emptyList());
    }

    @Override
    protected AttributeSet computeReferences() {
        return computeReferences(aggregates, groupings);
    }

    public static AttributeSet computeReferences(List<? extends NamedExpression> aggregates, List<? extends Expression> groupings) {
        var result = Expressions.references(groupings).combine(Expressions.references(aggregates)).asBuilder();
        for (Expression grouping : groupings) {
            if (grouping instanceof Alias) {
                result.remove(((Alias) grouping).toAttribute());
            }
        }
        return result.build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Aggregate other = (Aggregate) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(child(), other.child());
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        var groupRefsBuilder = AttributeSet.builder();
        // check grouping
        // The grouping can not be an aggregate function
        groupings.forEach(e -> {
            e.forEachUp(g -> {
                if (g instanceof AggregateFunction af) {
                    failures.add(fail(g, "cannot use an aggregate [{}] for grouping", af));
                } else if (g instanceof GroupingFunction gf) {
                    gf.children()
                        .forEach(
                            c -> c.forEachDown(
                                GroupingFunction.class,
                                inner -> failures.add(
                                    fail(
                                        inner,
                                        "cannot nest grouping functions; found [{}] inside [{}]",
                                        inner.sourceText(),
                                        gf.sourceText()
                                    )
                                )
                            )
                        );
                }
            });
            // keep the grouping attributes (common case)
            Attribute attr = Expressions.attribute(e);
            if (attr != null) {
                groupRefsBuilder.add(attr);
            }
            checkUnsupportedStatsGroupingType(e, failures);
        });
        var groupRefs = groupRefsBuilder.build();

        // check aggregates - accept only aggregate functions or expressions over grouping
        // don't allow the group by itself to avoid duplicates in the output
        // and since the groups are copied, only look at the declared aggregates
        Holder<Boolean> containsTimeSeries = new Holder<>(false);
        forEachDown(TimeSeriesAggregate.class, ts -> containsTimeSeries.set(true));
        aggregates.subList(0, aggregates.size() - groupings.size()).forEach(e -> {
            var exp = Alias.unwrap(e);
            if (exp.foldable() && containsTimeSeries.get() == false) {
                failures.add(fail(exp, "expected an aggregate function but found [{}]", exp.sourceText()));
            }
            // traverse the tree to find invalid matches
            checkInvalidNamedExpressionUsage(exp, groupings, groupRefs, failures, 0);
        });
        checkTimeSeriesAggregates(failures);
        checkCategorizeGrouping(failures);
        checkMultipleScoreAggregations(failures);
    }

    static void checkUnsupportedGroupingType(Expression e, Failures failures) {
        if ((e instanceof FieldAttribute f && f.dataType().isCounter())
            || e.dataType() == AGGREGATE_METRIC_DOUBLE
            || e.dataType() == DATE_PERIOD
            || e.dataType() == DATE_RANGE
            || e.dataType() == EXPONENTIAL_HISTOGRAM
            || e.dataType() == PARTIAL_AGG
            || e.dataType() == TDIGEST
            || e.dataType() == TIME_DURATION) {
            failures.add(fail(e, "cannot group by on [{}] type for grouping [{}]", e.dataType().typeName(), e.sourceText()));
        }
    }

    private static void checkUnsupportedStatsGroupingType(Expression e, Failures failures) {
        checkUnsupportedGroupingType(e, failures);
        if (e.dataType() == DENSE_VECTOR) {
            failures.add(fail(e, "cannot group by on [{}] type for grouping [{}]", e.dataType().typeName(), e.sourceText()));
        }
    }

    protected void checkTimeSeriesAggregates(Failures failures) {
        if (hasTimeSeriesSource(child())) {
            return;
        }
        Holder<Boolean> hasTimeSeriesAgg = new Holder<>(false);
        forEachExpression(TimeSeriesAggregateFunction.class, r -> hasTimeSeriesAgg.set(true));
        if (hasTimeSeriesAgg.get() == false) {
            return;
        }
        if (child().anyMatch(p -> p instanceof UnionAll)) {
            failures.add(
                fail(
                    this,
                    "time-series aggregation [{}] cannot be applied over a union of data sources; "
                        + "apply the time-series aggregation inside each subquery instead",
                    sourceText()
                )
            );
        } else {
            forEachExpression(
                TimeSeriesAggregateFunction.class,
                r -> failures.add(fail(r, "time_series aggregate[{}] can only be used with the TS command", r.sourceText()))
            );
        }
    }

    /**
     * Returns {@code true} if {@code plan} (or any non-{@link UnionAll} descendant, excluding the right-hand side
     * of an {@link AbstractSubqueryJoin}) holds an {@link EsRelation} in time-series index mode.
     * <p>
     * Traversal stops at {@link UnionAll} boundaries so that a {@code TS} source nested inside a {@code FROM}
     * subquery (e.g. {@code FROM (TS k8s), (FROM sample_data)}) does not allow time-series aggregate functions
     * in the outer {@code STATS}: the {@code TS} relation is isolated inside an independent subquery and does
     * not grant TS semantics to this aggregate.
     */
    private static boolean hasTimeSeriesSource(LogicalPlan plan) {
        if (plan instanceof EsRelation er && er.indexMode().isTsdb()) {
            return true;
        }
        if (plan instanceof UnionAll) {
            return false;
        }
        if (plan instanceof AbstractSubqueryJoin join) {
            return hasTimeSeriesSource(join.left());
        }
        for (LogicalPlan child : plan.children()) {
            if (hasTimeSeriesSource(child)) {
                return true;
            }
        }
        return false;
    }

    private void checkMultipleScoreAggregations(Failures failures) {
        Holder<Boolean> hasScoringAggs = new Holder<>();
        forEachExpression(FilteredExpression.class, fe -> {
            if (fe.delegate() instanceof AggregateFunction aggregateFunction) {
                if (aggregateFunction.field() instanceof MetadataAttribute metadataAttribute) {
                    if (MetadataAttribute.SCORE.equals(metadataAttribute.name())) {
                        if (fe.filter().anyMatch(e -> e instanceof FullTextFunction)) {
                            failures.add(fail(fe, "cannot use _score aggregations with a WHERE filter in a STATS command"));
                        }
                    }
                }
            }
        });
    }

    /**
     * Check CATEGORIZE grouping function usages.
     * <p>
     *     Some of those checks are temporary, until the required syntax or engine changes are implemented.
     * </p>
     */
    private void checkCategorizeGrouping(Failures failures) {
        // Forbid CATEGORIZE grouping function with other groupings
        if (groupings.size() > 1) {
            groupings.subList(1, groupings.size()).forEach(g -> {
                g.forEachDown(
                    Categorize.class,
                    categorize -> failures.add(
                        fail(
                            categorize,
                            "CATEGORIZE grouping function [{}] can only be in the first grouping expression",
                            categorize.sourceText()
                        )
                    )
                );
            });
        }

        // Forbid CATEGORIZE grouping functions not being top level groupings
        groupings.forEach(g -> {
            // Check all CATEGORIZE but the top level one
            Alias.unwrap(g)
                .children()
                .forEach(
                    child -> child.forEachDown(
                        Categorize.class,
                        c -> failures.add(
                            fail(c, "CATEGORIZE grouping function [{}] can't be used within other expressions", c.sourceText())
                        )
                    )
                );
        });

        // Forbid CATEGORIZE being used in the aggregations, unless it appears as a grouping
        aggregates.forEach(
            a -> a.forEachDown(
                AggregateFunction.class,
                aggregateFunction -> aggregateFunction.forEachDown(
                    Categorize.class,
                    categorize -> failures.add(
                        fail(categorize, "cannot use CATEGORIZE grouping function [{}] within an aggregation", categorize.sourceText())
                    )
                )
            )
        );

        // Forbid CATEGORIZE being referenced as a child of an aggregation function
        AttributeMap.Builder<Categorize> categorizeByAttributeBuilder = AttributeMap.builder();
        groupings.forEach(g -> {
            g.forEachDown(Alias.class, alias -> {
                if (alias.child() instanceof Categorize categorize) {
                    categorizeByAttributeBuilder.put(alias.toAttribute(), categorize);
                }
            });
        });
        AttributeMap<Categorize> categorizeByAttribute = categorizeByAttributeBuilder.build();
        aggregates.forEach(a -> a.forEachDown(AggregateFunction.class, aggregate -> aggregate.forEachDown(Attribute.class, attribute -> {
            var categorize = categorizeByAttribute.get(attribute);
            if (categorize != null) {
                failures.add(
                    fail(attribute, "cannot reference CATEGORIZE grouping function [{}] within an aggregation", attribute.sourceText())
                );
            }
        })));
        aggregates.forEach(a -> a.forEachDown(FilteredExpression.class, fe -> fe.filter().forEachDown(Attribute.class, attribute -> {
            var categorize = categorizeByAttribute.get(attribute);
            if (categorize != null) {
                failures.add(
                    fail(
                        attribute,
                        "cannot reference CATEGORIZE grouping function [{}] within an aggregation filter",
                        attribute.sourceText()
                    )
                );
            }
        })));
    }

    // traverse the expression and look either for an agg function or a grouping match
    // stop either when no children are left, the leafs are literals or a reference attribute is given
    private void checkInvalidNamedExpressionUsage(
        Expression e,
        List<Expression> groups,
        AttributeSet groupRefs,
        Failures failures,
        int level
    ) {
        // unwrap filtered expression
        if (e instanceof FilteredExpression fe) {
            e = fe.delegate();
            // make sure they work on aggregate functions
            if (e.anyMatch(AggregateFunction.class::isInstance) == false) {
                Expression filter = fe.filter();
                failures.add(fail(filter, "WHERE clause allowed only for aggregate functions, none found in [{}]", fe.sourceText()));
            }
            Expression f = fe.filter();
            // check filter's data type
            checkFilterConditionDataType(f, failures);
            // but that the filter doesn't use grouping or aggregate functions
            fe.filter().forEachDown(c -> {
                if (c instanceof AggregateFunction af) {
                    failures.add(
                        fail(af, "cannot use aggregate function [{}] in aggregate WHERE clause [{}]", af.sourceText(), fe.sourceText())
                    );
                }
                // check the grouping function against the group
                else if (c instanceof GroupingFunction gf) {
                    if (c instanceof Categorize
                        || Expressions.anyMatch(groups, ex -> ex instanceof Alias a && a.child().semanticEquals(gf)) == false) {
                        failures.add(fail(gf, "can only use grouping function [{}] as part of the BY clause", gf.sourceText()));
                    }
                }
            });
        }
        // found an aggregate, constant or a group, bail out
        if (e instanceof AggregateFunction af && af instanceof Sparkline == false) {
            Consumer<Expression> checkNested = arg -> arg.forEachDown(AggregateFunction.class, f -> {
                if (f instanceof TimeSeriesAggregateFunction == false) {
                    failures.add(fail(f, "nested aggregations [{}] not allowed inside other aggregations [{}]", f, af));
                }
            });
            checkNested.accept(af.field());
            af.parameters().forEach(checkNested);
        } else if (e instanceof GroupingFunction gf) {
            // optimizer will later unroll expressions with aggs and non-aggs with a grouping function into an EVAL, but that will no longer
            // be verified (by check above in checkAggregate()), so do it explicitly here
            if (Expressions.anyMatch(groups, ex -> ex instanceof Alias a && a.child().semanticEquals(gf)) == false) {
                failures.add(fail(gf, "can only use grouping function [{}] as part of the BY clause", gf.sourceText()));
            } else if (level == 0) {
                addFailureOnGroupingUsedNakedInAggs(failures, gf, "function");
            }
        } else if (e.foldable()) {
            // don't do anything
        } else if (groups.contains(e) || groupRefs.contains(e)) {
            if (level == 0) {
                // TODO: remove this if statement once TS translation is moved to analyzer
                if ((this instanceof TimeSeriesAggregate ts && ts.origin() == TimeSeriesAggregate.Origin.PROMQL_COMMAND) == false) {
                    addFailureOnGroupingUsedNakedInAggs(failures, e, "key");
                }
            }
        }
        // if a reference is found, mark it as an error
        else if (e instanceof NamedExpression ne) {
            boolean foundInGrouping = false;
            for (Expression g : groups) {
                if (g.anyMatch(se -> se.semanticEquals(ne))) {
                    foundInGrouping = true;
                    failures.add(
                        fail(
                            e,
                            "column [{}] cannot be used as an aggregate once declared in the STATS BY grouping key [{}]",
                            ne.name(),
                            g.sourceText()
                        )
                    );
                    break;
                }
            }
            // TimeSeriesAggregates allow bare named expressions as they are implicitly wrapped in a time series aggregate function
            if (foundInGrouping == false && (this instanceof TimeSeriesAggregate) == false) {
                Holder<Boolean> foundTsAgg = new Holder<>(Boolean.FALSE);
                forEachDown(TimeSeriesAggregate.class, ts -> { foundTsAgg.set(Boolean.TRUE); });
                if (foundTsAgg.get() == false) {
                    failures.add(fail(e, "column [{}] must appear in the STATS BY clause or be used in an aggregate function", ne.name()));
                }
            }
        } else if (e instanceof Sparkline) {
            // don't do anything
        }
        // other keep on going
        else {
            for (Expression child : e.children()) {
                checkInvalidNamedExpressionUsage(child, groups, groupRefs, failures, level + 1);
            }
        }
    }

    private static void addFailureOnGroupingUsedNakedInAggs(Failures failures, Expression e, String element) {
        failures.add(
            fail(e, "grouping {} [{}] cannot be used as an aggregate once declared in the STATS BY clause", element, e.sourceText())
        );
    }
}
