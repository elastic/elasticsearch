/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
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
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;
import static org.elasticsearch.xpack.esql.plan.logical.Filter.checkFilterConditionDataType;

public class Aggregate extends UnaryPlan implements PostAnalysisVerificationAware, TelemetryAware, SortAgnostic {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Aggregate",
        Aggregate::new
    );

    protected final List<Expression> groupings;
    protected final List<? extends NamedExpression> aggregates;

    private final List<Order> order;
    @Nullable
    private final Expression limit;

    protected List<Attribute> lazyOutput;

    public Aggregate(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
        this.order = List.of();
        this.limit = null;
    }

    public Aggregate(
        Source source,
        LogicalPlan child,
        List<Expression> groupings, List<? extends NamedExpression> aggregates,
        List<Order> order,
        @Nullable Expression limit
    ) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
        this.order = order;
        this.limit = limit;
    }

    public Aggregate(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class));
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)
            && in.getTransportVersion().before(TransportVersions.ESQL_REMOVE_AGGREGATE_TYPE)) {
            in.readString();
        }
        this.groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        this.aggregates = in.readNamedWriteableCollectionAsList(NamedExpression.class);
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_TOP_N_AGGREGATES)) {
            this.order = in.readCollectionAsList(Order::new);
            this.limit = in.readOptionalNamedWriteable(Expression.class);
        } else {
            this.order = emptyList();
            this.limit = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)
            && out.getTransportVersion().before(TransportVersions.ESQL_REMOVE_AGGREGATE_TYPE)) {
            out.writeString("STANDARD");
        }
        out.writeNamedWriteableCollection(groupings);
        out.writeNamedWriteableCollection(aggregates());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_TOP_N_AGGREGATES)) {
            out.writeCollection(order);
            out.writeOptionalNamedWriteable(limit);
        }
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
        return new Aggregate(source(), newChild, groupings, aggregates, order, limit);
    }

    public Aggregate with(List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return with(child(), newGroupings, newAggregates);
    }

    public Aggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new Aggregate(source(), child, newGroupings, newAggregates, order, limit);
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    public List<Order> order() {
        return order;
    }

    public Expression limit() {
        return limit;
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
            if (e instanceof FieldAttribute f && f.dataType().isCounter()) {
                failures.add(fail(e, "cannot group by on [{}] type for grouping [{}]", f.dataType().typeName(), e.sourceText()));
            }
        });
        var groupRefs = groupRefsBuilder.build();

        // check aggregates - accept only aggregate functions or expressions over grouping
        // don't allow the group by itself to avoid duplicates in the output
        // and since the groups are copied, only look at the declared aggregates
        // List<? extends NamedExpression> aggs = agg.aggregates();
        aggregates.subList(0, aggregates.size() - groupings.size()).forEach(e -> {
            var exp = Alias.unwrap(e);
            if (exp.foldable()) {
                failures.add(fail(exp, "expected an aggregate function but found [{}]", exp.sourceText()));
            }
            // traverse the tree to find invalid matches
            checkInvalidNamedExpressionUsage(exp, groupings, groupRefs, failures, 0);
        });
        if (anyMatch(l -> l instanceof EsRelation relation && relation.indexMode() == IndexMode.TIME_SERIES)) {
            aggregates.forEach(a -> checkRateAggregates(a, 0, failures));
        } else {
            forEachExpression(
                TimeSeriesAggregateFunction.class,
                r -> failures.add(fail(r, "time_series aggregate[{}] can only be used with the TS command", r.sourceText()))
            );
        }
        checkCategorizeGrouping(failures);
        checkMultipleScoreAggregations(failures);
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

    private static void checkRateAggregates(Expression expr, int nestedLevel, Failures failures) {
        if (expr instanceof AggregateFunction) {
            nestedLevel++;
        }
        if (expr instanceof Rate r) {
            if (nestedLevel != 2) {
                failures.add(
                    fail(expr, "the rate aggregate [{}] can only be used with the TS command and inside another aggregate", r.sourceText())
                );
            }
        }
        for (Expression child : expr.children()) {
            checkRateAggregates(child, nestedLevel, failures);
        }
    }

    // traverse the expression and look either for an agg function or a grouping match
    // stop either when no children are left, the leafs are literals or a reference attribute is given
    private static void checkInvalidNamedExpressionUsage(
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
        if (e instanceof AggregateFunction af) {
            af.field().forEachDown(AggregateFunction.class, f -> {
                // rate aggregate is allowed to be inside another aggregate
                if (f instanceof TimeSeriesAggregateFunction == false) {
                    failures.add(fail(f, "nested aggregations [{}] not allowed inside other aggregations [{}]", f, af));
                }
            });
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
                addFailureOnGroupingUsedNakedInAggs(failures, e, "key");
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
            if (foundInGrouping == false) {
                failures.add(fail(e, "column [{}] must appear in the STATS BY clause or be used in an aggregate function", ne.name()));
            }
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
