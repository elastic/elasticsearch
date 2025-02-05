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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
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

public class Aggregate extends UnaryPlan implements PostAnalysisVerificationAware, TelemetryAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Aggregate",
        Aggregate::new
    );

    public enum AggregateType {
        STANDARD,
        // include metrics aggregates such as rates
        METRICS;

        static void writeType(StreamOutput out, AggregateType type) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                out.writeString(type.name());
            } else if (type != STANDARD) {
                throw new IllegalStateException("cluster is not ready to support aggregate type [" + type + "]");
            }
        }

        static AggregateType readType(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                return AggregateType.valueOf(in.readString());
            } else {
                return STANDARD;
            }
        }
    }

    private final AggregateType aggregateType;
    private final List<Expression> groupings;
    private final List<? extends NamedExpression> aggregates;

    private List<Attribute> lazyOutput;

    public Aggregate(
        Source source,
        LogicalPlan child,
        AggregateType aggregateType,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates
    ) {
        super(source, child);
        this.aggregateType = aggregateType;
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    public Aggregate(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            AggregateType.readType(in),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        AggregateType.writeType(out, aggregateType());
        out.writeNamedWriteableCollection(groupings);
        out.writeNamedWriteableCollection(aggregates());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Aggregate> info() {
        return NodeInfo.create(this, Aggregate::new, child(), aggregateType, groupings, aggregates);
    }

    @Override
    public Aggregate replaceChild(LogicalPlan newChild) {
        return new Aggregate(source(), newChild, aggregateType, groupings, aggregates);
    }

    public Aggregate with(List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return with(child(), newGroupings, newAggregates);
    }

    public Aggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new Aggregate(source(), child, aggregateType(), newGroupings, newAggregates);
    }

    public AggregateType aggregateType() {
        return aggregateType;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    @Override
    public String telemetryLabel() {
        return switch (aggregateType) {
            case STANDARD -> "STATS";
            case METRICS -> "METRICS";
        };
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
        AttributeSet result = Expressions.references(groupings).combine(Expressions.references(aggregates));
        for (Expression grouping : groupings) {
            if (grouping instanceof Alias) {
                result.remove(((Alias) grouping).toAttribute());
            }
        }
        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregateType, groupings, aggregates, child());
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
        return aggregateType == other.aggregateType
            && Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(child(), other.child());
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        AttributeSet groupRefs = new AttributeSet();
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
                groupRefs.add(attr);
            }
            if (e instanceof FieldAttribute f && f.dataType().isCounter()) {
                failures.add(fail(e, "cannot group by on [{}] type for grouping [{}]", f.dataType().typeName(), e.sourceText()));
            }
        });

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
        if (aggregateType() == Aggregate.AggregateType.METRICS) {
            aggregates.forEach(a -> checkRateAggregates(a, 0, failures));
        } else {
            forEachExpression(
                Rate.class,
                r -> failures.add(fail(r, "the rate aggregate[{}] can only be used within the metrics command", r.sourceText()))
            );
        }
        checkCategorizeGrouping(failures);

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
        AttributeMap<Categorize> categorizeByAttribute = new AttributeMap<>();
        groupings.forEach(g -> {
            g.forEachDown(Alias.class, alias -> {
                if (alias.child() instanceof Categorize categorize) {
                    categorizeByAttribute.put(alias.toAttribute(), categorize);
                }
            });
        });
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
                    fail(
                        expr,
                        "the rate aggregate [{}] can only be used within the metrics command and inside another aggregate",
                        r.sourceText()
                    )
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
                if (f instanceof Rate == false) {
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
