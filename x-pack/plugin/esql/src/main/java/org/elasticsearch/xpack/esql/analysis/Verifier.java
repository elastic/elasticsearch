/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.stats.FeatureMetric;
import org.elasticsearch.xpack.esql.stats.Metrics;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer.PushFiltersToSource.canPushToSource;

/**
 * This class is part of the planner. Responsible for failing impossible queries with a human-readable error message.  In particular, this
 * step does type resolution and fails queries based on invalid type expressions.
 */
public class Verifier {

    private final Metrics metrics;

    public Verifier(Metrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Verify that a {@link LogicalPlan} can be executed.
     *
     * @param plan The logical plan to be verified
     * @param partialMetrics a bitset indicating a certain command (or "telemetry feature") is present in the query
     * @return a collection of verification failures; empty if and only if the plan is valid
     */
    Collection<Failure> verify(LogicalPlan plan, BitSet partialMetrics) {
        assert partialMetrics != null;
        Set<Failure> failures = new LinkedHashSet<>();
        // alias map, collected during the first iteration for better error messages
        AttributeMap<Expression> aliases = new AttributeMap<>();

        // quick verification for unresolved attributes
        plan.forEachUp(p -> {
            // if the children are unresolved, so will this node; counting it will only add noise
            if (p.childrenResolved() == false) {
                return;
            }

            if (p instanceof Unresolvable u) {
                failures.add(fail(p, u.unresolvedMessage()));
            }
            // p is resolved, skip
            else if (p.resolved()) {
                p.forEachExpressionUp(Alias.class, a -> aliases.put(a.toAttribute(), a.child()));
                return;
            }

            Consumer<Expression> unresolvedExpressions = e -> {
                // everything is fine, skip expression
                if (e.resolved()) {
                    return;
                }

                e.forEachUp(ae -> {
                    // we're only interested in the children
                    if (ae.childrenResolved() == false) {
                        return;
                    }

                    if (ae instanceof Unresolvable u) {
                        // special handling for Project and unsupported types
                        if (p instanceof Project == false || u instanceof UnsupportedAttribute == false) {
                            failures.add(fail(ae, u.unresolvedMessage()));
                        }
                    }
                    if (ae.typeResolved().unresolved()) {
                        failures.add(fail(ae, ae.typeResolved().message()));
                    }
                });
            };

            // aggregates duplicate grouping inside aggs - to avoid potentially confusing messages, we only check the aggregates
            if (p instanceof Aggregate agg) {
                // do groupings first
                var groupings = agg.groupings();
                groupings.forEach(unresolvedExpressions);
                // followed by just the aggregates (to avoid going through the groups again)
                var aggs = agg.aggregates();
                int size = aggs.size() - groupings.size();
                aggs.subList(0, size).forEach(unresolvedExpressions);
            }
            // similar approach for Lookup
            else if (p instanceof Lookup lookup) {
                // first check the table
                var tableName = lookup.tableName();
                if (tableName instanceof Unresolvable u) {
                    failures.add(fail(tableName, u.unresolvedMessage()));
                }
                // only after that check the match fields
                else {
                    lookup.matchFields().forEach(unresolvedExpressions);
                }
            }

            else {
                p.forEachExpression(unresolvedExpressions);
            }
        });

        // in case of failures bail-out as all other checks will be redundant
        if (failures.isEmpty() == false) {
            return failures;
        }

        // Concrete verifications
        plan.forEachDown(p -> {
            // if the children are unresolved, so will this node; counting it will only add noise
            if (p.childrenResolved() == false) {
                return;
            }
            checkFilterConditionType(p, failures);
            checkAggregate(p, failures);
            checkRegexExtractOnlyOnStrings(p, failures);

            checkRow(p, failures);
            checkEvalFields(p, failures);

            checkOperationsOnUnsignedLong(p, failures);
            checkBinaryComparison(p, failures);
            checkForSortOnSpatialTypes(p, failures);

            checkFilterMatchConditions(p, failures);
        });
        checkRemoteEnrich(plan, failures);

        // gather metrics
        if (failures.isEmpty()) {
            gatherMetrics(plan, partialMetrics);
        }

        return failures;
    }

    private static void checkFilterConditionType(LogicalPlan p, Set<Failure> localFailures) {
        if (p instanceof Filter f) {
            Expression condition = f.condition();
            if (condition.dataType() != BOOLEAN) {
                localFailures.add(fail(condition, "Condition expression needs to be boolean, found [{}]", condition.dataType()));
            }
        }
    }

    private static void checkAggregate(LogicalPlan p, Set<Failure> failures) {
        if (p instanceof Aggregate agg) {
            List<Expression> groupings = agg.groupings();
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
            List<? extends NamedExpression> aggs = agg.aggregates();
            aggs.subList(0, aggs.size() - groupings.size()).forEach(e -> {
                var exp = Alias.unwrap(e);
                if (exp.foldable()) {
                    failures.add(fail(exp, "expected an aggregate function but found [{}]", exp.sourceText()));
                }
                // traverse the tree to find invalid matches
                checkInvalidNamedExpressionUsage(exp, groupings, groupRefs, failures, 0);
            });
            if (agg.aggregateType() == Aggregate.AggregateType.METRICS) {
                aggs.forEach(a -> checkRateAggregates(a, 0, failures));
            } else {
                agg.forEachExpression(
                    Rate.class,
                    r -> failures.add(fail(r, "the rate aggregate[{}] can only be used within the metrics command", r.sourceText()))
                );
            }
        } else {
            p.forEachExpression(
                GroupingFunction.class,
                gf -> failures.add(fail(gf, "cannot use grouping function [{}] outside of a STATS command", gf.sourceText()))
            );
        }
    }

    private static void checkRateAggregates(Expression expr, int nestedLevel, Set<Failure> failures) {
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
        Set<Failure> failures,
        int level
    ) {
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
            if (groups.stream().anyMatch(ex -> ex instanceof Alias a && a.child().semanticEquals(gf)) == false) {
                failures.add(fail(gf, "can only use grouping function [{}] part of the BY clause", gf.sourceText()));
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

    private static void addFailureOnGroupingUsedNakedInAggs(Set<Failure> failures, Expression e, String element) {
        failures.add(
            fail(e, "grouping {} [{}] cannot be used as an aggregate once declared in the STATS BY clause", element, e.sourceText())
        );
    }

    private static void checkRegexExtractOnlyOnStrings(LogicalPlan p, Set<Failure> failures) {
        if (p instanceof RegexExtract re) {
            Expression expr = re.input();
            DataType type = expr.dataType();
            if (DataType.isString(type) == false) {
                failures.add(
                    fail(
                        expr,
                        "{} only supports KEYWORD or TEXT values, found expression [{}] type [{}]",
                        re.getClass().getSimpleName(),
                        expr.sourceText(),
                        type
                    )
                );
            }
        }
    }

    private static void checkRow(LogicalPlan p, Set<Failure> failures) {
        if (p instanceof Row row) {
            row.fields().forEach(a -> {
                if (DataType.isRepresentable(a.dataType()) == false) {
                    failures.add(fail(a, "cannot use [{}] directly in a row assignment", a.child().sourceText()));
                }
            });
        }
    }

    private static void checkEvalFields(LogicalPlan p, Set<Failure> failures) {
        if (p instanceof Eval eval) {
            eval.fields().forEach(field -> {
                // check supported types
                DataType dataType = field.dataType();
                if (DataType.isRepresentable(dataType) == false) {
                    failures.add(
                        fail(field, "EVAL does not support type [{}] in expression [{}]", dataType.typeName(), field.child().sourceText())
                    );
                }
                // check no aggregate functions are used
                field.forEachDown(AggregateFunction.class, af -> {
                    if (af instanceof Rate) {
                        failures.add(fail(af, "aggregate function [{}] not allowed outside METRICS command", af.sourceText()));
                    } else {
                        failures.add(fail(af, "aggregate function [{}] not allowed outside STATS command", af.sourceText()));
                    }
                });
                // check no MATCH expressions are used
                field.forEachDown(
                    MatchQueryPredicate.class,
                    mqp -> { failures.add(fail(mqp, "EVAL does not support MATCH expressions")); }
                );
            });
        }
    }

    private static void checkOperationsOnUnsignedLong(LogicalPlan p, Set<Failure> failures) {
        p.forEachExpression(e -> {
            Failure f = null;

            if (e instanceof BinaryOperator<?, ?, ?, ?> bo) {
                f = validateUnsignedLongOperator(bo);
            } else if (e instanceof Neg neg) {
                f = validateUnsignedLongNegation(neg);
            }

            if (f != null) {
                failures.add(f);
            }
        });
    }

    private static void checkBinaryComparison(LogicalPlan p, Set<Failure> failures) {
        p.forEachExpression(BinaryComparison.class, bc -> {
            Failure f = validateBinaryComparison(bc);
            if (f != null) {
                failures.add(f);
            }
        });
    }

    private void gatherMetrics(LogicalPlan plan, BitSet b) {
        plan.forEachDown(p -> FeatureMetric.set(p, b));
        for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i + 1)) {
            metrics.inc(FeatureMetric.values()[i]);
        }
    }

    /**
     * Limit QL's comparisons to types we support.
     */
    public static Failure validateBinaryComparison(BinaryComparison bc) {
        if (bc.left().dataType().isNumeric()) {
            if (false == bc.right().dataType().isNumeric()) {
                return fail(
                    bc,
                    "first argument of [{}] is [numeric] so second argument must also be [numeric] but was [{}]",
                    bc.sourceText(),
                    bc.right().dataType().typeName()
                );
            }
            return null;
        }

        List<DataType> allowed = new ArrayList<>();
        allowed.add(DataType.KEYWORD);
        allowed.add(DataType.TEXT);
        allowed.add(DataType.IP);
        allowed.add(DataType.DATETIME);
        allowed.add(DataType.VERSION);
        allowed.add(DataType.GEO_POINT);
        allowed.add(DataType.GEO_SHAPE);
        allowed.add(DataType.CARTESIAN_POINT);
        allowed.add(DataType.CARTESIAN_SHAPE);
        if (bc instanceof Equals || bc instanceof NotEquals) {
            allowed.add(DataType.BOOLEAN);
        }
        Expression.TypeResolution r = TypeResolutions.isType(
            bc.left(),
            allowed::contains,
            bc.sourceText(),
            FIRST,
            Stream.concat(Stream.of("numeric"), allowed.stream().map(DataType::typeName)).toArray(String[]::new)
        );
        if (false == r.resolved()) {
            return fail(bc, r.message());
        }
        if (DataType.isString(bc.left().dataType()) && DataType.isString(bc.right().dataType())) {
            return null;
        }
        if (bc.left().dataType() != bc.right().dataType()) {
            return fail(
                bc,
                "first argument of [{}] is [{}] so second argument must also be [{}] but was [{}]",
                bc.sourceText(),
                bc.left().dataType().typeName(),
                bc.left().dataType().typeName(),
                bc.right().dataType().typeName()
            );
        }
        return null;
    }

    /** Ensure that UNSIGNED_LONG types are not implicitly converted when used in arithmetic binary operator, as this cannot be done since:
     *  - unsigned longs are passed through the engine as longs, so/and
     *  - negative values cannot be represented (i.e. range [Long.MIN_VALUE, "abs"(Long.MIN_VALUE) + Long.MAX_VALUE] won't fit on 64 bits);
     *  - a conversion to double isn't possible, since upper range UL values can no longer be distinguished
     *  ex: (double) 18446744073709551615 == (double) 18446744073709551614
     *  - the implicit ESQL's Cast doesn't currently catch Exception and nullify the result.
     *  Let the user handle the operation explicitly.
     */
    public static Failure validateUnsignedLongOperator(BinaryOperator<?, ?, ?, ?> bo) {
        DataType leftType = bo.left().dataType();
        DataType rightType = bo.right().dataType();
        if ((leftType == DataType.UNSIGNED_LONG || rightType == DataType.UNSIGNED_LONG) && leftType != rightType) {
            return fail(
                bo,
                "first argument of [{}] is [{}] and second is [{}]. [{}] can only be operated on together with another [{}]",
                bo.sourceText(),
                leftType.typeName(),
                rightType.typeName(),
                DataType.UNSIGNED_LONG.typeName(),
                DataType.UNSIGNED_LONG.typeName()
            );
        }
        return null;
    }

    /**
     * Negating an unsigned long is invalid.
     */
    private static Failure validateUnsignedLongNegation(Neg neg) {
        DataType childExpressionType = neg.field().dataType();
        if (childExpressionType.equals(DataType.UNSIGNED_LONG)) {
            return fail(
                neg,
                "negation unsupported for arguments of type [{}] in expression [{}]",
                childExpressionType.typeName(),
                neg.sourceText()
            );
        }
        return null;
    }

    /**
     * Makes sure that spatial types do not appear in sorting contexts.
     */
    private static void checkForSortOnSpatialTypes(LogicalPlan p, Set<Failure> localFailures) {
        if (p instanceof OrderBy ob) {
            ob.forEachExpression(Attribute.class, attr -> {
                DataType dataType = attr.dataType();
                if (DataType.isSpatial(dataType)) {
                    localFailures.add(fail(attr, "cannot sort on " + dataType.typeName()));
                }
            });
        }
    }

    /**
     * Ensure that no remote enrich is allowed after a reduction or an enrich with coordinator mode.
     * <p>
     * TODO:
     * For Limit and TopN, we can insert the same node after the remote enrich (also needs to move projections around)
     * to eliminate this limitation. Otherwise, we force users to write queries that might not perform well.
     * For example, `FROM test | ORDER @timestamp | LIMIT 10 | ENRICH _remote:` doesn't work.
     * In that case, users have to write it as `FROM test | ENRICH _remote: | ORDER @timestamp | LIMIT 10`,
     * which is equivalent to bringing all data to the coordinating cluster.
     * We might consider implementing the actual remote enrich on the coordinating cluster, however, this requires
     * retaining the originating cluster and restructing pages for routing, which might be complicated.
     */
    private static void checkRemoteEnrich(LogicalPlan plan, Set<Failure> failures) {
        boolean[] agg = { false };
        boolean[] limit = { false };
        boolean[] enrichCoord = { false };

        plan.forEachUp(UnaryPlan.class, u -> {
            if (u instanceof Limit) {
                limit[0] = true; // TODO: Make Limit then enrich_remote work
            }
            if (u instanceof Aggregate) {
                agg[0] = true;
            } else if (u instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                enrichCoord[0] = true;
            }
            if (u instanceof Enrich enrich && enrich.mode() == Enrich.Mode.REMOTE) {
                if (limit[0]) {
                    failures.add(fail(enrich, "ENRICH with remote policy can't be executed after LIMIT"));
                }
                if (agg[0]) {
                    failures.add(fail(enrich, "ENRICH with remote policy can't be executed after STATS"));
                }
                if (enrichCoord[0]) {
                    failures.add(fail(enrich, "ENRICH with remote policy can't be executed after another ENRICH with coordinator policy"));
                }
            }
        });
    }

    /**
     * Currently any filter condition using MATCH needs to be pushed down to the Lucene query.
     * Conditions that use a combination of MATCH and ES|QL functions (e.g. `title MATCH "anna" OR DATE_EXTRACT("year", date) > 2010)
     * cannot be pushed down to Lucene.
     * Another condition is for MATCH to use index fields that have been mapped as text or keyword.
     * We are using canPushToSource at the Verifier level because we want to detect any condition that cannot be pushed down
     * early in the execution, rather than fail at the compute engine level.
     * In the future we will be able to handle MATCH at the compute and we will no longer need these checks.
     */
    private static void checkFilterMatchConditions(LogicalPlan plan, Set<Failure> failures) {
        if (plan instanceof Filter f) {
            Expression condition = f.condition();

            Holder<Boolean> hasMatch = new Holder<>(false);
            condition.forEachDown(MatchQueryPredicate.class, mqp -> {
                hasMatch.set(true);
                var field = mqp.field();
                if (field instanceof FieldAttribute == false) {
                    failures.add(fail(mqp, "MATCH requires a mapped index field, found [" + field.sourceText() + "]"));
                }

                if (DataType.isString(field.dataType()) == false) {
                    var message = LoggerMessageFormat.format(
                        null,
                        "MATCH requires a text or keyword field, but [{}] has type [{}]",
                        field.sourceText(),
                        field.dataType().esType()
                    );
                    failures.add(fail(mqp, message));
                }
            });

            if (canPushToSource(condition, x -> false)) {
                return;
            }
            if (hasMatch.get()) {
                failures.add(fail(condition, "Invalid condition using MATCH"));
            }
        }
    }
}
