/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.Functions;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.FullTextPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.expression.Exists;
import org.elasticsearch.xpack.sql.expression.function.Score;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Kurtosis;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.NumericAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Skewness;
import org.elasticsearch.xpack.sql.expression.function.aggregate.TopHits;
import org.elasticsearch.xpack.sql.expression.function.Array;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.plan.logical.Distinct;
import org.elasticsearch.xpack.sql.plan.logical.Having;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.stats.FeatureMetric;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.ql.analyzer.VerifierChecks.checkFilterConditionType;
import static org.elasticsearch.xpack.ql.common.Failure.fail;
import static org.elasticsearch.xpack.ql.type.DataTypes.BINARY;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;
import static org.elasticsearch.xpack.sql.session.VersionCompatibilityChecks.isTypeSupportedInVersion;
import static org.elasticsearch.xpack.sql.session.VersionCompatibilityChecks.versionIntroducingType;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.COMMAND;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.GROUPBY;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.HAVING;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.LIMIT;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.LOCAL;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.ORDERBY;
import static org.elasticsearch.xpack.sql.stats.FeatureMetric.WHERE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.SHAPE;

/**
 * The verifier has the role of checking the analyzed tree for failures and build a list of failures following this check.
 * It is created in the plan executor along with the metrics instance passed as constructor parameter.
 */
public final class Verifier {
    private final Metrics metrics;
    private final SqlVersion version;

    public Verifier(Metrics metrics, SqlVersion version) {
        this.metrics = metrics;
        this.version = version;
    }

    public Map<Node<?>, String> verifyFailures(LogicalPlan plan) {
        Collection<Failure> failures = verify(plan);
        return failures.stream().collect(toMap(Failure::node, Failure::message));
    }

    Collection<Failure> verify(LogicalPlan plan) {
        Set<Failure> failures = new LinkedHashSet<>();

        // start bottom-up
        plan.forEachUp(p -> {
            if (p.analyzed()) {
                return;
            }

            // if the children are unresolved, so will this node; counting it will only add noise
            if (p.childrenResolved() == false) {
                return;
            }

            Set<Failure> localFailures = new LinkedHashSet<>();

            //
            // First handle usual suspects
            //

            if (p instanceof Unresolvable) {
                localFailures.add(fail(p, ((Unresolvable) p).unresolvedMessage()));
            } else if (p instanceof Distinct) {
                localFailures.add(fail(p, "SELECT DISTINCT is not yet supported"));
            } else {
                // then take a look at the expressions
                p.forEachExpression(e -> {
                    // everything is fine, skip expression
                    if (e.resolved()) {
                        return;
                    }

                    e.forEachUp(ae -> {
                        // we're only interested in the children
                        if (ae.childrenResolved() == false) {
                            return;
                        }
                        // again the usual suspects
                        if (ae instanceof Unresolvable) {
                            // handle Attributes different to provide more context
                            if (ae instanceof UnresolvedAttribute) {
                                UnresolvedAttribute ua = (UnresolvedAttribute) ae;
                                // only work out the synonyms for raw unresolved attributes
                                if (ua.customMessage() == false) {
                                    boolean useQualifier = ua.qualifier() != null;
                                    List<String> potentialMatches = new ArrayList<>();
                                    for (Attribute a : p.inputSet()) {
                                        String nameCandidate = useQualifier ? a.qualifiedName() : a.name();
                                        // add only primitives (object types would only result in another error)
                                        if (DataTypes.isUnsupported(a.dataType()) == false && DataTypes.isPrimitive(a.dataType())) {
                                            potentialMatches.add(nameCandidate);
                                        }
                                    }

                                    List<String> matches = StringUtils.findSimilar(ua.qualifiedName(), potentialMatches);
                                    if (matches.isEmpty() == false) {
                                        ae = ua.withUnresolvedMessage(UnresolvedAttribute.errorMessage(ua.qualifiedName(), matches));
                                    }
                                }
                            }

                            localFailures.add(fail(ae, ((Unresolvable) ae).unresolvedMessage()));
                            return;
                        }
                        // type resolution
                        if (ae.typeResolved().unresolved()) {
                            localFailures.add(fail(ae, ae.typeResolved().message()));
                        } else if (ae instanceof Exists) {
                            localFailures.add(fail(ae, "EXISTS is not yet supported"));
                        }
                    });
                });
            }
            failures.addAll(localFailures);
        });

        // Concrete verifications

        // if there are no (major) unresolved failures, do more in-depth analysis

        if (failures.isEmpty()) {
            Set<Failure> localFailures = new LinkedHashSet<>();
            AttributeMap.Builder<Expression> collectRefs = AttributeMap.builder();

            checkFullTextSearchInSelect(plan, localFailures);

            // collect Attribute sources
            // only Aliases are interesting since these are the only ones that hide expressions
            // FieldAttribute for example are self replicating.
            plan.forEachExpressionUp(Alias.class, a -> collectRefs.put(a.toAttribute(), a.child()));

            AttributeMap<Expression> attributeRefs = collectRefs.build();

            // for filtering out duplicated errors
            final Set<LogicalPlan> groupingFailures = new LinkedHashSet<>();

            plan.forEachDown(p -> {
                if (p.analyzed()) {
                    return;
                }

                // if the children are unresolved, so will this node; counting it will only add noise
                if (p.childrenResolved() == false) {
                    return;
                }

                checkFilterConditionType(p, localFailures);
                checkGroupingFunctionInGroupBy(p, localFailures);
                checkFilterOnAggs(p, localFailures, attributeRefs);
                checkFilterOnGrouping(p, localFailures, attributeRefs);

                checkNestedAggregation(p, localFailures, attributeRefs);

                if (groupingFailures.contains(p) == false) {
                    checkGroupBy(p, localFailures, attributeRefs, groupingFailures);
                }

                checkForRestrictedFunctionInsideFunction(p, Score.class, localFailures);
                checkNestedUsedInGroupByOrHavingOrWhereOrOrderBy(p, localFailures, attributeRefs);
                checkForGeoFunctionsOnDocValues(p, localFailures);
                checkPivot(p, localFailures, attributeRefs);
                checkMatrixStats(p, localFailures);
                checkCastOnInexact(p, localFailures);
                checkBinaryHasDocValues(p, localFailures);
                // restricted array usage
                checkForRestrictedFunctionInsideFunction(p, Array.class, localFailures);
                checkArrayFunctionUsedInWhereOrOrderByOrAggregate(p, localFailures);
                checkArrayFunctionArguments(p, localFailures);

                // restricted array usage
                checkForRestrictedFunctionInsideFunction(p, Array.class, localFailures);
                checkArrayFunctionUsedInWhereOrOrderByOrAggregate(p, localFailures);
                checkArrayFunctionArguments(p, localFailures);

                // everything checks out
                // mark the plan as analyzed
                if (localFailures.isEmpty()) {
                    p.setAnalyzed();
                }

                failures.addAll(localFailures);
            });

            checkClientSupportsDataTypes(plan, failures, version);
        }

        // gather metrics
        if (failures.isEmpty()) {
            BitSet b = new BitSet(FeatureMetric.values().length);
            plan.forEachDown(p -> {
                if (p instanceof Aggregate) {
                    b.set(GROUPBY.ordinal());
                } else if (p instanceof OrderBy) {
                    b.set(ORDERBY.ordinal());
                } else if (p instanceof Filter) {
                    if (((Filter) p).child() instanceof Aggregate) {
                        b.set(HAVING.ordinal());
                    } else {
                        b.set(WHERE.ordinal());
                    }
                } else if (p instanceof Limit) {
                    b.set(LIMIT.ordinal());
                } else if (p instanceof LocalRelation) {
                    b.set(LOCAL.ordinal());
                } else if (p instanceof Command) {
                    b.set(COMMAND.ordinal());
                }
            });
            for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i + 1)) {
                metrics.inc(FeatureMetric.values()[i]);
            }
        }

        return failures;
    }

    private void checkNestedAggregation(LogicalPlan p, Set<Failure> localFailures, AttributeMap<Expression> attributeRefs) {
        if (p instanceof Aggregate) {
            ((Aggregate) p).child()
                .forEachDown(
                    Aggregate.class,
                    a -> { localFailures.add(fail(a, "Nested aggregations in sub-selects are not supported.")); }
                );
        }
    }

    private void checkFullTextSearchInSelect(LogicalPlan plan, Set<Failure> localFailures) {
        plan.forEachUp(Project.class, p -> {
            for (NamedExpression ne : p.projections()) {
                ne.forEachUp(FullTextPredicate.class, (e) ->
                    localFailures.add(fail(e, "Cannot use MATCH() or QUERY() full-text search " +
                        "functions in the SELECT clause"))
                );
            }
        });
    }

    /**
     * Check validity of Aggregate/GroupBy.
     * This rule is needed for multiple reasons:
     * 1. a user might specify an invalid aggregate (SELECT foo GROUP BY bar)
     * 2. the ORDER BY/HAVING might contain a non-grouped attribute. This is typically
     * caught by the Analyzer however if wrapped in a function (ABS()) it gets resolved
     * (because the expression gets resolved little by little without being pushed down,
     * without the Analyzer modifying anything.
     * 2a. HAVING also requires an Aggregate function
     * 3. composite agg (used for GROUP BY) allows ordering only on the group keys
     */
    private static boolean checkGroupBy(LogicalPlan p, Set<Failure> localFailures, AttributeMap<Expression> attributeRefs,
            Set<LogicalPlan> groupingFailures) {
        return checkGroupByInexactField(p, localFailures)
                && checkGroupByAgg(p, localFailures, attributeRefs)
                && checkGroupByOrder(p, localFailures, groupingFailures, attributeRefs)
                && checkGroupByHaving(p, localFailures, groupingFailures, attributeRefs)
                && checkGroupByTime(p, localFailures);
    }

    // check whether an orderBy failed or if it occurs on a non-key
    private static boolean checkGroupByOrder(LogicalPlan p, Set<Failure> localFailures, Set<LogicalPlan> groupingFailures,
            AttributeMap<Expression> attributeRefs) {
        if (p instanceof OrderBy) {
            OrderBy o = (OrderBy) p;
            LogicalPlan child = o.child();

            if (child instanceof Project) {
                child = ((Project) child).child();
            }
            if (child instanceof Filter) {
                child = ((Filter) child).child();
            }

            if (child instanceof Aggregate) {
                Aggregate a = (Aggregate) child;

                Map<Expression, Node<?>> missing = new LinkedHashMap<>();

                o.order().forEach(oe -> {
                    final Expression e = oe.child();
                    final Expression resolvedE = attributeRefs.resolve(e, e);

                    // aggregates are allowed
                    if (Functions.isAggregate(resolvedE)) {
                        return;
                    }

                    // take aliases declared inside the aggregates which point to the grouping (but are not included in there)
                    // to correlate them to the order
                    List<Expression> groupingAndMatchingAggregatesAliases = new ArrayList<>(a.groupings());

                    a.aggregates().forEach(as -> {
                        if (as instanceof Alias) {
                            Alias al = (Alias) as;
                            if (Expressions.anyMatch(a.groupings(), g -> Expressions.equalsAsAttribute(al.child(), g))) {
                                groupingAndMatchingAggregatesAliases.add(al);
                            }
                        }
                    });

                    // Make sure you can apply functions on top of the grouped by expressions in the ORDER BY:
                    // e.g.: if "GROUP BY f2(f1(field))" you can "ORDER BY f4(f3(f2(f1(field))))"
                    //
                    // Also, make sure to compare attributes directly
                    if (resolvedE.anyMatch(expression -> Expressions.anyMatch(groupingAndMatchingAggregatesAliases,
                        g -> {
                            Expression resolvedG = attributeRefs.resolve(g, g);
                            resolvedG = expression instanceof Attribute ? Expressions.attribute(resolvedG) : resolvedG;
                            return expression.semanticEquals(resolvedG);
                        }))) {
                        return;
                    }

                    // nothing matched, cannot group by it
                    missing.put(e, oe);
                });

                if (missing.isEmpty() == false) {
                    String plural = missing.size() > 1 ? "s" : StringUtils.EMPTY;
                    // get the location of the first missing expression as the order by might be on a different line
                    localFailures.add(
                            fail(missing.values().iterator().next(),
                                    "Cannot order by non-grouped column" + plural + " {}, expected {} or an aggregate function",
                                    Expressions.names(missing.keySet()),
                                    Expressions.names(a.groupings())));
                    groupingFailures.add(a);
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean checkGroupByHaving(LogicalPlan p, Set<Failure> localFailures,
            Set<LogicalPlan> groupingFailures, AttributeMap<Expression> attributeRefs) {
        if (p instanceof Having) {
            Having h = (Having) p;
            if (h.child() instanceof Aggregate) {
                Aggregate a = (Aggregate) h.child();

                Set<Expression> missing = new LinkedHashSet<>();
                Set<Expression> unsupported = new LinkedHashSet<>();
                Expression condition = h.condition();
                // variation of checkGroupMatch customized for HAVING, which requires just aggregations
                condition.collectFirstChildren(c -> checkGroupByHavingHasOnlyAggs(c, missing, unsupported, attributeRefs));

                if (missing.isEmpty() == false) {
                    String plural = missing.size() > 1 ? "s" : StringUtils.EMPTY;
                    localFailures.add(
                            fail(condition, "Cannot use HAVING filter on non-aggregate" + plural + " {}; use WHERE instead",
                            Expressions.names(missing)));
                    groupingFailures.add(a);
                    return false;
                }

                if (unsupported.isEmpty() == false) {
                    String plural = unsupported.size() > 1 ? "s" : StringUtils.EMPTY;
                    localFailures.add(
                        fail(condition, "HAVING filter is unsupported for function" + plural + " {}",
                            Expressions.names(unsupported)));
                    groupingFailures.add(a);
                    return false;
                }
            }
        }
        return true;
    }


    private static boolean checkGroupByHavingHasOnlyAggs(Expression e, Set<Expression> missing,
            Set<Expression> unsupported, AttributeMap<Expression> attributeRefs) {

        // resolve FunctionAttribute to backing functions
        if (e instanceof ReferenceAttribute) {
            e = attributeRefs.resolve(e);
        }

        // scalar functions can be a binary tree
        // first test the function against the grouping
        // and if that fails, start unpacking hoping to find matches
        if (e instanceof ScalarFunction) {
            ScalarFunction sf = (ScalarFunction) e;

            // unwrap function to find the base
            for (Expression arg : sf.arguments()) {
                arg.collectFirstChildren(c -> checkGroupByHavingHasOnlyAggs(c, missing, unsupported, attributeRefs));
            }
            return true;

        } else if (e instanceof Score) {
            // Score can't be used in having
            unsupported.add(e);
            return true;
        } else if (e instanceof TopHits) {
            // First and Last cannot be used in having
            unsupported.add(e);
            return true;
        } else if (e instanceof Min || e instanceof Max) {
            if (DataTypes.isString(((AggregateFunction) e).field().dataType())) {
                // Min & Max on a Keyword field will be translated to First & Last respectively
                unsupported.add(e);
                return true;
            }
        }

        // skip literals / foldable
        if (e.foldable()) {
            return true;
        }
        // skip aggs (allowed to refer to non-group columns)
        if (Functions.isAggregate(e) || Functions.isGrouping(e)) {
            return true;
        }

        // left without leaves which have to match; that's a failure since everything should be based on an agg
        if (e instanceof Attribute) {
            missing.add(e);
            return true;
        }

        return false;
    }

    private static boolean checkGroupByInexactField(LogicalPlan p, Set<Failure> localFailures) {
        if (p instanceof Aggregate) {
            return onlyExactFields(((Aggregate) p).groupings(), localFailures);
        }
        return true;
    }

    // The grouping can not be an aggregate function or an inexact field (e.g. text without a keyword)
    private static boolean onlyExactFields(List<Expression> expressions, Set<Failure> localFailures) {
        Holder<Boolean> onlyExact = new Holder<>(Boolean.TRUE);

        expressions.forEach(e -> e.forEachUp(FieldAttribute.class, c -> {
            EsField.Exact exact = c.getExactInfo();
            if (exact.hasExact() == false) {
                localFailures.add(fail(c, "Field [{}] of data type [{}] cannot be used for grouping; {}", c.sourceText(),
                    c.dataType().typeName(), exact.errorMsg()));
                onlyExact.set(Boolean.FALSE);
            }
        }));

        return onlyExact.get();
    }

    private static boolean onlyRawFields(Iterable<? extends Expression> expressions, Set<Failure> localFailures,
            AttributeMap<Expression> attributeRefs) {
        Holder<Boolean> onlyExact = new Holder<>(Boolean.TRUE);

        expressions.forEach(e -> e.forEachDown(c -> {
            if (c instanceof ReferenceAttribute) {
                c = attributeRefs.resolve(c, c);
            }
            if (c instanceof Function) {
                localFailures.add(fail(c, "No functions allowed (yet); encountered [{}]", c.sourceText()));
                onlyExact.set(Boolean.FALSE);
            }
        }));
        return onlyExact.get();
    }

    private static boolean checkGroupByTime(LogicalPlan p, Set<Failure> localFailures) {
        if (p instanceof Aggregate) {
            Aggregate a = (Aggregate) p;

            // TIME data type is not allowed for grouping key
            // https://github.com/elastic/elasticsearch/issues/40639
            a.groupings().forEach(f -> {
                if (f.dataType() == SqlDataTypes.TIME) {
                    localFailures.add(fail(f, "Function [" + f.sourceText() + "] with data type [" + f.dataType().typeName() +
                        "] " + "cannot be used for grouping"));
                }
            });
        }
        return true;
    }

    // check whether plain columns specified in an agg are mentioned in the group-by
    private static boolean checkGroupByAgg(LogicalPlan p, Set<Failure> localFailures, AttributeMap<Expression> attributeRefs) {
        if (p instanceof Aggregate) {
            Aggregate a = (Aggregate) p;

            // The grouping can not be an aggregate function
            a.groupings().forEach(e -> e.forEachUp(c -> {
                if (Functions.isAggregate(c)) {
                    localFailures.add(fail(c, "Cannot use an aggregate [" + c.nodeName().toUpperCase(Locale.ROOT) + "] for grouping"));
                }
                if (c instanceof Score) {
                    localFailures.add(fail(c, "Cannot use [SCORE()] for grouping"));
                }
            }));

            a.groupings().forEach(e -> {
                if (Functions.isGrouping(e) == false) {
                    e.collectFirstChildren(c -> {
                        if (Functions.isGrouping(c)) {
                            localFailures.add(fail(c,
                                    "Cannot combine [{}] grouping function inside GROUP BY, found [{}];"
                                            + " consider moving the expression inside the histogram",
                                    Expressions.name(c), Expressions.name(e)));
                            return true;
                        }
                        return false;
                    });
                }
            });

            if (localFailures.isEmpty() == false) {
                return false;
            }

            // The agg can be:
            // 1. plain column - in which case, there should be an equivalent in groupings
            // 2. aggregate over non-grouped column
            // 3. scalar function on top of 1 and/or 2. the function needs unfolding to make sure
            //    the 'source' is valid.

            // Note that grouping can be done by a function (GROUP BY YEAR(date)) which means date
            // cannot be used as a plain column, only YEAR(date) or aggs(?) on top of it

            Map<Expression, Node<?>> missing = new LinkedHashMap<>();
            a.aggregates().forEach(ne ->
                ne.collectFirstChildren(c -> checkGroupMatch(c, ne, a.groupings(), missing, attributeRefs)));

            if (missing.isEmpty() == false) {
                String plural = missing.size() > 1 ? "s" : StringUtils.EMPTY;
                localFailures.add(fail(missing.values().iterator().next(), "Cannot use non-grouped column" + plural + " {}, expected {}",
                        Expressions.names(missing.keySet()),
                        Expressions.names(a.groupings())));
                return false;
            }
        }

        return true;
    }

    private static boolean checkGroupMatch(Expression e, Node<?> source, List<Expression> groupings,
            Map<Expression, Node<?>> missing, AttributeMap<Expression> attributeRefs) {

        // 1:1 match
        if (Expressions.match(groupings, e::semanticEquals)) {
            return true;
        }

        // resolve FunctionAttribute to backing functions
        if (e instanceof ReferenceAttribute) {
            e = attributeRefs.resolve(e);
        }

        // scalar functions can be a binary tree
        // first test the function against the grouping
        // and if that fails, start unpacking hoping to find matches
        if (e instanceof ScalarFunction) {
            ScalarFunction sf = (ScalarFunction) e;

            // found group for the expression
            if (Expressions.anyMatch(groupings, e::semanticEquals)) {
                return true;
            }

            // unwrap function to find the base
            for (Expression arg : sf.arguments()) {
                arg.collectFirstChildren(c -> checkGroupMatch(c, source, groupings, missing, attributeRefs));
            }

            return true;
        } else if (e instanceof Score) {
            // Score can't be an aggregate function
            missing.put(e, source);
            return true;
        }

        // skip literals / foldable
        if (e.foldable()) {
            return true;
        }
        // skip aggs (allowed to refer to non-group columns)
        // TODO: need to check whether it's possible to agg on a field used inside a scalar for grouping
        if (Functions.isAggregate(e)) {
            return true;
        }

        // left without leaves which have to match; if not there's a failure
        // make sure to match directly on the expression and not on the tree
        // (since otherwise exp might match the function argument which would be incorrect)
        final Expression exp = e;
        if (e.children().isEmpty()) {
            if (Expressions.match(groupings, c -> exp.semanticEquals(exp instanceof Attribute ? Expressions.attribute(c) : c)) == false) {
                missing.put(exp, source);
            }
            return true;
        }
        return false;
    }

    private static void checkGroupingFunctionInGroupBy(LogicalPlan p, Set<Failure> localFailures) {
        // check if the query has a grouping function (Histogram) but no GROUP BY
        if (p instanceof Project) {
            Project proj = (Project) p;
            proj.projections().forEach(e -> e.forEachDown(GroupingFunction.class, f ->
                localFailures.add(fail(f, "[{}] needs to be part of the grouping", Expressions.name(f)))));
        }
        // if it does have a GROUP BY, check if the groupings contain the grouping functions (Histograms)
        else if (p instanceof Aggregate) {
            Aggregate a = (Aggregate) p;
            a.aggregates().forEach(agg -> agg.forEachDown(GroupingFunction.class, e -> {
                if (a.groupings().size() == 0
                    || Expressions.anyMatch(a.groupings(), g -> g instanceof Function && e.equals(g)) == false) {
                    localFailures.add(fail(e, "[{}] needs to be part of the grouping", Expressions.name(e)));
                } else {
                    checkGroupingFunctionTarget(e, localFailures);
                }
            }));

            a.groupings().forEach(g -> g.forEachDown(GroupingFunction.class, e -> {
                checkGroupingFunctionTarget(e, localFailures);
            }));
        }
    }

    private static void checkGroupingFunctionTarget(GroupingFunction f, Set<Failure> localFailures) {
        f.field().forEachDown(e -> {
            if (e instanceof GroupingFunction) {
                localFailures.add(fail(f.field(), "Cannot embed grouping functions within each other, found [{}] in [{}]",
                        Expressions.name(f.field()), Expressions.name(f)));
            }
        });
    }

    private static void checkFilterOnAggs(LogicalPlan p, Set<Failure> localFailures, AttributeMap<Expression> attributeRefs) {
        if (p instanceof Filter) {
            Filter filter = (Filter) p;
            if (filter.anyMatch(Aggregate.class::isInstance) == false) {
                filter.condition().forEachDown(Expression.class, e -> {
                    if (Functions.isAggregate(attributeRefs.resolve(e, e))) {
                        if (filter.child() instanceof Project) {
                            filter.condition().forEachDown(FieldAttribute.class,
                                f -> localFailures.add(fail(e, "[{}] field must appear in the GROUP BY clause or in an aggregate function",
                                        Expressions.name(f)))
                            );
                        } else {
                            localFailures.add(fail(e, "Cannot use WHERE filtering on aggregate function [{}], use HAVING instead",
                                Expressions.name(e)));

                        }
                    }
                });
            } else {
                Set<Expression> unsupported = new LinkedHashSet<>();
                filter.condition().forEachDown(Expression.class, e -> {
                    Expression f = attributeRefs.resolve(e, e);
                    if (f instanceof TopHits) {
                        unsupported.add(f);
                    }
                });
                if (unsupported.isEmpty() == false) {
                    String plural = unsupported.size() > 1 ? "s" : StringUtils.EMPTY;
                    localFailures.add(
                            fail(filter.condition(), "filtering is unsupported for function" + plural + " {}",
                                    Expressions.names(unsupported)));
                }
            }
        }
    }


    private static void checkFilterOnGrouping(LogicalPlan p, Set<Failure> localFailures, AttributeMap<Expression> attributeRefs) {
        if (p instanceof Filter) {
            Filter filter = (Filter) p;
            filter.condition().forEachDown(Expression.class, e -> {
                if (Functions.isGrouping(attributeRefs.resolve(e, e))) {
                    localFailures
                        .add(fail(e, "Cannot filter on grouping function [{}], use its argument instead", Expressions.name(e)));
                }
            });
        }
    }


    private static void checkForRestrictedFunctionInsideFunction(LogicalPlan p, Class<? extends Function> restrictedFunctionClass,
                                                                 Set<Failure> localFailures) {
        // Make sure that functions like SCORE or ARRAY only appear in clauses at their "top level", not part of other functions's args
        p.forEachExpression(Function.class, f ->
            f.arguments().stream()
                .filter(exp -> exp.anyMatch(restrictedFunctionClass::isInstance))
                .forEach(exp -> localFailures.add(fail(exp, "[{}()] cannot be used in expressions, does not support further processing",
                    restrictedFunctionClass.getSimpleName().toUpperCase(Locale.ROOT))))
        );
    }

    private static void checkNestedUsedInGroupByOrHavingOrWhereOrOrderBy(LogicalPlan p, Set<Failure> localFailures,
                                                                         AttributeMap<Expression> attributeRefs) {
        List<FieldAttribute> nested = new ArrayList<>();
        Consumer<FieldAttribute> matchNested = fa -> {
            if (fa.isNested()) {
                nested.add(fa);
            }
        };
        Consumer<Expression> checkForNested = e ->
            attributeRefs.resolve(e, e).forEachUp(FieldAttribute.class, matchNested);
        Consumer<ScalarFunction> checkForNestedInFunction = f -> f.arguments().forEach(
            arg -> arg.forEachUp(FieldAttribute.class, matchNested));

        // nested fields shouldn't be used in aggregates or having (yet)
        p.forEachDown(Aggregate.class, a -> a.groupings().forEach(agg -> agg.forEachUp(checkForNested)));
        if (nested.isEmpty() == false) {
            localFailures.add(
                    fail(nested.get(0), "Grouping isn't (yet) compatible with nested fields " + new AttributeSet(nested).names()));
            nested.clear();
        }

        // check in having
        p.forEachDown(Filter.class, f -> f.forEachDown(Aggregate.class, a -> f.condition().forEachUp(checkForNested)));
        if (nested.isEmpty() == false) {
            localFailures.add(
                fail(nested.get(0), "HAVING isn't (yet) compatible with nested fields " + new AttributeSet(nested).names()));
            nested.clear();
        }

        // check in where (scalars not allowed)
        p.forEachDown(Filter.class, f -> f.condition().forEachUp(e ->
            attributeRefs.resolve(e, e).forEachUp(ScalarFunction.class, sf -> {
                if (sf instanceof BinaryComparison == false &&
                    sf instanceof IsNull == false &&
                    sf instanceof IsNotNull == false &&
                    sf instanceof Not == false &&
                    sf instanceof BinaryLogic == false) {
                    checkForNestedInFunction.accept(sf);
                }
            })
        ));
        if (nested.isEmpty() == false) {
            localFailures.add(
                fail(nested.get(0), "WHERE isn't (yet) compatible with scalar functions on nested fields " +
                    new AttributeSet(nested).names()));
            nested.clear();
        }

        // check in order by (scalars not allowed)
        p.forEachDown(OrderBy.class, ob -> ob.order().forEach(o -> o.forEachUp(e ->
            attributeRefs.resolve(e, e).forEachUp(ScalarFunction.class, checkForNestedInFunction)
        )));
        if (nested.isEmpty() == false) {
            localFailures.add(
                fail(nested.get(0), "ORDER BY isn't (yet) compatible with scalar functions on nested fields " +
                    new AttributeSet(nested).names()));
        }
    }

    /**
     * Makes sure that geo shapes do not appear in filter, aggregation and sorting contexts
     */
    private static void checkForGeoFunctionsOnDocValues(LogicalPlan p, Set<Failure> localFailures) {

        p.forEachDown(Filter.class, f -> {
            f.condition().forEachUp(FieldAttribute.class, fa -> {
                if (fa.field().getDataType() == GEO_SHAPE) {
                    localFailures.add(fail(fa, "geo shapes cannot be used for filtering"));
                }
                if (fa.field().getDataType() == SHAPE) {
                    localFailures.add(fail(fa, "shapes cannot be used for filtering"));
                }
            });
        });

        // geo shape fields shouldn't be used in aggregates or having (yet)
        p.forEachDown(Aggregate.class, a -> a.groupings().forEach(agg -> agg.forEachUp(FieldAttribute.class, fa -> {
            if (fa.field().getDataType() == GEO_SHAPE) {
                localFailures.add(fail(fa, "geo shapes cannot be used in grouping"));
            }
            if (fa.field().getDataType() == SHAPE) {
                localFailures.add(fail(fa, "shapes cannot be used in grouping"));
            }
        })));


        // geo shape fields shouldn't be used in order by clauses
        p.forEachDown(OrderBy.class, o -> o.order().forEach(agg -> agg.forEachUp(FieldAttribute.class, fa -> {
            if (fa.field().getDataType() == GEO_SHAPE) {
                localFailures.add(fail(fa, "geo shapes cannot be used for sorting"));
            }
            if (fa.field().getDataType() == SHAPE) {
                localFailures.add(fail(fa, "shapes cannot be used for sorting"));
            }
        })));
    }

    private static void checkPivot(LogicalPlan p, Set<Failure> localFailures, AttributeMap<Expression> attributeRefs) {
        p.forEachDown(Pivot.class, pv -> {
            // check only exact fields are used inside PIVOTing
            if (onlyExactFields(combine(pv.groupingSet(), pv.column()), localFailures) == false
                || onlyRawFields(pv.groupingSet(), localFailures, attributeRefs) == false) {
                // if that is not the case, no need to do further validation since the declaration is fundamentally wrong
                return;
            }

            // check values
            DataType colType = pv.column().dataType();
            for (NamedExpression v : pv.values()) {
                // check all values are foldable
                Expression ex = v instanceof Alias ? ((Alias) v).child() : v;
                if (ex instanceof Literal == false) {
                    localFailures.add(fail(v, "Non-literal [{}] found inside PIVOT values", v.name()));
                }
                else if (ex.foldable() && ex.fold() == null) {
                    localFailures.add(fail(v, "Null not allowed as a PIVOT value", v.name()));
                }
                // and that their type is compatible with that of the column
                else if (SqlDataTypes.areCompatible(colType, v.dataType()) == false) {
                    localFailures.add(fail(v, "Literal [{}] of type [{}] does not match type [{}] of PIVOT column [{}]", v.name(),
                            v.dataType().typeName(), colType.typeName(), pv.column().sourceText()));
                }
            }

            // check aggregate function, in particular formulas that might hide literals or scalars
            pv.aggregates().forEach(a -> {
                Holder<Boolean> hasAggs = new Holder<>(Boolean.FALSE);
                List<Expression> aggs = a.collectFirstChildren(c -> {
                    // skip aggregate functions
                    if (Functions.isAggregate(c)) {
                        hasAggs.set(Boolean.TRUE);
                        return true;
                    }
                    if (c.children().isEmpty()) {
                        return true;
                    }
                    return false;
                });

                if (Boolean.FALSE.equals(hasAggs.get())) {
                    localFailures.add(fail(a, "No aggregate function found in PIVOT at [{}]", a.sourceText()));
                }
                // check mixture of Agg and column (wrapped in scalar)
                else {
                    for (Expression agg : aggs) {
                        if (agg instanceof FieldAttribute) {
                            localFailures.add(fail(a, "Non-aggregate function found in PIVOT at [{}]", a.sourceText()));
                        }
                    }
                }
            });

        });
    }

    private static void checkMatrixStats(LogicalPlan p, Set<Failure> localFailures) {
        // MatrixStats aggregate functions cannot operates on scalars
        // https://github.com/elastic/elasticsearch/issues/55344
        p.forEachExpressionUp(NumericAggregate.class, s -> {
            if ((s instanceof Kurtosis || s instanceof Skewness) && s.field() instanceof Function) {
                localFailures.add(fail(s.field(), "[{}()] cannot be used on top of operators or scalars", s.functionName()));
            }
        });
    }

    private static void checkCastOnInexact(LogicalPlan p, Set<Failure> localFailures) {
        p.forEachDown(Filter.class, f -> f.forEachExpressionUp(Cast.class, c -> {
            if (c.field() instanceof FieldAttribute) {
                EsField.Exact exactInfo = ((FieldAttribute) c.field()).getExactInfo();
                if (exactInfo.hasExact() == false
                    || ((FieldAttribute) c.field()).exactAttribute().equals(c.field()) == false) {
                    localFailures.add(fail(c.field(),
                        "[{}] of data type [{}] cannot be used for [{}()] inside the WHERE clause",
                        c.field().sourceText(), c.field().dataType().typeName(), c.functionName()));
                }
            }
        }));
    }

    // check that any binary field used in WHERE, GROUP BY, HAVING or ORDER BY has doc_values, for ES to allow querying it
    private static void checkBinaryHasDocValues(LogicalPlan plan, Set<Failure> localFailures) {
        List<Tuple<FieldAttribute, String>> fields = new ArrayList<>();

        plan.forEachDown(Filter.class, e -> e.condition().forEachDown(FieldAttribute.class,
            f -> fields.add(Tuple.tuple(f, "for filtering"))));
        plan.forEachDown(Aggregate.class, e -> e.groupings().forEach(g -> g.forEachDown(FieldAttribute.class,
            f -> fields.add(Tuple.tuple(f, "in aggregations")))));
        plan.forEachDown(OrderBy.class, e -> e.order().forEach(o -> o.child().forEachDown(FieldAttribute.class,
            f -> fields.add(Tuple.tuple(f, "for ordering")))));

        fields.stream().filter(t -> t.v1().dataType() == BINARY && t.v1().field().isAggregatable() == false).forEach(t -> {
            localFailures.add(fail(t.v1(), "Binary field [" + t.v1().name() + "] cannot be used " + t.v2() + " unless it has the "
                + "doc_values setting enabled"));
        });
    }

    private static void checkArrayFunctionUsedInWhereOrOrderByOrAggregate(LogicalPlan plan, Set<Failure> localFailures) {
        BiConsumer<Expression, String> check = (exp, clause) -> {
            if (exp instanceof Array) {
                localFailures.add(fail(exp, "[ARRAY()] may be used in the SELECT clause only, but found in [{}]", clause));
            }
        };
        // The filter would require a boolean type, so ARRAY() would need to be part of a function call in WHERE; meaning that
        // checkForRestrictedFunctionInsideFunction() would catch this usage already. However, the error message returned by that check
        // would be confusing (especially when a "function" use is not obvious, like: "SELECT ARRAY(..) AS x WHERE x IS NOT NULL").
        plan.forEachDown(Filter.class, e -> e.condition().forEachDown(o -> check.accept(o, "WHERE")));
        plan.forEachDown(OrderBy.class, e -> e.order().forEach(o -> check.accept(o.child(), "ORDER BY")));
        plan.forEachDown(Aggregate.class, e -> e.groupings().forEach(g -> check.accept(g, "GROUP BY")));
    }

    private static void checkArrayFunctionArguments(LogicalPlan plan, Set<Failure> localFailures) {
        // TODO: allow `*`? scalars? or possibly array-level aggs?
        plan.forEachExpression(Array.class, r -> r.arguments().forEach(a -> {
            if (a instanceof FieldAttribute == false &&
                (a instanceof Alias == false || ((Alias) a).child() instanceof FieldAttribute == false)) {
                localFailures.add(fail(r, "ARRAY()'s argument must be an index field, found [{}]", a.source().text()));
            }
        }));
    }

    private static void checkClientSupportsDataTypes(LogicalPlan p, Set<Failure> localFailures, SqlVersion version) {
        p.output().forEach(e -> {
            if (e.resolved() && isTypeSupportedInVersion(e.dataType(), version) == false) {
                localFailures.add(fail(e, "Cannot use [" + e.name() + "] with type [" + e.dataType() + "] unsupported " +
                    "in version [" + version + "], upgrade required (to version [" + versionIntroducingType(e.dataType()) +
                    "] or higher)"));
            }
        });
    }
}
