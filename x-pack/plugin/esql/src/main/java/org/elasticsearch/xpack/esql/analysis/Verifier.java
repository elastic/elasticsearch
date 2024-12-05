/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
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
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.stats.FeatureMetric;
import org.elasticsearch.xpack.esql.stats.Metrics;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * This class is part of the planner. Responsible for failing impossible queries with a human-readable error message.  In particular, this
 * step does type resolution and fails queries based on invalid type expressions.
 */
public class Verifier {

    private final Metrics metrics;
    private final XPackLicenseState licenseState;

    public Verifier(Metrics metrics, XPackLicenseState licenseState) {
        this.metrics = metrics;
        this.licenseState = licenseState;
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
                    // Special handling for Project and unsupported/union types: disallow renaming them but pass them through otherwise.
                    if (p instanceof Project) {
                        if (ae instanceof Alias as && as.child() instanceof UnsupportedAttribute ua) {
                            failures.add(fail(ae, ua.unresolvedMessage()));
                        }
                        if (ae instanceof UnsupportedAttribute) {
                            return;
                        }
                    }

                    // Do not fail multiple times in case the children are already unresolved.
                    if (ae.childrenResolved() == false) {
                        return;
                    }

                    if (ae instanceof Unresolvable u) {
                        failures.add(fail(ae, u.unresolvedMessage()));
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
            checkForSortableDataTypes(p, failures);
            checkSort(p, failures);

            checkFullTextQueryFunctions(p, failures);
            checkJoin(p, failures);
        });
        checkRemoteEnrich(plan, failures);
        checkMetadataScoreNameReserved(plan, failures);

        if (failures.isEmpty()) {
            checkLicense(plan, licenseState, failures);
        }

        // gather metrics
        if (failures.isEmpty()) {
            gatherMetrics(plan, partialMetrics);
        }

        return failures;
    }

    private static void checkMetadataScoreNameReserved(LogicalPlan p, Set<Failure> failures) {
        // _score can only be set as metadata attribute
        if (p.inputSet().stream().anyMatch(a -> MetadataAttribute.SCORE.equals(a.name()) && (a instanceof MetadataAttribute) == false)) {
            failures.add(fail(p, "`" + MetadataAttribute.SCORE + "` is a reserved METADATA attribute"));
        }
    }

    private void checkSort(LogicalPlan p, Set<Failure> failures) {
        if (p instanceof OrderBy ob) {
            ob.order().forEach(o -> {
                o.forEachDown(Function.class, f -> {
                    if (f instanceof AggregateFunction) {
                        failures.add(fail(f, "Aggregate functions are not allowed in SORT [{}]", f.functionName()));
                    }
                });
            });
        }
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
            checkCategorizeGrouping(agg, failures);
        } else {
            p.forEachExpression(
                GroupingFunction.class,
                gf -> failures.add(fail(gf, "cannot use grouping function [{}] outside of a STATS command", gf.sourceText()))
            );
        }
    }

    /**
     * Check CATEGORIZE grouping function usages.
     * <p>
     *     Some of those checks are temporary, until the required syntax or engine changes are implemented.
     * </p>
     */
    private static void checkCategorizeGrouping(Aggregate agg, Set<Failure> failures) {
        // Forbid CATEGORIZE grouping function with other groupings
        if (agg.groupings().size() > 1) {
            agg.groupings().forEach(g -> {
                g.forEachDown(
                    Categorize.class,
                    categorize -> failures.add(
                        fail(categorize, "cannot use CATEGORIZE grouping function [{}] with multiple groupings", categorize.sourceText())
                    )
                );
            });
        }

        // Forbid CATEGORIZE grouping functions not being top level groupings
        agg.groupings().forEach(g -> {
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
        agg.aggregates()
            .forEach(
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
        agg.groupings().forEach(g -> {
            g.forEachDown(Alias.class, alias -> {
                if (alias.child() instanceof Categorize categorize) {
                    categorizeByAttribute.put(alias.toAttribute(), categorize);
                }
            });
        });
        agg.aggregates()
            .forEach(a -> a.forEachDown(AggregateFunction.class, aggregate -> aggregate.forEachDown(Attribute.class, attribute -> {
                var categorize = categorizeByAttribute.get(attribute);
                if (categorize != null) {
                    failures.add(
                        fail(attribute, "cannot reference CATEGORIZE grouping function [{}] within an aggregation", attribute.sourceText())
                    );
                }
            })));
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
        // unwrap filtered expression
        if (e instanceof FilteredExpression fe) {
            e = fe.delegate();
            // make sure they work on aggregate functions
            if (e.anyMatch(AggregateFunction.class::isInstance) == false) {
                Expression filter = fe.filter();
                failures.add(fail(filter, "WHERE clause allowed only for aggregate functions, none found in [{}]", fe.sourceText()));
            }
            Expression f = fe.filter(); // check the filter has to be a boolean term, similar as checkFilterConditionType
            if (f.dataType() != NULL && f.dataType() != BOOLEAN) {
                failures.add(fail(f, "Condition expression needs to be boolean, found [{}]", f.dataType()));
            }
            // but that the filter doesn't use grouping or aggregate functions
            fe.filter().forEachDown(c -> {
                if (c instanceof AggregateFunction af) {
                    failures.add(
                        fail(af, "cannot use aggregate function [{}] in aggregate WHERE clause [{}]", af.sourceText(), fe.sourceText())
                    );
                }
                // check the bucketing function against the group
                else if (c instanceof GroupingFunction gf) {
                    if (Expressions.anyMatch(groups, ex -> ex instanceof Alias a && a.child().semanticEquals(gf)) == false) {
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
                    failures.add(fail(a.child(), "cannot use [{}] directly in a row assignment", a.child().sourceText()));
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
                        fail(
                            field,
                            "EVAL does not support type [{}] as the return data type of expression [{}]",
                            dataType.typeName(),
                            field.child().sourceText()
                        )
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

    private void checkLicense(LogicalPlan plan, XPackLicenseState licenseState, Set<Failure> failures) {
        plan.forEachExpressionDown(Function.class, p -> {
            if (p.checkLicense(licenseState) == false) {
                failures.add(new Failure(p, "current license is non-compliant for function [" + p.sourceText() + "]"));
            }
        });
    }

    private void gatherMetrics(LogicalPlan plan, BitSet b) {
        plan.forEachDown(p -> FeatureMetric.set(p, b));
        for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i + 1)) {
            metrics.inc(FeatureMetric.values()[i]);
        }
        Set<Class<?>> functions = new HashSet<>();
        plan.forEachExpressionDown(Function.class, p -> functions.add(p.getClass()));
        functions.forEach(f -> metrics.incFunctionMetric(f));
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
        if (EsqlCapabilities.Cap.SEMANTIC_TEXT_TYPE.isEnabled()) {
            allowed.add(DataType.SEMANTIC_TEXT);
        }
        allowed.add(DataType.IP);
        allowed.add(DataType.DATETIME);
        allowed.add(DataType.DATE_NANOS);
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
     * Some datatypes are not sortable
     */
    private static void checkForSortableDataTypes(LogicalPlan p, Set<Failure> localFailures) {
        if (p instanceof OrderBy ob) {
            ob.order().forEach(order -> {
                if (DataType.isSortable(order.dataType()) == false) {
                    localFailures.add(fail(order, "cannot sort on " + order.dataType().typeName()));
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
        boolean[] enrichCoord = { false };

        plan.forEachUp(UnaryPlan.class, u -> {
            if (u instanceof Aggregate) {
                agg[0] = true;
            } else if (u instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                enrichCoord[0] = true;
            }
            if (u instanceof Enrich enrich && enrich.mode() == Enrich.Mode.REMOTE) {
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
     * Checks whether a condition contains a disjunction with the specified typeToken. Adds to failure if it does.
     *
     * @param condition        condition to check for disjunctions
     * @param typeNameProvider provider for the type name to add in the failure message
     * @param failures         failures collection to add to
     */
    private static void checkNotPresentInDisjunctions(
        Expression condition,
        java.util.function.Function<FullTextFunction, String> typeNameProvider,
        Set<Failure> failures
    ) {
        condition.forEachUp(Or.class, or -> {
            checkNotPresentInDisjunctions(or.left(), or, typeNameProvider, failures);
            checkNotPresentInDisjunctions(or.right(), or, typeNameProvider, failures);
        });
    }

    /**
     * Checks whether a condition contains a disjunction with the specified typeToken. Adds to failure if it does.
     *
     * @param parentExpression parent expression to add to the failure message
     * @param or               disjunction that is being checked
     * @param failures         failures collection to add to
     */
    private static void checkNotPresentInDisjunctions(
        Expression parentExpression,
        Or or,
        java.util.function.Function<FullTextFunction, String> elementName,
        Set<Failure> failures
    ) {
        parentExpression.forEachDown(FullTextFunction.class, ftp -> {
            failures.add(
                fail(or, "Invalid condition [{}]. {} can't be used as part of an or condition", or.sourceText(), elementName.apply(ftp))
            );
        });
    }

    /**
     * Checks Joins for invalid usage.
     *
     * @param plan root plan to check
     * @param failures failures found
     */
    private static void checkJoin(LogicalPlan plan, Set<Failure> failures) {
        if (plan instanceof Join join) {
            JoinConfig config = join.config();
            for (int i = 0; i < config.leftFields().size(); i++) {
                Attribute leftField = config.leftFields().get(i);
                Attribute rightField = config.rightFields().get(i);
                if (leftField.dataType() != rightField.dataType()) {
                    failures.add(
                        fail(
                            leftField,
                            "JOIN left field [{}] of type [{}] is incompatible with right field [{}] of type [{}]",
                            leftField.name(),
                            leftField.dataType(),
                            rightField.name(),
                            rightField.dataType()
                        )
                    );
                }
            }

        }
    }

    /**
     * Checks full text query functions for invalid usage.
     *
     * @param plan root plan to check
     * @param failures failures found
     */
    private static void checkFullTextQueryFunctions(LogicalPlan plan, Set<Failure> failures) {
        if (plan instanceof Filter f) {
            Expression condition = f.condition();

            List.of(QueryString.class, Kql.class).forEach(functionClass -> {
                // Check for limitations of QSTR and KQL function.
                checkCommandsBeforeExpression(
                    plan,
                    condition,
                    functionClass,
                    lp -> (lp instanceof Filter || lp instanceof OrderBy || lp instanceof EsRelation),
                    fullTextFunction -> "[" + fullTextFunction.functionName() + "] " + fullTextFunction.functionType(),
                    failures
                );
            });

            checkCommandsBeforeExpression(
                plan,
                condition,
                Match.class,
                lp -> (lp instanceof Limit == false) && (lp instanceof Aggregate == false),
                m -> "[" + m.functionName() + "] " + m.functionType(),
                failures
            );
            checkNotPresentInDisjunctions(condition, ftf -> "[" + ftf.functionName() + "] " + ftf.functionType(), failures);
            checkFullTextFunctionsParents(condition, failures);
        } else {
            plan.forEachExpression(FullTextFunction.class, ftf -> {
                failures.add(fail(ftf, "[{}] {} is only supported in WHERE commands", ftf.functionName(), ftf.functionType()));
            });
        }
    }

    /**
     * Checks all commands that exist before a specific type satisfy conditions.
     *
     * @param plan plan that contains the condition
     * @param condition condition to check
     * @param typeToken type to check for. When a type is found in the condition, all plans before the root plan are checked
     * @param commandCheck check to perform on each command that precedes the plan that contains the typeToken
     * @param typeErrorMsgProvider provider for the type name in the error message
     * @param failures failures to add errors to
     * @param <E> class of the type to look for
     */
    private static <E extends Expression> void checkCommandsBeforeExpression(
        LogicalPlan plan,
        Expression condition,
        Class<E> typeToken,
        Predicate<LogicalPlan> commandCheck,
        java.util.function.Function<E, String> typeErrorMsgProvider,
        Set<Failure> failures
    ) {
        condition.forEachDown(typeToken, exp -> {
            plan.forEachDown(LogicalPlan.class, lp -> {
                if (commandCheck.test(lp) == false) {
                    failures.add(
                        fail(
                            plan,
                            "{} cannot be used after {}",
                            typeErrorMsgProvider.apply(exp),
                            lp.sourceText().split(" ")[0].toUpperCase(Locale.ROOT)
                        )
                    );
                }
            });
        });
    }

    /**
     * Checks parents of a full text function to ensure they are allowed
     * @param condition condition that contains the full text function
     * @param failures failures to add errors to
     */
    private static void checkFullTextFunctionsParents(Expression condition, Set<Failure> failures) {
        forEachFullTextFunctionParent(condition, (ftf, parent) -> {
            if ((parent instanceof FullTextFunction == false)
                && (parent instanceof BinaryLogic == false)
                && (parent instanceof Not == false)) {
                failures.add(
                    fail(
                        condition,
                        "Invalid condition [{}]. [{}] {} can't be used with {}",
                        condition.sourceText(),
                        ftf.functionName(),
                        ftf.functionType(),
                        ((Function) parent).functionName()
                    )
                );
            }
        });
    }

    /**
     * Executes the action on every parent of a FullTextFunction in the condition if it is found
     *
     * @param action the action to execute for each parent of a FullTextFunction
     */
    private static FullTextFunction forEachFullTextFunctionParent(Expression condition, BiConsumer<FullTextFunction, Expression> action) {
        if (condition instanceof FullTextFunction ftf) {
            return ftf;
        }
        for (Expression child : condition.children()) {
            FullTextFunction foundMatchingChild = forEachFullTextFunctionParent(child, action);
            if (foundMatchingChild != null) {
                action.accept(foundMatchingChild, condition);
                return foundMatchingChild;
            }
        }
        return null;
    }
}
