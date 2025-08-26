/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;
import org.elasticsearch.xpack.esql.telemetry.Metrics;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;

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
        Failures failures = new Failures();

        // quick verification for unresolved attributes
        checkUnresolvedAttributes(plan, failures);

        // in case of failures bail-out as all other checks will be redundant
        if (failures.hasFailures()) {
            return failures.failures();
        }

        // collect plan checkers
        var planCheckers = planCheckers(plan);

        // Concrete verifications
        plan.forEachDown(p -> {
            // if the children are unresolved, so will this node; counting it will only add noise
            if (p.childrenResolved() == false) {
                return;
            }

            planCheckers.forEach(c -> c.accept(p, failures));

            checkOperationsOnUnsignedLong(p, failures);
            checkBinaryComparison(p, failures);
        });

        if (failures.hasFailures() == false) {
            licenseCheck(plan, failures);
        }

        // gather metrics
        if (failures.hasFailures() == false) {
            gatherMetrics(plan, partialMetrics);
        }

        return failures.failures();
    }

    private static void checkUnresolvedAttributes(LogicalPlan plan, Failures failures) {
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
    }

    private static List<BiConsumer<LogicalPlan, Failures>> planCheckers(LogicalPlan plan) {
        List<BiConsumer<LogicalPlan, Failures>> planCheckers = new ArrayList<>();
        Consumer<? super Node<?>> collectPlanCheckers = p -> {
            if (p instanceof PostAnalysisPlanVerificationAware pva) {
                planCheckers.add(pva.postAnalysisPlanVerification());
            }
        };
        plan.forEachDown(p -> {
            collectPlanCheckers.accept(p);
            p.forEachExpression(collectPlanCheckers);

            if (p instanceof PostAnalysisVerificationAware va) {
                planCheckers.add((lp, failures) -> {
                    if (lp.getClass().equals(va.getClass())) {
                        va.postAnalysisVerification(failures);
                    }
                });
            }
        });
        return planCheckers;
    }

    private static void checkOperationsOnUnsignedLong(LogicalPlan p, Failures failures) {
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

    private static void checkBinaryComparison(LogicalPlan p, Failures failures) {
        p.forEachExpression(BinaryComparison.class, bc -> {
            Failure f = validateBinaryComparison(bc);
            if (f != null) {
                failures.add(f);
            }
        });
    }

    private void licenseCheck(LogicalPlan plan, Failures failures) {
        Consumer<Node<?>> licenseCheck = n -> {
            if (n instanceof LicenseAware la && la.licenseCheck(licenseState) == false) {
                failures.add(fail(n, "current license is non-compliant for [{}]", n.sourceText()));
            }
        };
        plan.forEachDown(p -> {
            licenseCheck.accept(p);
            p.forEachExpression(Expression.class, licenseCheck);
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

    public XPackLicenseState licenseState() {
        return licenseState;
    }

    /**
     * Limit QL's comparisons to types we support.  This should agree with
     * {@link EsqlBinaryComparison}'s checkCompatibility method
     *
     * @return null if the given binary comparison has valid input types,
     *         otherwise a failure message suitable to return to the user.
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

        // Allow mixed millisecond and nanosecond binary comparisons
        if (bc.left().dataType().isDate() && bc.right().dataType().isDate()) {
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
}
