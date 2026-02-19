/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;
import org.elasticsearch.xpack.esql.telemetry.Metrics;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.areTypesCompatible;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.formatIncompatibleTypesMessage;

/**
 * This class is part of the planner. Responsible for failing impossible queries with a human-readable error message.  In particular, this
 * step does type resolution and fails queries based on invalid type expressions.
 */
public class Verifier {

    /**
     * Extra plan verification checks defined in plugins.
     */
    private final List<BiConsumer<LogicalPlan, Failures>> extraCheckers;
    private final Metrics metrics;
    private final XPackLicenseState licenseState;

    public Verifier(Metrics metrics, XPackLicenseState licenseState) {
        this(metrics, licenseState, Collections.emptyList());
    }

    public Verifier(Metrics metrics, XPackLicenseState licenseState, List<BiConsumer<LogicalPlan, Failures>> extraCheckers) {
        this.metrics = metrics;
        this.licenseState = licenseState;
        this.extraCheckers = extraCheckers;
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

        ConfigurationAware.verifyNoMarkerConfiguration(plan, failures);

        // in case of failures bail-out as all other checks will be redundant
        if (failures.hasFailures()) {
            return failures.failures();
        }

        // collect plan checkers
        var planCheckers = planCheckers(plan);
        planCheckers.addAll(extraCheckers);

        // Concrete verifications
        plan.forEachDown(p -> {
            // if the children are unresolved, so will this node; counting it will only add noise
            if (p.childrenResolved() == false) {
                return;
            }

            planCheckers.forEach(c -> c.accept(p, failures));

            checkOperationsOnUnsignedLong(p, failures);
            checkBinaryComparison(p, failures);
            checkUnsupportedAttributeRenaming(p, failures);
            checkInsist(p, failures);
            checkLimitBeforeInlineStats(p, failures);
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
                    // UnsupportedAttribute can pass through Project/Insist unchanged.
                    // Renaming is checked separately in #checkUnsupportedAttributeRenaming.
                    if ((p instanceof Project || p instanceof Insist) && ae instanceof UnsupportedAttribute) {
                        return;
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

                // We don't count _timeseries which is added implicitly to grouping but not to a list of aggs
                boolean hasGroupByAll = false;
                for (Expression grouping : groupings) {
                    if (MetadataAttribute.isTimeSeriesAttribute(grouping)) {
                        hasGroupByAll = true;
                        break;
                    }
                }
                int groupingSize = hasGroupByAll ? groupings.size() - 1 : groupings.size();

                // followed by just the aggregates (to avoid going through the groups again)
                var aggs = agg.aggregates();
                int size = aggs.size() - groupingSize;
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
            // The expressions of the PromqlCommand itself are not relevant here.
            // The promqlPlan is a separate tree and its children may contain UnresolvedAttribute expressions
            else if (p instanceof PromqlCommand promql) {
                promql.promqlPlan().forEachExpressionDown(Expression.class, unresolvedExpressions);
            }

            else {
                p.forEachExpression(unresolvedExpressions);
            }
        });
    }

    /**
     * Build a list of checkers based on the components in the plan.
     */
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

    /**
     * This validates only the negation operator, the rest of the validation is performed in
     * {@link Verifier#validateBinaryComparison(BinaryComparison)}
     * and
     * {@link EsqlBinaryComparison#areTypesCompatible(DataType, DataType)}
     */
    private static void checkOperationsOnUnsignedLong(LogicalPlan p, Failures failures) {
        p.forEachExpression(Neg.class, neg -> {
            Failure f = validateUnsignedLongNegation(neg);
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

    private static void checkInsist(LogicalPlan p, Failures failures) {
        if (p instanceof Insist i) {
            LogicalPlan child = i.child();
            if ((child instanceof EsRelation || child instanceof Insist) == false) {
                failures.add(fail(i, "[insist] can only be used after [from] or [insist] commands, but was [{}]", child.sourceText()));
            }
        }
    }

    /**
     * Check that UnsupportedAttribute is not renamed via Alias in Project or Insist.
     * UnsupportedAttribute can pass through these plans unchanged, but renaming is not allowed.
     * This check runs unconditionally (not gated by {@link LogicalPlan#resolved()}) because
     * {@link Project#expressionsResolved()} treats UnsupportedAttribute as resolved to allow pass-through.
     */
    private static void checkUnsupportedAttributeRenaming(LogicalPlan p, Failures failures) {
        if (p instanceof Project || p instanceof Insist) {
            p.forEachExpression(Alias.class, alias -> {
                if (alias.child() instanceof UnsupportedAttribute ua) {
                    failures.add(fail(alias, ua.unresolvedMessage()));
                }
            });
        }
    }

    /*
     * This is a rudimentary check to prevent INLINE STATS after LIMIT. A LIMIT command can be added by other commands by default,
     * the best example being FORK. A more robust solution would be to track the commands that add LIMIT and prevent them from doing so
     * if INLINE STATS is present in the plan. However, this would require authors of new such commands to be aware of this limitation and
     * implement the necessary checks, which is error-prone.
     */
    private static void checkLimitBeforeInlineStats(LogicalPlan plan, Failures failures) {
        if (plan instanceof InlineStats is) {
            Holder<Limit> inlineStatsDescendantLimit = new Holder<>();
            is.forEachDownMayReturnEarly((p, breakEarly) -> {
                if (p instanceof Limit l) {
                    inlineStatsDescendantLimit.set(l);
                    breakEarly.set(true);
                    return;
                }
            });

            var firstLimit = inlineStatsDescendantLimit.get();
            if (firstLimit != null) {
                var isString = is.sourceText().length() > Node.TO_STRING_MAX_WIDTH
                    ? is.sourceText().substring(0, Node.TO_STRING_MAX_WIDTH) + "..."
                    : is.sourceText();
                var limitString = firstLimit.sourceText().length() > Node.TO_STRING_MAX_WIDTH
                    ? firstLimit.sourceText().substring(0, Node.TO_STRING_MAX_WIDTH) + "..."
                    : firstLimit.sourceText();
                failures.add(
                    fail(
                        is,
                        "INLINE STATS cannot be used after an explicit or implicit LIMIT command, but was [{}] after [{}] [{}]",
                        isString,
                        limitString,
                        firstLimit.source().source().toString()
                    )
                );
            }
        }
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
        if (areTypesCompatible(bc.left().dataType(), bc.right().dataType())) {
            return null;
        }

        Failure typeCheckFailure = checkBinaryComparisonLeftOperandType(bc);
        if (typeCheckFailure != null) {
            return typeCheckFailure;
        }

        return fail(bc, formatIncompatibleTypesMessage(bc.left().dataType(), bc.right().dataType(), bc.sourceText()));
    }

    /**
     * Check that the left operand of a binary comparison is of an allowed type.
     * This validates that the comparison operation is supported for the given data types.
     *
     * @return a Failure if the left operand type is not allowed, null otherwise
     */
    private static Failure checkBinaryComparisonLeftOperandType(BinaryComparison bc) {
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
        allowed.add(DataType.GEOHASH);
        allowed.add(DataType.GEOTILE);
        allowed.add(DataType.GEOHEX);
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
