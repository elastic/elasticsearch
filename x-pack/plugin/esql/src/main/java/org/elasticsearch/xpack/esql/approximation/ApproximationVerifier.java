/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Variance;
import org.elasticsearch.xpack.esql.expression.function.aggregate.WeightedAvg;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.RegisteredDomain;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;
import org.elasticsearch.xpack.esql.plan.logical.UserAgent;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This class approximate results for certain classes of ES|QL queries.
 * Approximate results are usually much faster to compute than exact results.
 * <p>
 * A query is currently suitable for approximation if:
 * <ul>
 *   <li> it has a supported {@code STATS} layout: without {@code FORK}, exactly one
 *        {@code STATS}; with {@code FORK}, either {@code FROM | FORK (...) | STATS}
 *        (one {@code STATS} after the fork, none inside branches) or
 *        {@code FROM | STATS | FORK (...)} (exactly one {@code STATS} per branch)
 *   <li> the other processing commands are from the supported set
 *        ({@link ApproximationVerifier#SUPPORTED_COMMANDS}); this set contains almost all
 *        unary commands, and some non-unary ones; most notably not {@code FORK}.
 *   <li> the aggregate functions are from the supported set
 *        ({@link ApproximationVerifier#SUPPORTED_SINGLE_VALUED_AGGS} and
 *         {@link ApproximationVerifier#SUPPORTED_MULTIVALUED_AGGS})
 * </ul>
 * Some of these restrictions may be lifted in the future.
 */
public class ApproximationVerifier {

    public record QueryProperties(Boolean hasGrouping, Boolean preservesRows, List<QueryProperties> forkBranchProperties) {}

    /**
     * These processing commands are fully supported.
     * <p>
     * When a command is not supported, it should be added to
     * ApproximationSupportTests.UNSUPPORTED_COMMANDS
     * to make sure all commands are captured.
     */
    static final Set<Class<? extends LogicalPlan>> SUPPORTED_COMMANDS;
    static {
        Set<Class<? extends LogicalPlan>> SUPPORTED_COMMANDS_BUILDER = new HashSet<>(
            List.of(
                Aggregate.class,
                Completion.class,
                Dissect.class,
                Enrich.class,
                EsRelation.class,
                Eval.class,
                Filter.class,
                Grok.class,
                Insist.class,
                LocalRelation.class,
                MvExpand.class,
                OrderBy.class,
                Project.class,
                RegexExtract.class,
                RegisteredDomain.class,
                Rerank.class,
                Row.class,
                Sample.class,
                SampledAggregate.class,
                UriParts.class,
                UserAgent.class
            )
        );
        if (EsqlCapabilities.Cap.APPROXIMATION_LOOKUP_JOIN.isEnabled()) {
            SUPPORTED_COMMANDS_BUILDER.add(Join.class);
        }
        if (EsqlCapabilities.Cap.APPROXIMATION_INLINE_STATS_V2.isEnabled()) {
            SUPPORTED_COMMANDS_BUILDER.add(InlineJoin.class);
            SUPPORTED_COMMANDS_BUILDER.add(StubRelation.class);  // temporary node
        }
        if (EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled()) {
            SUPPORTED_COMMANDS_BUILDER.add(Fork.class);
        }
        SUPPORTED_COMMANDS = Collections.unmodifiableSet(SUPPORTED_COMMANDS_BUILDER);
    }

    /**
     * These processing commands are only supported after the initial STATS.
     */
    static final Set<Class<? extends LogicalPlan>> SUPPORTED_COMMANDS_AFTER_STATS = Set.of(
        // It makes no sense to approximate "FROM index | LIMIT N | STATS ...".
        // Furthermore, the LIMIT here breaks the estimation of the sample probability.
        Limit.class,
        // Same for LIMIT BY, SORT, or SORT + LIMIT BY
        LimitBy.class,
        TopN.class,
        TopNBy.class,
        // CHANGE_POINT implicitly uses LIMIT
        ChangePoint.class
    );

    /**
     * These index modes of EsRelation are supported.
     * <p>
     * Note: LOOKUP is added to make query validation output nicer messages
     * ("query with [LOOKUP JOIN ...] ..." instead of "query with [test_lookup] ...").
     */
    private static final Set<IndexMode> SUPPORTED_INDEX_MODES = Set.of(IndexMode.STANDARD, IndexMode.LOOKUP);

    /**
     * These commands preserve all rows (may expand them, never drops any),
     * making it easier to predict the number of output rows.
     */
    private static final Set<Class<? extends LogicalPlan>> ROW_PRESERVING_COMMANDS = Set.of(
        Completion.class,
        Dissect.class,
        Enrich.class,
        Eval.class,
        Grok.class,
        Insist.class,
        MvExpand.class,
        OrderBy.class,
        Project.class,
        RegexExtract.class,
        Rerank.class
    );

    /**
     * These aggregate functions behave well with random sampling, in the sense
     * that they converge to the true value as the sample size increases.
     * <p>
     * Aggregation functions that depend on the value of many or all of the
     * individual rows (like AVG, COUNT, MEDIAN) will generally satisfy the CLT
     * and are suitable for approximation.
     * On the other hand, aggregation functions that depend on a small subset of
     * rows (like COUNT DISTINCT, MAX, PRESENT) are not.
     * Aggregation function that depend on all values, but are very sensitive to
     * some (outlier) values (like STDDEV, PERCENTILE for low/high percentiles)
     * are challenging to approximate. They are supported, but may require a
     * larger sample size to obtain good accuracy.
     * <p>
     * When an aggregation is not supported, it should be added to
     * ApproximationSupportTests.UNSUPPORTED_AGGS
     * to make sure all aggregations are captured.
     */
    static final Set<Class<? extends AggregateFunction>> SUPPORTED_SINGLE_VALUED_AGGS = Set.of(
        Avg.class,
        Count.class,
        CountApproximate.class,
        Median.class,
        MedianAbsoluteDeviation.class,
        Percentile.class,
        StdDev.class,
        Sum.class,
        Variance.class,
        WeightedAvg.class
    );

    /**
     * These multivalued aggregate functions work well with random sampling.
     * However, confidence intervals make no sense anymore and are dropped.
     */
    static final Set<Class<? extends AggregateFunction>> SUPPORTED_MULTIVALUED_AGGS = Set.of(
        org.elasticsearch.xpack.esql.expression.function.aggregate.Sample.class
    );

    /**
     * Verifies that a plan is suitable for approximation.
     * @return the query properties relevant for approximation if it's suitable, or null otherwise
     * Adds warning headers as a side effect when the plan is not suitable
     */
    public static QueryProperties verifyPlan(LogicalPlan logicalPlan) {
        try {
            return verifyPlanOrThrow(logicalPlan);
        } catch (VerificationException e) {
            HeaderWarning.addWarning(e.getMessage());
            return null;
        }
    }

    static QueryProperties verifyPlanOrThrow(LogicalPlan logicalPlan) {
        // The plan must contain a STATS command.
        if (logicalPlan.anyMatch(plan -> plan instanceof Aggregate) == false) {
            Location location = logicalPlan.collectLeaves().getFirst().source().source();
            throw new VerificationException(
                "line {}:{}: approximation not supported: query must have [STATS] with aggregation function(s) that can be approximated",
                location.getLineNumber(),
                location.getColumnNumber()
            );
        }
        // Verify that all commands are supported.
        logicalPlan.forEachUp(plan -> {
            if ((SUPPORTED_COMMANDS.contains(plan.getClass()) == false && SUPPORTED_COMMANDS_AFTER_STATS.contains(plan.getClass()) == false)
                || (plan instanceof EsRelation esRelation && SUPPORTED_INDEX_MODES.contains(esRelation.indexMode()) == false)) {
                // TODO: ideally just return the command from the source
                // this can give bad messages (e.g. for subqueries) or long ones (many irrelevant extras)
                throw new VerificationException(
                    "line {}:{}: approximation not supported: query with [{}] cannot be approximated",
                    plan.source().source().getLineNumber(),
                    plan.source().source().getColumnNumber(),
                    plan.sourceText()
                );
            }
        });

        // Check whether there's a FORK.
        List<Fork> forks = logicalPlan.collect(Fork.class);
        if (forks.isEmpty()) {
            // When there's no FORK, verify this logical plan.
            return verifyBranchOrThrow(logicalPlan);
        } else {
            // When there's a FORK, find the structure.
            assert forks.size() == 1;
            Fork fork = forks.getFirst();

            boolean statsInBranches = fork.anyMatch(plan -> plan instanceof Aggregate);

            if (statsInBranches == false) {
                // When the FORK is after the STATS, like
                // - FROM index | FORK (...) (...) | STATS ...
                // verify there's just one STATS, and verify as if there were no FORK.
                List<Aggregate> aggregates = logicalPlan.collect(Aggregate.class);
                if (aggregates.size() > 1) {
                    throw new VerificationException(
                        "line {}:{}: approximation not supported: query with chained [STATS] cannot be approximated",
                        logicalPlan.source().source().getLineNumber(),
                        logicalPlan.source().source().getColumnNumber()
                    );
                }
                return new QueryProperties(aggregates.getFirst().groupings().isEmpty() == false, false, null);
            } else {
                // When the STATS is in a branch, like
                // - FROM index | STATS ... | FORK (...) (...)
                // - FROM index | FORK (STATS ...) (STATS ...) (...)
                // verify all branches.
                List<QueryProperties> branchProperties = new ArrayList<>();

                VerificationException firstVerificationException = null;
                for (int branchIndex = 0; branchIndex < fork.children().size(); branchIndex++) {
                    int branchIndexFinal = branchIndex;
                    LogicalPlan branch = logicalPlan.transformDown(Fork.class, f -> f.children().get(branchIndexFinal));
                    try {
                        branchProperties.add(verifyBranchOrThrow(branch));
                    } catch (VerificationException e) {
                        // Chained STATS result in a non-approximable query.
                        if (e.getMessage().contains("query with chained [STATS] cannot be approximated")) {
                            throw e;
                        }
                        // Otherwise, we can still approximate the other branch(es).
                        // Keep track of the exception to throw if all branches are non-approximable.
                        branchProperties.add(null);
                        if (firstVerificationException == null) {
                            firstVerificationException = e;
                        }
                    }
                }

                if (branchProperties.stream().allMatch(Objects::isNull)) {
                    assert firstVerificationException != null;
                    throw firstVerificationException;
                } else {
                    return new QueryProperties(null, null, branchProperties);
                }
            }
        }
    }

    private static QueryProperties verifyBranchOrThrow(LogicalPlan logicalPlan) {
        Holder<Boolean> encounteredStats = new Holder<>(false);
        Holder<Boolean> hasGrouping = new Holder<>();
        Holder<Boolean> preservesRows = new Holder<>(true);

        if (logicalPlan.anyMatch(plan -> plan instanceof Aggregate) == false) {
            return null;
        }

        logicalPlan.forEachUp(plan -> {
            if (encounteredStats.get() == false) {
                if (SUPPORTED_COMMANDS_AFTER_STATS.contains(plan.getClass())) {
                    throw new VerificationException(
                        "line {}:{}: approximation not supported: query with [{}] before [STATS] cannot be approximated",
                        plan.source().source().getLineNumber(),
                        plan.source().source().getColumnNumber(),
                        plan.sourceText()
                    );
                }
                if (plan instanceof Aggregate aggregate) {
                    // Verify that the aggregate functions are supported.
                    encounteredStats.set(true);
                    hasGrouping.set(aggregate.groupings().isEmpty() == false);
                    plan.forEachExpression(AggregateFunction.class, aggFn -> {
                        if (SUPPORTED_SINGLE_VALUED_AGGS.contains(aggFn.getClass()) == false
                            && SUPPORTED_MULTIVALUED_AGGS.contains(aggFn.getClass()) == false) {
                            // TODO: ideally just return aggregate function from the source
                            throw new VerificationException(
                                "line {}:{}: approximation not supported: aggregation function [{}] cannot be approximated",
                                aggFn.source().source().getLineNumber(),
                                aggFn.source().source().getColumnNumber(),
                                aggFn.sourceText()
                            );
                        }
                        if (aggFn.dataType().isNumeric() == false) {
                            throw new VerificationException(
                                "line {}:{}: approximation not supported: aggregation function [{}] must return a numeric value; got [{}]",
                                aggFn.source().source().getLineNumber(),
                                aggFn.source().source().getColumnNumber(),
                                aggFn.sourceText(),
                                aggFn.dataType()
                            );
                        }
                    });
                } else if (plan instanceof LeafPlan == false) {
                    if (ROW_PRESERVING_COMMANDS.contains(plan.getClass()) == false) {
                        preservesRows.set(false);
                    }
                }
            } else {
                // Chained STATS commands are not supported.
                if (plan instanceof Aggregate) {
                    throw new VerificationException(
                        "line {}:{}: approximation not supported: query with chained [STATS] cannot be approximated",
                        plan.source().source().getLineNumber(),
                        plan.source().source().getColumnNumber()
                    );
                }
            }
        });

        return new QueryProperties(hasGrouping.get(), preservesRows.get(), null);
    }
}
