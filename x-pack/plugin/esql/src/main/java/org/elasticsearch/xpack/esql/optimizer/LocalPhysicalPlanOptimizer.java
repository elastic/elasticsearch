/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext.SplitPlanAfterTopN;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.AvoidFieldExtractionAfterTopN;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.EnableSpatialDistancePushdown;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushLimitToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushSampleToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushStatsToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceRoundToWithQueryAndTags;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SpatialDocValuesExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SpatialShapeBoundsExtraction;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages field extraction and pushing parts of the query into Lucene. (Query elements that are not pushed into Lucene are executed via
 * the compute engine)
 */
public class LocalPhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, LocalPhysicalOptimizerContext> {
    private static final List<Batch<PhysicalPlan>> SPLIT_AFTER_TOP_N = rules(true, SplitPlanAfterTopN.SPLIT);
    private static final List<Batch<PhysicalPlan>> NO_SPLIT_AFTER_TOP_N = rules(true, SplitPlanAfterTopN.NO_SPLIT);

    private final PostOptimizationPhasePlanVerifier<PhysicalPlan> verifier;

    public LocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context) {
        this(context, PhysicalVerifier.INSTANCE);
    }

    public LocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context, PostOptimizationPhasePlanVerifier<PhysicalPlan> verifier) {
        super(context);
        this.verifier = verifier;
    }

    public PhysicalPlan localOptimize(PhysicalPlan plan) {
        PhysicalPlan result = execute(plan);
        try {
            return verify(result, plan.output());
        } catch (VerificationException e) {
            return switch (context().splitDataDriverPlanAfterTopN()) {
                case NO_SPLIT -> throw e;
                // If we perform the split, the output will verify likely be different (since we modified the ProjectExec after TopNExec).
                case SPLIT -> verifyTopNDataPlan(result);
            };
        }
    }

    private static PhysicalPlan verifyTopNDataPlan(PhysicalPlan result) {
        if (result.output().stream().noneMatch(EsQueryExec::isSourceAttribute)) {
            throw new VerificationException("Data-driver plan did not include source attribute");
        }

        return result;
    }

    private PhysicalPlan verify(PhysicalPlan optimizedPlan, List<Attribute> expectedOutputAttributes) {
        Failures failures = verifier.verify(optimizedPlan, true, expectedOutputAttributes);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        return optimizedPlan;
    }

    @Override
    protected List<Batch<PhysicalPlan>> batches() {
        return switch (context().splitDataDriverPlanAfterTopN()) {
            case SPLIT -> SPLIT_AFTER_TOP_N;
            case NO_SPLIT -> NO_SPLIT_AFTER_TOP_N;
        };
    }

    @SuppressWarnings("unchecked")
    protected static List<Batch<PhysicalPlan>> rules(boolean optimizeForEsSource, SplitPlanAfterTopN split) {
        // execute the rules multiple times to improve the chances of things being pushed down. If we we want to remove the Project after
        // TopN, this should be done in the first pass.
        var firstRules = new ArrayList<Rule<?, PhysicalPlan>>(2);
        firstRules.add(new ReplaceSourceAttributes());
        if (optimizeForEsSource && split == SplitPlanAfterTopN.SPLIT) {
            // This rule should only be applied once, since it is not idempotent.
            firstRules.add(new AvoidFieldExtractionAfterTopN());
        }
        var prePushdown = new Batch<PhysicalPlan>("Pre-pushdown", Limiter.ONCE, firstRules.toArray(Rule[]::new));
        var pushdown = new Batch<>("Push to ES", pushdownRules(optimizeForEsSource));

        // execute the SubstituteRoundToWithQueryAndTags rule once after all the other pushdown rules are applied, as this rule generate
        // multiple QueryBuilders according the number of RoundTo points, it should be applied after all the other eligible pushdowns are
        // done, and it should be executed only once.
        var substitutionRules = new Batch<>("Substitute RoundTo with QueryAndTags", Limiter.ONCE, new ReplaceRoundToWithQueryAndTags());
        // add the field extraction in just one pass
        // add it at the end after all the other rules have run
        List<Rule<?, PhysicalPlan>> fieldExtractionRules = new ArrayList<>(3);
        fieldExtractionRules.add(new InsertFieldExtraction());
        fieldExtractionRules.add(new SpatialDocValuesExtraction());
        fieldExtractionRules.add(new SpatialShapeBoundsExtraction());
        var fieldExtractionBatch = new Batch<PhysicalPlan>("Field extraction", Limiter.ONCE, fieldExtractionRules.toArray(Rule[]::new));

        return optimizeForEsSource ?

            List.of(prePushdown, pushdown, substitutionRules, fieldExtractionBatch) : List.of(prePushdown, pushdown, fieldExtractionBatch);

    }

    @SuppressWarnings("unchecked")
    private static Rule<?, PhysicalPlan>[] pushdownRules(boolean optimizeForEsSource) {
        var rules = optimizeForEsSource
            ? List.of(
                new PushTopNToSource(),
                new PushLimitToSource(),
                new PushFiltersToSource(),
                new PushSampleToSource(),
                new PushStatsToSource(),
                new EnableSpatialDistancePushdown()
            )
            : List.of();
        return rules.toArray(Rule[]::new);
    }
}
