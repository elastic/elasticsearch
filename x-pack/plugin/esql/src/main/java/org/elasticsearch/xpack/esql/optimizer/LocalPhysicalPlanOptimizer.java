/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.EnableSpatialDistancePushdown;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ParallelizeTimeSeriesSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushLimitToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushSampleToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushStatsToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.RemoveProjectAfterTopN;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SpatialDocValuesExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SpatialShapeBoundsExtraction;
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

    private static final List<Batch<PhysicalPlan>> RULES = rules(true);

    private final PhysicalVerifier verifier = PhysicalVerifier.INSTANCE;

    public LocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context) {
        super(context);
    }

    public PhysicalPlan localOptimize(PhysicalPlan plan) {
        return verify(execute(plan));
    }

    PhysicalPlan verify(PhysicalPlan plan) {
        Failures failures = verifier.verify(plan, true);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    @Override
    protected List<Batch<PhysicalPlan>> batches() {
        return RULES;
    }

    @SuppressWarnings("unchecked")
    protected static List<Batch<PhysicalPlan>> rules(boolean optimizeForEsSource) {
        // execute the rules multiple times to improve the chances of things being pushed down
        var pushdownFirst = new Batch<PhysicalPlan>("Push to ES first", Limiter.ONCE, esSourceRules(optimizeForEsSource, true));
        var pushdownContinuous = new Batch<PhysicalPlan>("Push to ES continuous", esSourceRules(optimizeForEsSource, false));

        // add the field extraction in just one pass
        // add it at the end after all the other rules have ran
        List<Rule<?, PhysicalPlan>> fieldExtractionRules = new ArrayList<>(4);
        fieldExtractionRules.add(new InsertFieldExtraction());
        fieldExtractionRules.add(new SpatialDocValuesExtraction());
        fieldExtractionRules.add(new SpatialShapeBoundsExtraction());
        fieldExtractionRules.add(new ParallelizeTimeSeriesSource());
        var fieldExtractionBatch = new Batch<PhysicalPlan>("Field extraction", Limiter.ONCE, fieldExtractionRules.toArray(Rule[]::new));

        return List.of(pushdownFirst, pushdownContinuous, fieldExtractionBatch);
    }

    @SuppressWarnings("rawtypes")
    private static Rule[] esSourceRules(boolean optimizeForEsSource, boolean first) {
        List<Rule<?, PhysicalPlan>> esSourceRules = new ArrayList<>(8);
        esSourceRules.add(new ReplaceSourceAttributes());
        if (optimizeForEsSource) {
            if (first) {
                // This rule should only be applied once, since it is not idempotent.
                esSourceRules.add(new RemoveProjectAfterTopN());
            }
            esSourceRules.add(new PushTopNToSource());
            esSourceRules.add(new PushLimitToSource());
            esSourceRules.add(new PushFiltersToSource());
            esSourceRules.add(new PushSampleToSource());
            esSourceRules.add(new PushStatsToSource());
            esSourceRules.add(new EnableSpatialDistancePushdown());
        }
        return esSourceRules.toArray(Rule[]::new);
    }
}
