/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.EnableSpatialDistancePushdown;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushLimitToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushStatsToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.SpatialDocValuesExtraction;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsqlTranslatorHandler;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Arrays.asList;

/**
 * Manages field extraction and pushing parts of the query into Lucene. (Query elements that are not pushed into Lucene are executed via
 * the compute engine)
 */
public class LocalPhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, LocalPhysicalOptimizerContext> {
    public static final EsqlTranslatorHandler TRANSLATOR_HANDLER = new EsqlTranslatorHandler();

    private final PhysicalVerifier verifier = PhysicalVerifier.INSTANCE;

    public LocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context) {
        super(context);
    }

    public PhysicalPlan localOptimize(PhysicalPlan plan) {
        return verify(execute(plan));
    }

    PhysicalPlan verify(PhysicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    protected List<Batch<PhysicalPlan>> rules(boolean optimizeForEsSource) {
        List<Rule<?, PhysicalPlan>> esSourceRules = new ArrayList<>(4);
        esSourceRules.add(new ReplaceSourceAttributes());

        if (optimizeForEsSource) {
            esSourceRules.add(new PushTopNToSource());
            esSourceRules.add(new PushLimitToSource());
            esSourceRules.add(new PushFiltersToSource());
            esSourceRules.add(new PushStatsToSource());
            esSourceRules.add(new EnableSpatialDistancePushdown());
        }

        // execute the rules multiple times to improve the chances of things being pushed down
        @SuppressWarnings("unchecked")
        var pushdown = new Batch<PhysicalPlan>("Push to ES", esSourceRules.toArray(Rule[]::new));
        // add the field extraction in just one pass
        // add it at the end after all the other rules have ran
        var fieldExtraction = new Batch<>("Field extraction", Limiter.ONCE, new InsertFieldExtraction(), new SpatialDocValuesExtraction());
        return asList(pushdown, fieldExtraction);
    }

    @Override
    protected List<Batch<PhysicalPlan>> batches() {
        return rules(true);
    }

    /**
     * this method is supposed to be used to define if a field can be used for exact push down (eg. sort or filter).
     * "aggregatable" is the most accurate information we can have from field_caps as of now.
     * Pushing down operations on fields that are not aggregatable would result in an error.
     */
    public static boolean isAggregatable(FieldAttribute f) {
        return f.exactAttribute().field().isAggregatable();
    }

    public static boolean canPushSorts(PhysicalPlan plan) {
        if (plan instanceof EsQueryExec queryExec) {
            return queryExec.canPushSorts();
        }
        if (plan instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
            return queryExec.canPushSorts();
        }
        return false;
    }

    public static boolean hasIdenticalDelegate(FieldAttribute attr, SearchStats stats) {
        return stats.hasIdenticalDelegate(attr.name());
    }

    public static boolean isPushableFieldAttribute(Expression exp, Predicate<FieldAttribute> hasIdenticalDelegate) {
        if (exp instanceof FieldAttribute fa && fa.getExactInfo().hasExact() && isAggregatable(fa)) {
            return fa.dataType() != DataType.TEXT || hasIdenticalDelegate.test(fa);
        }
        return false;
    }
}
