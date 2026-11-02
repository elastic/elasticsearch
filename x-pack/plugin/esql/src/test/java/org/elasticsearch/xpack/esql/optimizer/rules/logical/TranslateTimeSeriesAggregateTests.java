/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;

public class TranslateTimeSeriesAggregateTests extends ESTestCase {
    /** Native source dimensions still use the aggregate eligible for late source extraction. */
    public void testNativeTsSourceDimensionsRetainCombinedAggregate() {
        LogicalPlan plan = analyzer().addK8s()
            .minimumTransportVersion(TransportVersion.current())
            .query("TS k8s | STATS value = SUM(LAST_OVER_TIME(network.cost)) BY region | LIMIT 100");
        TimeSeriesAggregate firstPass = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertTrue(
            firstPass.toString(),
            firstPass.aggregates().stream().map(Alias::unwrap).anyMatch(expression -> expression instanceof PackDimsAgg)
        );
    }

    /** Native TS computed grouping keys keep their ordinary multivalue semantics. */
    public void testNativeTsComputedGroupingDoesNotBecomeDimension() {
        LogicalPlan plan = analyzer().addK8s()
            .query(
                "TS k8s | EVAL label = MV_DIFFERENCE(region, \"\") "
                    + "| STATS value = SUM(LAST_OVER_TIME(network.cost)) BY label | LIMIT 100"
            );
        assertTrue(plan.toString(), plan.collect(UnpackDims.class).isEmpty());
        TimeSeriesAggregate firstPass = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertTrue(
            firstPass.toString(),
            firstPass.aggregates()
                .stream()
                .map(Alias::unwrap)
                .anyMatch(expression -> expression instanceof Values values && ((Attribute) values.field()).name().equals("label"))
        );
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(failures.toString(), failures.hasFailures());
    }

    public void testExplicitTsidGroupingUsesFirstPassKey() {
        for (String grouping : List.of("_tsid", "series = _tsid", "_tsid, step = TBUCKET(1 minute)")) {
            LogicalPlan plan = analyzer().addK8s()
                .query("TS k8s METADATA _tsid | STATS value = MAX(LAST_OVER_TIME(network.cost)) BY " + grouping + " | LIMIT 100");
            Failures failures = new Failures();
            plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
            assertFalse(grouping + ": " + failures, failures.hasFailures());

            TimeSeriesAggregate firstPass = plan.collect(TimeSeriesAggregate.class).getFirst();
            Attribute tsid = firstPass.output()
                .stream()
                .filter(attribute -> attribute.name().equals(MetadataAttribute.TSID_FIELD))
                .findFirst()
                .orElseThrow();
            assertTrue(
                plan.collect(Aggregate.class)
                    .stream()
                    .filter(aggregate -> aggregate instanceof TimeSeriesAggregate == false)
                    .anyMatch(aggregate -> aggregate.groupings().stream().anyMatch(key -> Alias.unwrap(key).semanticEquals(tsid)))
            );
            firstPass.forEachExpressionDown(Values.class, values -> assertFalse(values.field().semanticEquals(tsid)));
        }
    }
}
