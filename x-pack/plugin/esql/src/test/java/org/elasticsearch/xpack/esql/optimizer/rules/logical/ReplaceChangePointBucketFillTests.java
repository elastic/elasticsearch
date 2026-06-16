/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.ChangePointFillEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ReplaceChangePointBucketFillTests extends ESTestCase {

    public void testInsertsFillForBucketWithTimestampBounds() {
        TimestampBounds bounds = bounds();
        LogicalPlan plan = analyzedPlan(bounds);
        LogicalPlan optimized = optimize(plan, bounds);

        List<LogicalPlan> fills = new ArrayList<>();
        optimized.forEachDown(ChangePointFillEmptyBuckets.class, fills::add);
        if (fills.isEmpty()) {
            fail("expected ChangePointFillEmptyBuckets in plan:\n" + optimized);
        }
        assertThat(fills.size(), equalTo(1));
        assertTrue(((ChangePointFillEmptyBuckets) fills.getFirst()).child() instanceof TopN);
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testInsertsFillWithTopNByForGroupedBucket() {
        TimestampBounds bounds = bounds();
        LogicalPlan plan = EsqlTestUtils.analyzer().addK8s().timestampBounds(bounds).query("""
            FROM k8s
            | STATS c = COUNT() BY @timestamp = BUCKET(@timestamp, 1 MINUTE), cluster
            | CHANGE_POINT c ON @timestamp BY cluster
            """);
        LogicalPlan optimized = optimize(plan, bounds);

        List<LogicalPlan> fills = new ArrayList<>();
        optimized.forEachDown(ChangePointFillEmptyBuckets.class, fills::add);
        if (fills.isEmpty()) {
            fail("expected ChangePointFillEmptyBuckets in plan:\n" + optimized);
        }
        assertThat(fills.size(), equalTo(1));
        assertTrue(((ChangePointFillEmptyBuckets) fills.getFirst()).child() instanceof TopNBy);
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testSkipsTBucket() {
        TimestampBounds bounds = bounds();
        LogicalPlan plan = EsqlTestUtils.analyzer().addK8s().timestampBounds(bounds).query("""
            FROM k8s
            | STATS c = COUNT() BY @timestamp = TBUCKET(60)
            | CHANGE_POINT c ON @timestamp
            """);

        LogicalPlan optimized = optimize(plan, bounds);
        List<LogicalPlan> fills = new ArrayList<>();
        optimized.forEachDown(ChangePointFillEmptyBuckets.class, fills::add);
        assertTrue(fills.isEmpty());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testWarnsAndSkipsFillWhenTimerangeTooShort() {
        TimestampBounds bounds = new TimestampBounds(Instant.parse("2024-05-10T00:00:00.000Z"), Instant.parse("2024-05-10T00:15:00.000Z"));
        LogicalPlan plan = analyzedPlan(bounds);

        LogicalPlan optimized = optimize(plan, bounds);
        List<LogicalPlan> fills = new ArrayList<>();
        optimized.forEachDown(ChangePointFillEmptyBuckets.class, fills::add);
        assertTrue(fills.isEmpty());
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 3:3: CHANGE_POINT bucket zero-fill skipped; the requested time range and bucket interval allow only [15] buckets but at least [22] are required"
        );
    }

    private static LogicalPlan analyzedPlan(TimestampBounds bounds) {
        return EsqlTestUtils.analyzer().addK8s().timestampBounds(bounds).query("""
            FROM k8s
            | STATS c = COUNT() BY @timestamp = BUCKET(@timestamp, 1 MINUTE)
            | CHANGE_POINT c ON @timestamp
            """);
    }

    private static TimestampBounds bounds() {
        return new TimestampBounds(Instant.parse("2024-05-10T00:00:00.000Z"), Instant.parse("2024-05-10T01:00:00.000Z"));
    }

    private static LogicalPlan optimize(LogicalPlan plan, TimestampBounds bounds) {
        LogicalOptimizerContext context = new LogicalOptimizerContext(
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            TransportVersion.current(),
            bounds
        );
        return new LogicalPlanOptimizer(context).optimize(plan);
    }
}
