/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceChangePointBucketFill;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogatePlans;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.ChangePointFillEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ChangePointBucketSpecExtractorTests extends ESTestCase {

    public void testExtractAfterSurrogateSubstitution() {
        LogicalPlan afterSurrogate = afterSurrogateSubstitution();
        ChangePoint changePoint = findChangePoint(afterSurrogate);
        assertThat(changePoint, notNullValue());
        assertThat(changePoint.child().getClass().getSimpleName(), is("Limit"));

        Optional<ChangePointBucketSpecExtractor.ChangePointBucketFillSpec> spec = ChangePointBucketSpecExtractor.extract(
            changePoint,
            context(bounds())
        );
        assertTrue("expected spec, plan:\n" + afterSurrogate, spec.isPresent());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testFillInsertedAfterSurrogateOnly() {
        LogicalPlan plan = new ReplaceChangePointBucketFill().apply(afterSurrogateSubstitution(), context(bounds()));

        List<LogicalPlan> fills = new ArrayList<>();
        plan.forEachDown(ChangePointFillEmptyBuckets.class, fills::add);
        assertFalse("expected fill after surrogate only, plan:\n" + plan, fills.isEmpty());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testFillInsertedAfterSubstitutionsBatch() {
        LogicalPlan plan = new TestSubstitutionOnlyOptimizer(context(bounds())) {
            LogicalPlan run(LogicalPlan analyzed) {
                return execute(analyzed);
            }
        }.run(analyzedPlan());

        List<LogicalPlan> fills = new ArrayList<>();
        plan.forEachDown(ChangePointFillEmptyBuckets.class, fills::add);
        assertFalse("expected fill after substitutions batch, plan:\n" + plan, fills.isEmpty());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    private static LogicalPlan analyzedPlan() {
        return EsqlTestUtils.analyzer().addK8s().query("""
            FROM k8s
            | STATS c = COUNT() BY @timestamp = BUCKET(@timestamp, 1 MINUTE)
            | CHANGE_POINT c ON @timestamp
            """);
    }

    private static LogicalPlan afterSurrogateSubstitution() {
        return new SubstituteSurrogatePlans().apply(analyzedPlan());
    }

    private static TimestampBounds bounds() {
        return new TimestampBounds(Instant.parse("2024-05-10T00:00:00.000Z"), Instant.parse("2024-05-10T01:00:00.000Z"));
    }

    private static LogicalOptimizerContext context(TimestampBounds bounds) {
        return new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current(), bounds);
    }

    private static ChangePoint findChangePoint(LogicalPlan plan) {
        List<ChangePoint> changePoints = new ArrayList<>();
        plan.forEachDown(ChangePoint.class, changePoints::add);
        assertThat(changePoints.size(), is(1));
        return changePoints.getFirst();
    }
}
