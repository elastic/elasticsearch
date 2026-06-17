/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;

import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests that {@link ChangePoint#keyBucketMetadata} correctly surfaces bucket interval metadata
 * from the upstream STATS BY clause, including the {@code "start"} and {@code "end"} epoch-millis
 * introduced for the 4-arg date form.
 */
public class ChangePointKeyBucketMetadataTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * A 4-arg date BUCKET in the upstream STATS BY should surface interval, unit, start, and end.
     */
    public void testFindsStartEndFromFourArgDateBucket() {
        String query = """
            FROM test
            | STATS c=COUNT(*) BY b=BUCKET(hire_date, 20, "1985-01-01T00:00:00Z", "1995-01-01T00:00:00Z")
            | CHANGE_POINT c ON b
            """;

        LogicalPlan optimized = optimizedPlan(query);

        ChangePoint changePoint = findChangePoint(optimized);

        Map<String, Object> meta = changePoint.keyBucketMetadata(FoldContext.small());
        assertThat(meta, notNullValue());

        @SuppressWarnings("unchecked")
        Map<String, Object> inner = (Map<String, Object>) meta.get("bucket");
        assertThat(inner, notNullValue());
        assertThat(inner, hasKey("interval"));
        assertThat(inner, hasKey("unit"));

        long expectedStart = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("1985-01-01T00:00:00Z");
        long expectedEnd = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("1995-01-01T00:00:00Z");
        assertThat(inner, hasEntry("start", (Object) expectedStart));
        assertThat(inner, hasEntry("end", (Object) expectedEnd));
    }

    /**
     * A 2-arg date BUCKET in the upstream STATS BY should surface interval and unit only — no start or end.
     */
    public void testReturnsIntervalOnlyForTwoArgDateBucket() {
        String query = """
            FROM test
            | STATS c=COUNT(*) BY b=BUCKET(hire_date, 1 year)
            | CHANGE_POINT c ON b
            """;

        LogicalPlan optimized = optimizedPlan(query);

        ChangePoint changePoint = findChangePoint(optimized);

        Map<String, Object> meta = changePoint.keyBucketMetadata(FoldContext.small());
        assertThat(meta, notNullValue());

        @SuppressWarnings("unchecked")
        Map<String, Object> inner = (Map<String, Object>) meta.get("bucket");
        assertThat(inner, notNullValue());
        assertThat(inner, hasKey("interval"));
        assertThat(inner, hasKey("unit"));
        assertThat(inner, not(hasKey("start")));
        assertThat(inner, not(hasKey("end")));
    }

    /**
     * When the CHANGE_POINT key is not backed by a BUCKET (just a plain field), metadata is null.
     */
    public void testReturnsNullForNonBucketKey() {
        String query = """
            FROM test
            | STATS c=COUNT(*) BY hire_date
            | CHANGE_POINT c ON hire_date
            """;

        LogicalPlan optimized = optimizedPlan(query);

        ChangePoint changePoint = findChangePoint(optimized);

        Map<String, Object> meta = changePoint.keyBucketMetadata(FoldContext.small());
        assertThat(meta, nullValue());
    }

    private static ChangePoint findChangePoint(LogicalPlan plan) {
        LogicalPlan node = plan.collectFirstChildren(ChangePoint.class::isInstance).stream().findFirst().orElse(null);
        assertThat("no ChangePoint node found in plan", node, notNullValue());
        return (ChangePoint) node;
    }
}
