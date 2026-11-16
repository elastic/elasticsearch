/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.QueryTimeRangeExtractor.TimeRange;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ViewBoundaryPreservation;
import org.elasticsearch.xpack.esql.view.ViewCompaction;
import org.elasticsearch.xpack.esql.view.ViewResolver;
import org.junit.After;
import org.junit.Before;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@link QueryTimeRangeExtractor} on queries that read views. The plans go through what a query does before its indices are
 * resolved: the views are replaced by their definitions and the unions are turned into branches. Each query is checked with
 * every way of keeping the boundaries of the views in the plan, which the request picks by its filter and its settings.
 */
public class QueryTimeRangeExtractorViewTests extends ESTestCase {

    private static final long NOW = TEST_CFG.now().toEpochMilli();
    private static final long T1 = Instant.parse("2024-05-01T00:00:00Z").toEpochMilli();
    private static final long T2 = Instant.parse("2024-05-02T00:00:00Z").toEpochMilli();
    private static final String FROM_T1 = "@timestamp >= \"2024-05-01T00:00:00Z\"";
    private static final String TO_T2 = "@timestamp <= \"2024-05-02T00:00:00Z\"";

    private InMemoryViewService viewService;

    @Before
    public void createViewService() {
        viewService = InMemoryViewService.makeViewService();
        for (String index : new String[] { "logs", "logs-1", "logs-2", "metrics", "traces" }) {
            viewService.addIndex(ProjectId.DEFAULT, index);
        }
    }

    @After
    public void closeViewService() {
        viewService.close();
    }

    public void testFilterOnView() {
        addView("all_logs", "FROM logs-*");
        assertRanges("FROM all_logs | WHERE " + FROM_T1, Map.of("logs-*", new TimeRange(T1, null)));
        assertRanges("FROM all_logs | WHERE level == \"error\"", Map.of("logs-*", TimeRange.UNBOUNDED));
    }

    /** The query of the issue: a view that ends in a wildcard DROP over a wide pattern, with the time range in the query. */
    public void testViewWithWildcardDrop() {
        addView("collectors", "FROM logs-*, metrics METADATA _id, _index | WHERE service IN (\"ingest\", \"index\") | DROP lost.*");
        assertRanges(
            "FROM collectors | WHERE @timestamp >= NOW() - 15 minutes AND @timestamp <= NOW() | WHERE dropped IS NOT NULL "
                + "| KEEP @timestamp, message | SORT @timestamp ASC | LIMIT 1",
            Map.of("logs-*,metrics", new TimeRange(NOW - Duration.ofMinutes(15).toMillis(), NOW))
        );
    }

    public void testViewWithItsOwnFilter() {
        addView("recent_logs", "FROM logs | WHERE " + FROM_T1 + " | EVAL a = 1");
        assertRanges("FROM recent_logs", Map.of("logs", new TimeRange(T1, null)));
        assertRanges("FROM recent_logs | WHERE " + TO_T2, Map.of("logs", new TimeRange(T1, T2)));
    }

    /** A filter on the output of these views does not filter the rows that the view reads. */
    public void testViewsThatResetTheRange() {
        addView("counts", "FROM logs | STATS c = COUNT(*) BY @timestamp");
        addView("sample", "FROM logs | LIMIT 100");
        addView("shifted", "FROM logs | EVAL @timestamp = @timestamp + 1 hour");
        addView("renamed", "FROM logs | RENAME ingested AS @timestamp");
        for (String view : new String[] { "counts", "sample", "shifted", "renamed" }) {
            assertRanges("FROM " + view + " | WHERE " + FROM_T1, Map.of("logs", TimeRange.UNBOUNDED));
        }
        // The filter of the view itself still applies when it comes before such a command.
        addView("recent_counts", "FROM logs | WHERE " + FROM_T1 + " | STATS c = COUNT(*) BY @timestamp");
        assertRanges("FROM recent_counts | WHERE " + TO_T2, Map.of("logs", new TimeRange(T1, null)));
    }

    public void testNestedViews() {
        addView("inner_view", "FROM logs | WHERE " + FROM_T1);
        addView("middle_view", "FROM inner_view | DROP lost.*");
        addView("outer_view", "FROM middle_view | WHERE " + TO_T2 + " | KEEP @timestamp, message");
        assertRanges("FROM outer_view", Map.of("logs", new TimeRange(T1, T2)));
        assertRanges("FROM outer_view | WHERE @timestamp >= \"2024-05-02T00:00:00Z\"", Map.of("logs", new TimeRange(T2, T2)));
        assertRanges("FROM middle_view | WHERE level == \"error\"", Map.of("logs", new TimeRange(T1, null)));
    }

    public void testViewNextToIndex() {
        addView("recent_logs", "FROM logs | WHERE " + FROM_T1);
        assertRanges(
            "FROM recent_logs, metrics | WHERE " + TO_T2,
            Map.of("logs", new TimeRange(T1, T2), "metrics", new TimeRange(null, T2))
        );
        assertRanges("FROM recent_logs, metrics", Map.of("logs", new TimeRange(T1, null), "metrics", TimeRange.UNBOUNDED));
    }

    public void testSeveralViews() {
        addView("recent_logs", "FROM logs | WHERE " + FROM_T1);
        addView("metric_counts", "FROM metrics | STATS c = COUNT(*) BY @timestamp");
        addView("old_traces", "FROM traces | WHERE " + TO_T2);
        assertRanges(
            "FROM recent_logs, metric_counts, old_traces | WHERE " + FROM_T1,
            Map.of("logs", new TimeRange(T1, null), "metrics", TimeRange.UNBOUNDED, "traces", new TimeRange(T1, T2))
        );
    }

    /** An index gets the widest of the ranges that the views and the query read it with, none if one of them has none. */
    public void testViewsOverTheSameIndex() {
        addView("recent_logs", "FROM logs | WHERE @timestamp >= \"2024-05-02T00:00:00Z\"");
        addView("error_logs", "FROM logs | WHERE level == \"error\"");
        addView("day_logs", "FROM logs | WHERE " + FROM_T1 + " AND " + TO_T2);
        assertRanges("FROM recent_logs, error_logs", Map.of("logs", TimeRange.UNBOUNDED));
        assertRanges("FROM recent_logs, day_logs", Map.of("logs", new TimeRange(T1, null)));
        assertRanges("FROM recent_logs, error_logs | WHERE " + FROM_T1, Map.of("logs", new TimeRange(T1, null)));
        assertRanges("FROM recent_logs, logs", Map.of("logs", TimeRange.UNBOUNDED));
    }

    public void testViewInSubquery() {
        addView("recent_logs", "FROM logs | WHERE " + FROM_T1);
        assertRanges(
            "FROM (FROM recent_logs | WHERE level == \"error\"), (FROM metrics | LIMIT 5) | WHERE " + TO_T2,
            Map.of("logs", new TimeRange(T1, T2), "metrics", TimeRange.UNBOUNDED)
        );
    }

    private void addView(String name, String query) {
        PlainActionFuture<Object> future = new PlainActionFuture<>();
        viewService.putView(
            ProjectId.DEFAULT,
            new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query, null)),
            future.map(response -> response)
        );
        future.actionGet(TEST_REQUEST_TIMEOUT);
    }

    private void assertRanges(String query, Map<String, TimeRange> expected) {
        for (ViewBoundaryPreservation boundaryPreservation : ViewBoundaryPreservation.values()) {
            Map<String, TimeRange> ranges = new HashMap<>();
            QueryTimeRangeExtractor.ranges(replaceViews(query, boundaryPreservation), TEST_CFG)
                .forEach((pattern, range) -> ranges.merge(pattern.indexPattern(), range, TimeRange::cover));
            assertThat(query + " with " + boundaryPreservation, ranges, equalTo(expected));
        }
    }

    /** The plan that the session extracts the time ranges from. */
    private LogicalPlan replaceViews(String query, ViewBoundaryPreservation boundaryPreservation) {
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        viewService.getViewResolver()
            .replaceViews(TEST_PARSER.parseQuery(query), null, true, false, this::parseView, boundaryPreservation, future);
        return ViewCompaction.preIndexResolution(future.actionGet(TEST_REQUEST_TIMEOUT).plan());
    }

    private EsqlStatement parseView(String query, String viewName) {
        return TEST_PARSER.parseView(
            query,
            new QueryParams(),
            new InferenceSettings(Settings.EMPTY),
            viewName,
            QueryPragmas.EMPTY,
            EsqlFlags.DEFAULTS
        );
    }
}
