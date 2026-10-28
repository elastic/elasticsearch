/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.test.ActivityLoggingUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_INDICES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_PARAMS;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageFailure;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration test verifying that PromQL queries submitted via {@link PromqlQueryRequest}
 * are logged with {@code type = "promql"} rather than the default {@code "esql"} type,
 * and that the index and query expression are surfaced correctly in the activity log.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 1)
public class PromqlQueryLoggingIT extends AbstractEsqlIntegTestCase {
    private static final String PROMQL_TYPE = PromqlQueryRequest.PROMQL_TYPE;
    private static final String TEST_INDEX = "metrics-*";

    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(QueryLogging.QUERY_LOGGER_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new AccumulatingMockAppender("promql_trace_appender");
        appender.start();
        Loggers.addAppender(queryLog, appender);
        Loggers.setLevel(queryLog, Level.TRACE);
    }

    @AfterClass
    public static void cleanup() {
        Loggers.removeAppender(queryLog, appender);
        appender.stop();
        Loggers.setLevel(queryLog, origQueryLogLevel);
    }

    @Before
    public void enableLog() {
        ActivityLoggingUtils.enableLoggers();
        appender.reset();
    }

    @After
    public void disableLog() {
        ActivityLoggingUtils.disableLoggers();
    }

    public void testLogging() {
        String promqlExpr = "3.14";
        var result = PromqlQueryPlanBuilder.buildStatement(
            promqlExpr,
            TEST_INDEX,
            Instant.ofEpochSecond(1_000_000),
            PrometheusQueryResponseListener.QueryMode.INSTANT
        );
        var request = new PromqlQueryRequest(TEST_INDEX, result.esqlStatement(), promqlExpr);
        try (var ignored = run(request)) {
            var event = appender.getLastEventAndReset();
            Map<String, String> message = getMessageData(event);
            assertMessageSuccess(message, PROMQL_TYPE, promqlExpr);
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo(TEST_INDEX));
            assertNull(getMessageField(event, QUERY_FIELD_PARAMS));
        }
    }

    @SuppressWarnings("unchecked")
    public void testLoggingParams() {
        String promqlExpr = "3.14";
        var result = PromqlQueryPlanBuilder.buildStatement(
            promqlExpr,
            TEST_INDEX,
            Instant.ofEpochSecond(1_000_000),
            PrometheusQueryResponseListener.QueryMode.INSTANT
        );
        var request = new PromqlQueryRequest(TEST_INDEX, result.esqlStatement(), promqlExpr, "metric_name", "http_requests", "limit", 100);
        try (var ignored = run(request)) {
            var event = appender.getLastEventAndReset();
            var message = getMessageData(event);
            assertMessageSuccess(message, PROMQL_TYPE, promqlExpr);
            var params = (Map<String, Object>) getMessageField(event, QUERY_FIELD_PARAMS);
            assertNotNull(params);
            assertThat(params.values(), hasSize(2));
            assertThat(params.get("metric_name"), equalTo("http_requests"));
            assertThat(params.get("limit"), equalTo("100"));
        }

        var request2 = new PromqlQueryRequest(
            TEST_INDEX,
            result.esqlStatement(),
            promqlExpr,
            "metric_name",
            "http_requests",
            "limit",
            null,
            "emptyList",
            List.of()
        );
        try (var ignored = run(request2)) {
            var event = appender.getLastEventAndReset();
            var message = getMessageData(event);
            assertMessageSuccess(message, PROMQL_TYPE, promqlExpr);
            var params = (Map<String, Object>) getMessageField(event, QUERY_FIELD_PARAMS);
            assertNotNull(params);
            assertThat(params.values(), hasSize(1));
            assertThat(params.get("metric_name"), equalTo("http_requests"));
        }
    }

    public void testLoggingFailure() {
        String promqlExpr = "1 and 1";
        var result = PromqlQueryPlanBuilder.buildStatement(
            promqlExpr,
            TEST_INDEX,
            Instant.ofEpochSecond(1_000_000),
            PrometheusQueryResponseListener.QueryMode.INSTANT
        );
        var request = new PromqlQueryRequest(TEST_INDEX, result.esqlStatement(), promqlExpr);
        expectThrows(VerificationException.class, () -> run(request));
        Map<String, String> message = getMessageData(appender.getLastEventAndReset());
        assertMessageFailure(message, PROMQL_TYPE, promqlExpr, VerificationException.class, "Found 1 problem");
    }
}
