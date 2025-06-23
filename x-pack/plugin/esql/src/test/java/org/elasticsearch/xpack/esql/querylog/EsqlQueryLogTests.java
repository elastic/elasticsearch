/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.MockAppender;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.Result;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_PLANNING_TOOK;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_PLANNING_TOOK_MILLIS;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_QUERY;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK_MILLIS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class EsqlQueryLogTests extends ESTestCase {
    private static ClusterSettings settings = new ClusterSettings(
        Settings.builder()
            .put(EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_WARN_SETTING.getKey(), "40ms")
            .put(EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_INFO_SETTING.getKey(), "30ms")
            .put(EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING.getKey(), "20ms")
            .put(EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING.getKey(), "10ms")
            .put(EsqlPlugin.ESQL_QUERYLOG_INCLUDE_USER_SETTING.getKey(), true)

            .build(),
        new HashSet<>(
            Arrays.asList(
                EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_WARN_SETTING,
                EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_INFO_SETTING,
                EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING,
                EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING,
                EsqlPlugin.ESQL_QUERYLOG_INCLUDE_USER_SETTING
            )
        )
    );

    static MockAppender appender;
    static Logger queryLog = LogManager.getLogger(EsqlQueryLog.LOGGER_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("test_appender");
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

    public void testPrioritiesOnSuccess() {
        EsqlQueryLog queryLog = new EsqlQueryLog(settings, mockFieldProvider());
        String query = "from " + randomAlphaOfLength(10);

        long[] actualTook = {
            randomLongBetween(10_000_000, 20_000_000),
            randomLongBetween(20_000_000, 30_000_000),
            randomLongBetween(30_000_000, 40_000_000),
            randomLongBetween(40_000_000, 50_000_000),
            randomLongBetween(0, 9_999_999) };
        long[] actualPlanningTook = {
            randomLongBetween(0, 1_000_000),
            randomLongBetween(0, 1_000_000),
            randomLongBetween(0, 1_000_000),
            randomLongBetween(0, 1_000_000),
            randomLongBetween(0, 1_000_000), };
        Level[] expectedLevel = { Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, null };

        for (int i = 0; i < actualTook.length; i++) {
            EsqlExecutionInfo warnQuery = getEsqlExecutionInfo(actualTook[i], actualPlanningTook[i]);
            queryLog.onQueryPhase(new Result(List.of(), List.of(), DriverCompletionInfo.EMPTY, warnQuery), query);
            if (expectedLevel[i] != null) {
                assertThat(appender.lastEvent(), is(not(nullValue())));
                var msg = (ESLogMessage) appender.lastMessage();
                long took = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK));
                long tookMillisExpected = took / 1_000_000L;
                long tookMillis = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS));
                assertThat(took, is(actualTook[i]));
                assertThat(tookMillis, is(tookMillisExpected));

                long planningTook = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_PLANNING_TOOK));
                long planningTookMillisExpected = planningTook / 1_000_000;
                long planningTookMillis = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_PLANNING_TOOK_MILLIS));
                assertThat(planningTook, is(actualPlanningTook[i]));
                assertThat(planningTookMillis, is(planningTookMillisExpected));
                assertThat(took, greaterThan(planningTook));
                assertThat(msg.get(ELASTICSEARCH_QUERYLOG_QUERY), is(query));
                assertThat(appender.getLastEventAndReset().getLevel(), equalTo(expectedLevel[i]));
            } else {
                assertThat(appender.lastEvent(), is(nullValue()));
            }

        }
    }

    private SlowLogFieldProvider mockFieldProvider() {
        return new SlowLogFieldProvider() {
            @Override
            public SlowLogFields create(IndexSettings indexSettings) {
                return create();
            }

            @Override
            public SlowLogFields create() {
                return new SlowLogFields() {
                    @Override
                    public Map<String, String> indexFields() {
                        return Map.of();
                    }

                    @Override
                    public Map<String, String> searchFields() {
                        return Map.of();
                    }
                };
            }
        };
    }

    public void testPrioritiesOnFailure() {
        EsqlQueryLog queryLog = new EsqlQueryLog(settings, mockFieldProvider());
        String query = "from " + randomAlphaOfLength(10);

        long[] actualTook = {
            randomLongBetween(10_000_000, 20_000_000),
            randomLongBetween(20_000_000, 30_000_000),
            randomLongBetween(30_000_000, 40_000_000),
            randomLongBetween(40_000_000, 50_000_000),
            randomLongBetween(0, 9_999_999) };

        Level[] expectedLevel = { Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, null };

        String validationError = randomAlphaOfLength(10);
        ValidationException ex = new ValidationException().addValidationError(validationError);
        for (int i = 0; i < actualTook.length; i++) {
            queryLog.onQueryFailure(query, ex, actualTook[i]);
            if (expectedLevel[i] != null) {
                assertThat(appender.lastEvent(), is(not(nullValue())));
                var msg = (ESLogMessage) appender.lastMessage();
                long took = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK));
                long tookMillisExpected = took / 1_000_000L;
                long tookMillis = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS));
                assertThat(took, is(actualTook[i]));
                assertThat(tookMillis, is(tookMillisExpected));
                assertThat(msg.get(ELASTICSEARCH_QUERYLOG_PLANNING_TOOK), is(nullValue()));
                assertThat(msg.get(ELASTICSEARCH_QUERYLOG_PLANNING_TOOK_MILLIS), is(nullValue()));
                assertThat(msg.get(ELASTICSEARCH_QUERYLOG_QUERY), is(query));
                assertThat(appender.getLastEventAndReset().getLevel(), equalTo(expectedLevel[i]));
            } else {
                assertThat(appender.lastEvent(), is(nullValue()));
            }
        }
    }

    private static EsqlExecutionInfo getEsqlExecutionInfo(long tookNanos, long planningTookNanos) {
        EsqlExecutionInfo info = new EsqlExecutionInfo(false) {
            @Override
            public TimeValue overallTook() {
                return new TimeValue(tookNanos, TimeUnit.NANOSECONDS);
            }

            @Override
            public TimeValue planningTookTime() {
                return new TimeValue(planningTookNanos, TimeUnit.NANOSECONDS);
            }
        };
        return info;
    }
}
