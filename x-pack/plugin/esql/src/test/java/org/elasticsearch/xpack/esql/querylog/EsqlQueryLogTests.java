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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.MockAppender;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryProfile;
import org.elasticsearch.xpack.esql.action.TimeSpan;
import org.elasticsearch.xpack.esql.action.TimeSpanMarker;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.fetch.FetchPhaseStats;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhaseOutcomes;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.view.ViewQuerySettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_FETCH_PHASE_DOCUMENTS;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_FETCH_PHASE_NODES;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_FETCH_PHASE_OUTCOME;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_FETCH_PHASE_REQUESTS;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_FETCH_PHASE_TOOK_MILLIS;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_PREFIX;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_QUERY;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_SETTINGS_PREFIX;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK_MILLIS;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK_MILLIS_SUFFIX;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_TOOK_SUFFIX;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLog.ELASTICSEARCH_QUERYLOG_VIEW_SETTINGS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class EsqlQueryLogTests extends ESTestCase {
    private static final ResolvedSettings RESOLVED_SETTINGS = ResolvedSettings.EMPTY.withOverride(
        QuerySettings.APPROXIMATION,
        new ApproximationSettings(123456, 0.789)
    )
        .withOverride(QuerySettings.PROJECT_ROUTING, "_alias:_origin")
        .withOverride(QuerySettings.UNMAPPED_FIELDS, UnmappedResolution.NULLIFY)
        .withOverride(QuerySettings.TIME_ZONE, ZoneId.of("Europe/Paris"));
    /** One view with a SET and one without, both logged apart from the caller's settings. */
    private static final Map<String, ViewQuerySettings> VIEW_SETTINGS = Map.of(
        "nullifying_view",
        new ViewQuerySettings(UnmappedResolution.NULLIFY, true, false, Set.of("unmapped_fields", "wildcards_match_views")),
        "plain_view",
        ViewQuerySettings.DEFAULTS
    );
    /** Above every threshold of {@link #settings}, so the query log logs it. */
    private static final long SLOW_QUERY_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

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

    public static ActionLoggingFieldsProvider mockLogFieldProvider() {
        return context -> new ActionLoggingFields(context) {};
    }

    private static Configuration configurationWith(ResolvedSettings settings) {
        Configuration configuration = EsqlTestUtils.TEST_CFG;
        for (QuerySettingDef<?> def : QuerySettings.all()) {
            configuration = withSetting(configuration, def, settings);
        }
        return configuration;
    }

    private static <T> Configuration withSetting(Configuration configuration, QuerySettingDef<T> def, ResolvedSettings settings) {
        return configuration.withSetting(def, def.get(settings));
    }

    private static void assertResolvedSettingsLogged(ESLogMessage msg) {
        assertThat(
            msg.get(ELASTICSEARCH_QUERYLOG_SETTINGS_PREFIX + ".approximation"),
            is("ApproximationSettings[rows=123456, confidenceLevel=0.789]")
        );
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_SETTINGS_PREFIX + ".time_zone"), is("Europe/Paris"));
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_SETTINGS_PREFIX + ".project_routing"), is("_alias:_origin"));
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_SETTINGS_PREFIX + ".unmapped_fields"), is("NULLIFY"));
    }

    private static Map<String, ViewQuerySettings> randomViewSettings() {
        return randomFrom(VIEW_SETTINGS, Map.of(), Map.of("plain_view", ViewQuerySettings.DEFAULTS));
    }

    private static void assertViewSettingsLogged(ESLogMessage msg, Map<String, ViewQuerySettings> viewSettings) {
        if (viewSettings.isEmpty()) {
            assertThat(msg.getData(), not(hasKey(ELASTICSEARCH_QUERYLOG_VIEW_SETTINGS)));
            return;
        }
        // every resolved view, with what its definition SETs and the default for the rest
        Map<String, Map<String, String>> expected = new HashMap<>();
        expected.put(
            "plain_view",
            Map.of("unmapped_fields", "DEFAULT", "wildcards_match_views", "false", "wildcards_match_datasets", "false")
        );
        if (viewSettings.containsKey("nullifying_view")) {
            expected.put(
                "nullifying_view",
                Map.of("unmapped_fields", "NULLIFY", "wildcards_match_views", "true", "wildcards_match_datasets", "false")
            );
        }
        assertThat(msg.getData().get(ELASTICSEARCH_QUERYLOG_VIEW_SETTINGS), equalTo(expected));
        // the caller's settings fields are untouched by the views' own
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_SETTINGS_PREFIX + ".unmapped_fields"), is("NULLIFY"));
    }

    public void testPrioritiesOnSuccess() {
        EsqlQueryLog queryLog = new EsqlQueryLog(settings, mockLogFieldProvider());
        assertWarnings(EsqlQueryLog.DEPRECATION_MESSAGE);
        String query = "from " + randomAlphaOfLength(10);
        Configuration configuration = configurationWith(RESOLVED_SETTINGS);
        Map<String, ViewQuerySettings> viewSettings = randomViewSettings();

        long[] actualTook = {
            randomLongBetween(10_000_000, 20_000_000),
            randomLongBetween(20_000_000, 30_000_000),
            randomLongBetween(30_000_000, 40_000_000),
            randomLongBetween(40_000_000, 50_000_000),
            randomLongBetween(0, 9_999_999) };
        Level[] expectedLevel = { Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, null };
        for (int i = 0; i < actualTook.length; i++) {
            EsqlExecutionInfo warnQuery = getEsqlExecutionInfo(actualTook[i]);
            queryLog.onQueryPhase(
                new Versioned<>(
                    new Result(List.of(), List.of(), null, configuration, DriverCompletionInfo.EMPTY, warnQuery, null),
                    TransportVersion.current()
                ),
                query,
                viewSettings
            );
            if (expectedLevel[i] != null) {
                assertThat(appender.lastEvent(), is(not(nullValue())));
                var msg = (ESLogMessage) appender.lastMessage();
                long took = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK));
                long tookMillisExpected = took / 1_000_000L;
                long tookMillis = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS));
                assertThat(took, is(actualTook[i]));
                assertThat(tookMillis, is(tookMillisExpected));

                // Checks values for all planning timespans
                for (TimeSpanMarker timeSpan : warnQuery.queryProfile().timeSpanMarkers()) {
                    String tookValue = msg.get(ELASTICSEARCH_QUERYLOG_PREFIX + timeSpan.name() + ELASTICSEARCH_QUERYLOG_TOOK_SUFFIX);
                    assertNotNull(tookValue);
                    Long timeSpanTook = Long.valueOf(tookValue);
                    long timeSpanTookMillisExpected = timeSpanTook / 1_000_000;
                    String tookValueMillis = msg.get(
                        ELASTICSEARCH_QUERYLOG_PREFIX + timeSpan.name() + ELASTICSEARCH_QUERYLOG_TOOK_MILLIS_SUFFIX
                    );
                    assertNotNull(tookValueMillis);
                    long timeSpanTookMillis = Long.valueOf(tookValueMillis);
                    assertThat(timeSpanTookMillis, is(timeSpanTookMillisExpected));
                }
                assertThat(msg.get(ELASTICSEARCH_QUERYLOG_QUERY), is(query));
                assertResolvedSettingsLogged(msg);
                assertViewSettingsLogged(msg, viewSettings);
                assertThat(appender.getLastEventAndReset().getLevel(), equalTo(expectedLevel[i]));
            } else {
                assertThat(appender.lastEvent(), is(nullValue()));
            }
        }

    }

    public void testPrioritiesOnFailure() {
        EsqlQueryLog queryLog = new EsqlQueryLog(settings, mockLogFieldProvider());
        assertWarnings(EsqlQueryLog.DEPRECATION_MESSAGE);
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
        Map<String, ViewQuerySettings> viewSettings = randomViewSettings();
        for (int i = 0; i < actualTook.length; i++) {
            queryLog.onQueryFailure(query, RESOLVED_SETTINGS, viewSettings, new FetchPhaseStats(), ex, actualTook[i]);
            if (expectedLevel[i] != null) {
                assertThat(appender.lastEvent(), is(not(nullValue())));
                var msg = (ESLogMessage) appender.lastMessage();
                long took = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK));
                long tookMillisExpected = took / 1_000_000L;
                long tookMillis = Long.valueOf(msg.get(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS));
                assertThat(took, is(actualTook[i]));
                assertThat(tookMillis, is(tookMillisExpected));
                assertThat(msg.get(ELASTICSEARCH_QUERYLOG_QUERY), is(query));
                assertResolvedSettingsLogged(msg);
                assertViewSettingsLogged(msg, viewSettings);
                assertThat(appender.getLastEventAndReset().getLevel(), equalTo(expectedLevel[i]));
            } else {
                assertThat(appender.lastEvent(), is(nullValue()));
            }
        }
    }

    /**
     * A query that fetched logs what the planner did and what the fetches sent, so slow queries can be split by it.
     */
    public void testAQueryThatFetchedLogsItsFetches() {
        EsqlExecutionInfo executionInfo = getEsqlExecutionInfo(SLOW_QUERY_NANOS);
        executionInfo.fetchPhase().plannedBy(outcomes(Outcome.APPLIED));
        executionInfo.fetchPhase().batchEnded(7, List.of("n1", "n2"), TimeUnit.MILLISECONDS.toNanos(3));

        ESLogMessage msg = logSuccess(executionInfo);
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_OUTCOME), is("applied"));
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_DOCUMENTS), is("7"));
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_REQUESTS), is("2"));
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_NODES), is("2"));
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_TOOK_MILLIS), is("3"));
    }

    /**
     * A query that asked for the fetch phase and didn't get it logs why, and nothing about fetches.
     */
    public void testAQueryThatDidntFetchLogsOnlyTheOutcome() {
        EsqlExecutionInfo executionInfo = getEsqlExecutionInfo(SLOW_QUERY_NANOS);
        executionInfo.fetchPhase().plannedBy(outcomes(Outcome.INELIGIBLE_SHAPE));

        ESLogMessage msg = logSuccess(executionInfo);
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_OUTCOME), is("ineligible_shape"));
        assertThat(msg.getData(), not(hasKey(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_DOCUMENTS)));
    }

    /**
     * A query that left the fetch phase alone logs nothing about it, like before the fetch phase.
     */
    public void testAQueryThatLeftTheFetchPhaseAloneLogsNothingAboutIt() {
        EsqlExecutionInfo executionInfo = getEsqlExecutionInfo(SLOW_QUERY_NANOS);
        executionInfo.fetchPhase().plannedBy(outcomes(Outcome.DISABLED_SETTING));

        ESLogMessage msg = logSuccess(executionInfo);
        assertThat(msg.getData(), not(hasKey(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_OUTCOME)));
    }

    /**
     * A query that failed after its fetches logs them too, since a fetch can be what failed it.
     */
    public void testAFailedQueryLogsItsFetches() {
        EsqlQueryLog queryLog = new EsqlQueryLog(settings, mockLogFieldProvider());
        assertWarnings(EsqlQueryLog.DEPRECATION_MESSAGE);
        FetchPhaseStats fetchPhase = new FetchPhaseStats();
        fetchPhase.plannedBy(outcomes(Outcome.APPLIED));
        fetchPhase.batchEnded(4, List.of("n1"), TimeUnit.MILLISECONDS.toNanos(1));

        queryLog.onQueryFailure("from idx", RESOLVED_SETTINGS, Map.of(), fetchPhase, new ValidationException(), SLOW_QUERY_NANOS);
        ESLogMessage msg = (ESLogMessage) appender.getLastEventAndReset().getMessage();
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_OUTCOME), is("applied"));
        assertThat(msg.get(ELASTICSEARCH_QUERYLOG_FETCH_PHASE_DOCUMENTS), is("4"));
    }

    private static FetchPhaseOutcomes outcomes(Outcome outcome) {
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        outcomes.record(outcome, null);
        return outcomes;
    }

    private ESLogMessage logSuccess(EsqlExecutionInfo executionInfo) {
        EsqlQueryLog queryLog = new EsqlQueryLog(settings, mockLogFieldProvider());
        assertWarnings(EsqlQueryLog.DEPRECATION_MESSAGE);
        Result result = new Result(
            List.of(),
            List.of(),
            null,
            configurationWith(RESOLVED_SETTINGS),
            DriverCompletionInfo.EMPTY,
            executionInfo,
            null
        );
        queryLog.onQueryPhase(new Versioned<>(result, TransportVersion.current()), "from idx", Map.of());
        return (ESLogMessage) appender.getLastEventAndReset().getMessage();
    }

    private static EsqlExecutionInfo getEsqlExecutionInfo(long tookNanos) {
        EsqlExecutionInfo esqlExecutionInfo = new EsqlExecutionInfo(
            Predicates.always(),
            EsqlExecutionInfo.IncludeExecutionMetadata.CCS_ONLY
        ) {
            @Override
            public TimeValue overallTook() {
                return new TimeValue(tookNanos, TimeUnit.NANOSECONDS);
            }

            @Override
            public EsqlQueryProfile queryProfile() {
                return new EsqlQueryProfile(
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomTimeSpan(),
                    randomIntBetween(0, 100),
                    randomIntBetween(0, 100),
                    randomIntBetween(0, 1000),
                    randomNonNegativeLong(),
                    randomFrom(UnmappedResolution.values()),
                    randomIntBetween(0, 100),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                );
            }
        };

        return esqlExecutionInfo;
    }

    private static TimeSpan randomTimeSpan() {
        long startNanos = randomNonNegativeLong();
        long stopNanos = startNanos + randomLongBetween(1, 100_000);
        return new TimeSpan(startNanos / 1_000_000, startNanos, stopNanos / 1_000_000, stopNanos);
    }

}
