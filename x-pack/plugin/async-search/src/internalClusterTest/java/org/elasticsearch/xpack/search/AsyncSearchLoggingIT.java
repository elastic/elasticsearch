/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.search.TransportSearchAction.SEARCH_ACTIONLOG_NAME;
import static org.elasticsearch.common.logging.action.ActionLogger.SEARCH_ACTION_LOGGER_ENABLED;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class AsyncSearchLoggingIT extends AsyncSearchIntegTestCase {
    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(SEARCH_ACTIONLOG_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new AccumulatingMockAppender("trace_appender");
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
        updateClusterSettings(Settings.builder().put(SEARCH_ACTION_LOGGER_ENABLED.getConcreteSettingForNamespace("search").getKey(), true));
        appender.reset();
    }

    @After
    public void restoreLog() {
        updateClusterSettings(
            Settings.builder().put(SEARCH_ACTION_LOGGER_ENABLED.getConcreteSettingForNamespace("search").getKey(), (String) null)
        );
    }

    private static final String INDEX_NAME = "test_index";

    private List<LogEvent> getNonSystemEvents() {
        return appender.events.stream().filter(event -> {
            assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
            ESLogMessage message = (ESLogMessage) event.getMessage();
            return message.get("type").equals("search") == false || Objects.equals(message.get("indices"), ".async-search") == false;
        }).toList();
    }

    // Test _search
    public void testSearchLog() throws Exception {
        setupIndex();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(INDEX_NAME);
        request.getSearchRequest().source(new SearchSourceBuilder().query(matchQuery("field1", "quick")));
        request.setKeepOnCompletion(true);
        request.setWaitForCompletionTimeout(TimeValue.THIRTY_SECONDS);
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertFalse(response.isRunning());
        } finally {
            response.decRef();
        }
        // async search cleanup also does searches. Remove potential events caused by it
        var events = getNonSystemEvents();
        assertThat(events, hasSize(1));
        ESLogMessage message = (ESLogMessage) events.getFirst().getMessage();
        var data = message.getIndexedReadOnlyStringMap();
        assertThat(message.get("success"), equalTo("true"));
        assertThat(message.get("type"), equalTo("search"));
        assertThat(message.get("hits"), equalTo("3"));
        assertThat(data.getValue("took"), greaterThan(0L));
        assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
        assertThat(message.get("query"), containsString("quick"));
        assertThat(message.get("indices"), equalTo(INDEX_NAME));
    }

    public void testFailureLog() throws Exception {
        setupIndex();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(INDEX_NAME);
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), -1);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));
        request.setKeepOnCompletion(true);
        request.setWaitForCompletionTimeout(TimeValue.THIRTY_SECONDS);
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertFalse(response.isRunning());
            assertThat(response.status().getStatus(), equalTo(500));
        } finally {
            response.decRef();
        }
        var events = getNonSystemEvents();
        assertThat(events, hasSize(1));
        ESLogMessage message = (ESLogMessage) events.getFirst().getMessage();
        var data = message.getIndexedReadOnlyStringMap();
        assertThat(message.get("success"), equalTo("false"));
        assertThat(message.get("type"), equalTo("search"));
        assertThat(message.get("hits"), equalTo("0"));
        assertThat(data.getValue("took"), greaterThan(0L));
        assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
        assertThat(message.get("query"), containsString("throw"));
        assertThat(message.get("indices"), equalTo(INDEX_NAME));
        assertThat(message.get("error.type"), equalTo("org.elasticsearch.action.search.SearchPhaseExecutionException"));
        assertThat(message.get("error.message"), equalTo("all shards failed"));
    }

    private void setupIndex() {
        createIndex(INDEX_NAME);
        indexRandom(
            true,
            prepareIndex(INDEX_NAME).setId("1").setSource("field1", "the quick brown fox jumps"),
            prepareIndex(INDEX_NAME).setId("2").setSource("field1", "quick brown"),
            prepareIndex(INDEX_NAME).setId("3").setSource("field1", "quick")
        );
    }

}
