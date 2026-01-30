/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.MapMessage;
import org.elasticsearch.action.search.SearchLogProducer;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.test.ActionLoggingUtils;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class AsyncSearchLoggingIT extends AsyncSearchIntegTestCase {
    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(SearchLogProducer.LOGGER_NAME);
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
        ActionLoggingUtils.enableLoggers();
        appender.reset();
    }

    @After
    public void restoreLog() {
        ActionLoggingUtils.disableLoggers();
    }

    private static final String INDEX_NAME = "test_index";

    @SuppressWarnings("unchecked")
    private Map<String, String> getMessageData(LogEvent event) {
        assertNotNull(event);
        assertThat(event.getMessage(), instanceOf(MapMessage.class));

        return ((MapMessage<?, String>) event.getMessage()).getData();
    }

    private List<LogEvent> getNonSystemEvents() {
        return appender.events.stream().filter(event -> {
            Map<String, String> message = getMessageData(event);
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
        Map<String, String> message = getMessageData(events.getFirst());
        assertThat(message.get("success"), equalTo("true"));
        assertThat(message.get("type"), equalTo("search"));
        assertThat(message.get("hits"), equalTo("3"));
        assertThat(Long.valueOf(message.get("took")), greaterThan(0L));
        assertThat(Long.valueOf(message.get("took_millis")), greaterThanOrEqualTo(0L));
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
        Map<String, String> message = getMessageData(events.getFirst());
        assertThat(message.get("success"), equalTo("false"));
        assertThat(message.get("type"), equalTo("search"));
        assertThat(message.get("hits"), equalTo("0"));
        assertThat(Long.valueOf(message.get("took")), greaterThan(0L));
        assertThat(Long.valueOf(message.get("took_millis")), greaterThanOrEqualTo(0L));
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
