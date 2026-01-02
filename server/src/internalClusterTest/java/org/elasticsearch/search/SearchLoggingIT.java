/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.MockAppender;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.elasticsearch.action.search.TransportSearchAction.SEARCH_ACTIONLOG_NAME;
import static org.elasticsearch.common.logging.action.ActionLogger.SEARCH_ACTION_LOGGER_ENABLED;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class SearchLoggingIT extends ESIntegTestCase {
    static MockAppender appender;
    static Logger queryLog = LogManager.getLogger(SEARCH_ACTIONLOG_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("trace_appender");
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
    }

    @After
    public void restoreLog() {
        updateClusterSettings(
            Settings.builder().put(SEARCH_ACTION_LOGGER_ENABLED.getConcreteSettingForNamespace("search").getKey(), (String) null)
        );
    }

    private static final String INDEX_NAME = "test_index";

    // Test _search
    public void testSearchLog() throws Exception {
        createIndex(INDEX_NAME);
        indexRandom(
            true,
            prepareIndex(INDEX_NAME).setId("1").setSource("field1", "the quick brown fox jumps"),
            prepareIndex(INDEX_NAME).setId("2").setSource("field1", "quick brown"),
            prepareIndex(INDEX_NAME).setId("3").setSource("field1", "quick")
        );

        // Simple request
        {
            assertSearchHitsWithoutFailures(prepareSearch().setQuery(simpleQueryStringQuery("fox")), "1");
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
            ESLogMessage message = (ESLogMessage) event.getMessage();
            var data = message.getIndexedReadOnlyStringMap();
            assertThat(message.get("success"), equalTo("true"));
            assertThat(message.get("type"), equalTo("search"));
            assertThat(message.get("hits"), equalTo("1"));
            assertThat(data.getValue("took"), greaterThan(0L));
            assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
            assertThat(message.get("query"), containsString("fox"));
            assertThat(message.get("indices"), equalTo(""));
        }

        // Match
        {
            assertSearchHitsWithoutFailures(prepareSearch(INDEX_NAME).setQuery(matchQuery("field1", "quick")), "1", "2", "3");
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
            ESLogMessage message = (ESLogMessage) event.getMessage();
            var data = message.getIndexedReadOnlyStringMap();
            assertThat(message.get("success"), equalTo("true"));
            assertThat(message.get("type"), equalTo("search"));
            assertThat(message.get("hits"), equalTo("3"));
            assertThat(data.getValue("took"), greaterThan(0L));
            assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
            assertThat(message.get("query"), containsString("quick"));
            assertThat(message.get("indices"), equalTo(INDEX_NAME));
        }

    }

    public void testFailureLog() {
        assertAcked(prepareCreate(INDEX_NAME).setMapping("field1", "type=text,index_options=docs"));
        indexRandom(
            true,
            prepareIndex(INDEX_NAME).setId("1").setSource("field1", "quick brown fox", "field2", "quick brown fox"),
            prepareIndex(INDEX_NAME).setId("2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox")
        );

        assertFailures(
            prepareSearch(INDEX_NAME).setQuery(matchPhraseQuery("field1", "quick brown").slop(0)),
            RestStatus.BAD_REQUEST,
            containsString("field:[field1] was indexed without position data; cannot run PhraseQuery")
        );
        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
        ESLogMessage message = (ESLogMessage) event.getMessage();
        var data = message.getIndexedReadOnlyStringMap();
        assertThat(message.get("success"), equalTo("false"));
        assertThat(message.get("type"), equalTo("search"));
        assertThat(message.get("hits"), equalTo("0"));
        assertThat(data.getValue("took"), greaterThan(0L));
        assertThat(data.getValue("took_millis"), greaterThanOrEqualTo(0L));
        assertThat(message.get("query"), containsString("quick brown"));
        assertThat(message.get("indices"), equalTo(INDEX_NAME));
        assertThat(message.get("error.type"), equalTo("org.elasticsearch.action.search.SearchPhaseExecutionException"));
        assertThat(message.get("error.message"), equalTo("all shards failed"));
    }
    // Test async search
    // Test multisearch
}
