/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchLogContext;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.activity.ActivityLogProducer;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Map;

import static org.elasticsearch.common.logging.activity.ActivityLogger.ACTIVITY_LOGGER_ENABLED;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_INDICES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_IS_REMOTE;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_REMOTES;
import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_REMOTE_COUNT;
import static org.elasticsearch.test.ActivityLoggingUtils.assertMessageSuccess;
import static org.elasticsearch.test.ActivityLoggingUtils.getMessageData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests that verify cross-cluster search (CCS) request logging.
 */
public class CrossClusterSearchLoggingIT extends AbstractCrossClusterSearchTestCase {

    static AccumulatingMockAppender appender;
    static Logger queryLog = LogManager.getLogger(QueryLogging.QUERY_LOGGER_NAME);
    static Level origQueryLogLevel = queryLog.getLevel();

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    @BeforeClass
    public static void initAppender() throws IllegalAccessException {
        appender = new AccumulatingMockAppender("ccs_logging_appender");
        appender.start();
        Loggers.addAppender(queryLog, appender);
        Loggers.setLevel(queryLog, Level.TRACE);
    }

    @AfterClass
    public static void cleanupAppender() {
        Loggers.removeAppender(queryLog, appender);
        appender.stop();
        Loggers.setLevel(queryLog, origQueryLogLevel);
    }

    @Before
    public void enableActivityLogger() {
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(ACTIVITY_LOGGER_ENABLED.getKey(), true))
                .get()
        );
        appender.reset();
    }

    @After
    public void disableActivityLogger() {
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(ACTIVITY_LOGGER_ENABLED.getKey(), (String) null))
                .get()
        );
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(ACTIVITY_LOGGER_ENABLED.getKey(), (String) null))
                .get()
        );
    }

    /**
     * CCS over both local and remote indices: activity log must contain remote_count=1.
     */
    public void testCCSLoggingWithRemote() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        // Remote + local
        {
            SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
            searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

            assertResponse(client(LOCAL_CLUSTER).search(searchRequest), Assert::assertNotNull);

            assertThat(appender.events.size(), equalTo(1));
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            Map<String, String> message = getMessageData(event);
            assertMessageSuccess(message, SearchLogContext.TYPE, "match_all");
            assertThat(message.get(QUERY_FIELD_REMOTE_COUNT), equalTo("1"));
            assertThat(message.get(QUERY_FIELD_REMOTES), containsString(REMOTE_CLUSTER));
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo(localIndex + "," + REMOTE_CLUSTER + ":" + remoteIndex));
            assertNull(message.get(QUERY_FIELD_IS_REMOTE));
        }
        // Remote only
        {
            SearchRequest searchRequest = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
            searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

            assertResponse(client(LOCAL_CLUSTER).search(searchRequest), Assert::assertNotNull);

            assertThat(appender.events.size(), equalTo(1));
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            Map<String, String> message = getMessageData(event);
            assertMessageSuccess(message, SearchLogContext.TYPE, "match_all");
            assertThat(message.get(QUERY_FIELD_REMOTE_COUNT), equalTo("1"));
            assertThat(message.get(QUERY_FIELD_REMOTES), containsString(REMOTE_CLUSTER));
            assertNull(message.get(QUERY_FIELD_IS_REMOTE));
        }
        // Wildcard
        {
            SearchRequest searchRequest = new SearchRequest("*:" + remoteIndex);
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
            searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

            assertResponse(client(LOCAL_CLUSTER).search(searchRequest), Assert::assertNotNull);

            assertThat(appender.events.size(), equalTo(1));
            var event = appender.getLastEventAndReset();
            assertNotNull(event);
            Map<String, String> message = getMessageData(event);
            assertMessageSuccess(message, SearchLogContext.TYPE, "match_all");
            assertThat(message.get(QUERY_FIELD_REMOTE_COUNT), equalTo("1"));
            assertThat(message.get(QUERY_FIELD_REMOTES), containsString(REMOTE_CLUSTER));
            assertThat(message.get(QUERY_FIELD_INDICES), equalTo("*:" + remoteIndex));
            assertNull(message.get(QUERY_FIELD_IS_REMOTE));
        }
    }

    /**
     * Local-only search must not include remote_count in the activity log.
     */
    public void testLocalOnlySearchDoesNotLogCcsFields() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");

        SearchRequest searchRequest = new SearchRequest(localIndex);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), Assert::assertNotNull);

        var event = appender.getLastEventAndReset();
        assertNotNull(event);
        Map<String, String> message = getMessageData(event);
        assertMessageSuccess(message, SearchLogContext.TYPE, "match_all");
        assertNull(message.get(QUERY_FIELD_REMOTE_COUNT));
        assertNull(message.get(QUERY_FIELD_REMOTES));
    }

    public void testCCSLoggingOnRemote() throws Exception {
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(ACTIVITY_LOGGER_ENABLED.getKey(), true))
                .get()
        );
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        // Remote + local
        {
            SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
            // remote only logs on MRT=true
            searchRequest.setCcsMinimizeRoundtrips(true);
            searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

            assertResponse(client(LOCAL_CLUSTER).search(searchRequest), Assert::assertNotNull);

            assertThat(appender.events.size(), equalTo(2)); // logged on both sides!
            for (var event : appender.events) {
                Map<String, String> message = getMessageData(event);
                assertMessageSuccess(message, SearchLogContext.TYPE, "match_all");
                if (message.get(ActivityLogProducer.PARENT_TASK_ID_FIELD) == null) {
                    // Local side
                    assertThat(message.get(QUERY_FIELD_REMOTE_COUNT), equalTo("1"));
                    assertThat(message.get(QUERY_FIELD_REMOTES), equalTo("[" + REMOTE_CLUSTER + "]"));
                    assertThat(message.get(QUERY_FIELD_INDICES), equalTo(localIndex + "," + REMOTE_CLUSTER + ":" + remoteIndex));
                    assertNull(message.get(QUERY_FIELD_IS_REMOTE));
                } else {
                    // Remote side
                    assertNull(message.get(QUERY_FIELD_REMOTE_COUNT));
                    assertNull(message.get(QUERY_FIELD_REMOTES));
                    assertThat(message.get(QUERY_FIELD_IS_REMOTE), equalTo("true"));
                    assertThat(message.get(QUERY_FIELD_INDICES), equalTo(remoteIndex));
                }
            }
        }
    }

}
