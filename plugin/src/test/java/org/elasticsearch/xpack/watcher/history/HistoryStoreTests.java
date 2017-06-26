/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import org.apache.http.HttpStatus;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpRequest;
import org.elasticsearch.xpack.common.http.HttpResponse;
import org.elasticsearch.xpack.notification.jira.JiraAccount;
import org.elasticsearch.xpack.notification.jira.JiraIssue;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.actions.jira.JiraAction;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionResult;
import org.elasticsearch.xpack.watcher.execution.Wid;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.DateTime;
import org.junit.Before;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.watcher.history.HistoryStore.getHistoryIndexNameForTime;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HistoryStoreTests extends ESTestCase {

    private HistoryStore historyStore;
    private Client client;

    @Before
    public void init() {
        client = mock(Client.class);
        historyStore = new HistoryStore(Settings.EMPTY, client);
        historyStore.start();
    }

    public void testPut() throws Exception {
        DateTime now = new DateTime(0, UTC);
        Wid wid = new Wid("_name", now);
        String index = getHistoryIndexNameForTime(now);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(wid.watchId(), now, now);
        WatchRecord watchRecord = new WatchRecord.MessageWatchRecord(wid, event, ExecutionState.EXECUTED, null, randomAlphaOfLength(10));

        IndexResponse indexResponse = mock(IndexResponse.class);

        doAnswer(invocation -> {
            IndexRequest request = (IndexRequest) invocation.getArguments()[0];
            PlainActionFuture<IndexResponse> indexFuture = PlainActionFuture.newFuture();
            if (request.id().equals(wid.value()) && request.type().equals(HistoryStore.DOC_TYPE) && request.opType() == OpType.CREATE
                    && request.index().equals(index)) {
                indexFuture.onResponse(indexResponse);
            } else {
                indexFuture.onFailure(new ElasticsearchException("test issue"));
            }
            return indexFuture;
        }).when(client).index(any());

        historyStore.put(watchRecord);
        verify(client).index(any());
    }

    public void testPutStopped() throws Exception {
        Wid wid = new Wid("_name", new DateTime(0, UTC));
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(wid.watchId(), new DateTime(0, UTC), new DateTime(0, UTC));
        WatchRecord watchRecord = new WatchRecord.MessageWatchRecord(wid, event, ExecutionState.EXECUTED, null, randomAlphaOfLength(10));

        historyStore.stop();
        try {
            historyStore.put(watchRecord);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("unable to persist watch record history store is not ready"));
        } finally {
            historyStore.start();
        }
    }

    public void testIndexNameGeneration() {
        String indexTemplateVersion = WatcherIndexTemplateRegistry.INDEX_TEMPLATE_VERSION;
        assertThat(getHistoryIndexNameForTime(new DateTime(0, UTC)),
                equalTo(".watcher-history-"+ indexTemplateVersion +"-1970.01.01"));
        assertThat(getHistoryIndexNameForTime(new DateTime(100000000000L, UTC)),
                equalTo(".watcher-history-" + indexTemplateVersion + "-1973.03.03"));
        assertThat(getHistoryIndexNameForTime(new DateTime(1416582852000L, UTC)),
                equalTo(".watcher-history-" + indexTemplateVersion + "-2014.11.21"));
        assertThat(getHistoryIndexNameForTime(new DateTime(2833165811000L, UTC)),
                equalTo(".watcher-history-" + indexTemplateVersion + "-2059.10.12"));
    }

    public void testStoreWithHideSecrets() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR));

        final String username = randomFrom("admin", "elastic", "test");
        final String password = randomFrom("secret", "password", "123456");
        final String url = "https://" + randomFrom("localhost", "internal-jira.elastic.co") + ":" + randomFrom(80, 8080, 449, 9443);

        Settings settings = Settings.builder().put("url", url).put("user", username).put("password", password).build();
        JiraAccount account = new JiraAccount("_account", settings, httpClient);

        JiraIssue jiraIssue = account.createIssue(singletonMap("foo", "bar"), null);
        ActionWrapper.Result result = new ActionWrapper.Result(JiraAction.TYPE, new JiraAction.Executed(jiraIssue));

        DateTime now = new DateTime(0, UTC);
        Wid wid = new Wid("_name", now);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        when(watch.status()).thenReturn(new WatchStatus(now, singletonMap("_action", new ActionStatus(now))));

        WatchExecutionContext context = mock(WatchExecutionContext.class);
        when(context.id()).thenReturn(wid);
        when(context.triggerEvent()).thenReturn(new ScheduleTriggerEvent(wid.watchId(), now, now));
        when(context.vars()).thenReturn(emptyMap());
        when(context.watch()).thenReturn(watch);

        WatchExecutionResult watchExecutionResult = new WatchExecutionResult(context, 0);

        WatchRecord watchRecord;
        if (randomBoolean()) {
            watchRecord = new WatchRecord.MessageWatchRecord(context, watchExecutionResult);
        } else {
            watchRecord = new WatchRecord.ExceptionWatchRecord(context, watchExecutionResult, new IllegalStateException());
        }
        watchRecord.result().actionsResults().put(JiraAction.TYPE, result);

        PlainActionFuture<IndexResponse> indexResponseFuture = PlainActionFuture.newFuture();
        indexResponseFuture.onResponse(mock(IndexResponse.class));
        when(client.index(any())).thenReturn(indexResponseFuture);
        if (randomBoolean()) {
            historyStore.put(watchRecord);
        } else {
            historyStore.forcePut(watchRecord);
        }
        verify(client).index(argThat(indexRequestDoesNotContainPassword(username, password)));
    }

    private static Matcher<IndexRequest> indexRequestDoesNotContainPassword(String username, String password) {
        return new IndexRequestNoPasswordMatcher(username, password);
    }

    private static class IndexRequestNoPasswordMatcher extends TypeSafeMatcher<IndexRequest> {

        private String username;
        private String password;

        IndexRequestNoPasswordMatcher(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        protected boolean matchesSafely(IndexRequest indexRequest) {
            String source  = indexRequest.source().utf8ToString();
            assertThat(source, containsString(username));
            assertThat(source, not(containsString(password)));
            return true;
        }

        @Override
        public void describeMismatchSafely(final IndexRequest indexRequest, final Description mismatchDescription) {
            mismatchDescription.appendText(" was ").appendValue(indexRequest.sourceAsMap());
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("IndexRequest id should contain username [")
                    .appendValue(username)
                    .appendText("] and should not contain [")
                    .appendValue(password)
                    .appendText("]");
        }
    }
}
