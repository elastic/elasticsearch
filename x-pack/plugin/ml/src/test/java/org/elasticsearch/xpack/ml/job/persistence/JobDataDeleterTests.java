/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.retention.MockWritableIndexExpander;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Set;

import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JobDataDeleterTests extends ESTestCase {

    private static final String JOB_ID = "my-job-id";

    private Client client;
    private ArgumentCaptor<DeleteByQueryRequest> deleteRequestCaptor;

    @Before
    public void setUpTests() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        deleteRequestCaptor = ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    }

    @After
    public void verifyNoMoreInteractionsWithClient() {
        verify(client, times(2)).threadPool();
        verifyNoMoreInteractions(client);
    }

    public void testDeleteAllAnnotations() {
        MockWritableIndexExpander.create(true);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            jobDataDeleter.deleteAllAnnotations(ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));

            if (deleteUserAnnotations) {
                verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            } else {
                verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            }

            DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
            assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
            String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
            assertThat(dbqQueryString, not(containsString("timestamp")));
            assertThat(dbqQueryString, not(containsString("event")));
            if (deleteUserAnnotations) {
                assertThat(dbqQueryString, not(containsString("_xpack")));
            } else {
                assertThat(dbqQueryString, containsString("_xpack"));
            }
        });
    }

    public void testDeleteAnnotations_TimestampFiltering() {
        MockWritableIndexExpander.create(true);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            Tuple<Long, Long> range = randomFrom(
                tuple(1_000_000_000L, 2_000_000_000L),
                tuple(1_000_000_000L, null),
                tuple(null, 2_000_000_000L)
            );
            jobDataDeleter.deleteAnnotations(range.v1(), range.v2(), null, ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));

            if (deleteUserAnnotations) {
                verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            } else {
                verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            }

            DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
            assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
            String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
            assertThat(dbqQueryString, containsString("timestamp"));
            assertThat(dbqQueryString, not(containsString("event")));
            if (deleteUserAnnotations) {
                assertThat(dbqQueryString, not(containsString("_xpack")));
            } else {
                assertThat(dbqQueryString, containsString("_xpack"));
            }
        });
    }

    public void testDeleteAnnotations_EventFiltering() {
        MockWritableIndexExpander.create(true);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            jobDataDeleter.deleteAnnotations(
                null,
                null,
                Set.of("dummy_event"),
                ActionTestUtils.assertNoFailureListener(deleteResponse -> {})
            );

            if (deleteUserAnnotations) {
                verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            } else {
                verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            }

            DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
            assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
            String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
            assertThat(dbqQueryString, not(containsString("timestamp")));
            assertThat(dbqQueryString, containsString("event"));
            if (deleteUserAnnotations) {
                assertThat(dbqQueryString, not(containsString("_xpack")));
            } else {
                assertThat(dbqQueryString, containsString("_xpack"));
            }
        });
    }

    public void testDeleteDatafeedTimingStats() {
        MockWritableIndexExpander.create(true);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            jobDataDeleter.deleteDatafeedTimingStats(ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));

            if (deleteUserAnnotations) {
                verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            } else {
                verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
            }

            DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
            assertThat(deleteRequest.indices(), is(arrayContaining(AnomalyDetectorsIndex.jobResultsAliasedName(JOB_ID))));
        });
    }

    public void testDeleteDatafeedTimingStats_WhenIndexReadOnly_ShouldNotDeleteAnything() {
        MockWritableIndexExpander.create(false);
        Arrays.asList(false, true).forEach(deleteUserAnnotations -> {
            JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID, deleteUserAnnotations);
            jobDataDeleter.deleteDatafeedTimingStats(ActionTestUtils.assertNoFailureListener(deleteResponse -> {}));

            if (deleteUserAnnotations) {
                verify(client, never()).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
                client.threadPool();
            } else {
                verify(client, never()).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
                client.threadPool();
            }
        });
    }
}
