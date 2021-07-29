/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
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
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
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
        verify(client).threadPool();
        verifyNoMoreInteractions(client);
    }

    public void testDeleteAllAnnotations() {
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteAllAnnotations(ActionListener.wrap(
            deleteResponse -> {},
            e -> fail(e.toString())
        ));

        verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());

        DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
        assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
        String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
        assertThat(dbqQueryString, not(containsString("timestamp")));
        assertThat(dbqQueryString, not(containsString("event")));
    }

    public void testDeleteAnnotations_TimestampFiltering() {
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        Tuple<Long, Long> range =
            randomFrom(
                tuple(1_000_000_000L, 2_000_000_000L),
                tuple(1_000_000_000L, null),
                tuple(null, 2_000_000_000L));
        jobDataDeleter.deleteAnnotations(range.v1(), range.v2(), null, ActionListener.wrap(
            deleteResponse -> {},
            e -> fail(e.toString())
        ));

        verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());

        DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
        assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
        String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
        assertThat(dbqQueryString, containsString("timestamp"));
        assertThat(dbqQueryString, not(containsString("event")));
    }

    public void testDeleteAnnotations_EventFiltering() {
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteAnnotations(null, null, Collections.singleton("dummy_event"), ActionListener.wrap(
            deleteResponse -> {},
            e -> fail(e.toString())
        ));

        verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());

        DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
        assertThat(deleteRequest.indices(), is(arrayContaining(AnnotationIndex.READ_ALIAS_NAME)));
        String dbqQueryString = Strings.toString(deleteRequest.getSearchRequest().source().query());
        assertThat(dbqQueryString, not(containsString("timestamp")));
        assertThat(dbqQueryString, containsString("event"));
    }

    public void testDeleteDatafeedTimingStats() {
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteDatafeedTimingStats(ActionListener.wrap(
            deleteResponse -> {},
            e -> fail(e.toString())
        ));

        verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());

        DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
        assertThat(deleteRequest.indices(), is(arrayContaining(AnomalyDetectorsIndex.jobResultsAliasedName(JOB_ID))));
    }
}
