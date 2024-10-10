/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryableUpdateModelSnapshotActionTests extends ESTestCase {

    private static final String JOB_ID = "valid_id";

    private Client client;

    private ThreadPool threadPool;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenReturn(DIRECT_EXECUTOR_SERVICE);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), any());
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testFirstTimeSuccess() {

        PutJobAction.Response response = mock(PutJobAction.Response.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<PutJobAction.Response> listener = (ActionListener<PutJobAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        AtomicReference<PutJobAction.Response> storedResponse = new AtomicReference<>();

        UpdateJobAction.Request updateRequest = new UpdateJobAction.Request(JOB_ID, new JobUpdate.Builder(JOB_ID).build());
        RetryableUpdateModelSnapshotAction updateModelSnapshotAction = new RetryableUpdateModelSnapshotAction(
            client,
            updateRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(PutJobAction.Response response) {
                    storedResponse.set(response);
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }
        );
        updateModelSnapshotAction.run();

        verify(threadPool, never()).schedule(any(), any(), any());
        assertSame(response, storedResponse.get());
    }

    public void testRetriesNeeded() {

        int numRetries = randomIntBetween(1, 5);

        PutJobAction.Response response = mock(PutJobAction.Response.class);
        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<PutJobAction.Response> listener = (ActionListener<PutJobAction.Response>) invocationOnMock.getArguments()[2];
            if (callCount.incrementAndGet() > numRetries) {
                listener.onResponse(response);
            } else {
                listener.onFailure(new Exception());
            }
            return null;
        }).when(client).execute(any(), any(), any());

        AtomicReference<PutJobAction.Response> storedResponse = new AtomicReference<>();

        UpdateJobAction.Request updateRequest = new UpdateJobAction.Request(JOB_ID, new JobUpdate.Builder(JOB_ID).build());
        RetryableUpdateModelSnapshotAction updateModelSnapshotAction = new RetryableUpdateModelSnapshotAction(
            client,
            updateRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(PutJobAction.Response response) {
                    storedResponse.set(response);
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }
        );
        updateModelSnapshotAction.run();

        verify(threadPool, times(numRetries)).schedule(any(), any(), any());
        assertSame(response, storedResponse.get());
    }

    public void testCompleteFailure() {

        int numRetries = randomIntBetween(1, 5);

        AtomicInteger callCount = new AtomicInteger(0);
        AtomicLong relativeTimeMs = new AtomicLong(0);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<PutJobAction.Response> listener = (ActionListener<PutJobAction.Response>) invocationOnMock.getArguments()[2];
            if (callCount.incrementAndGet() > numRetries) {
                relativeTimeMs.set(TimeValue.timeValueMinutes(5).millis() + 1);
            }
            listener.onFailure(new Exception(Long.toString(relativeTimeMs.get())));
            return null;
        }).when(client).execute(any(), any(), any());
        doAnswer(invocationOnMock -> relativeTimeMs.get()).when(threadPool).relativeTimeInMillis();

        AtomicReference<Exception> storedFailure = new AtomicReference<>();

        UpdateJobAction.Request updateRequest = new UpdateJobAction.Request(JOB_ID, new JobUpdate.Builder(JOB_ID).build());
        RetryableUpdateModelSnapshotAction updateModelSnapshotAction = new RetryableUpdateModelSnapshotAction(
            client,
            updateRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(PutJobAction.Response response) {
                    fail("this should not be called");
                }

                @Override
                public void onFailure(Exception e) {
                    storedFailure.set(e);
                }
            }
        );
        updateModelSnapshotAction.run();

        verify(threadPool, times(numRetries)).schedule(any(), any(), any());
        assertEquals(Long.toString(relativeTimeMs.get()), storedFailure.get().getMessage());
    }
}
