/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransformContextTests extends ESTestCase {

    private TransformContext.Listener listener;

    @Before
    public void setUpMocks() {
        listener = mock(TransformContext.Listener.class);
    }

    @After
    public void verifyNoMoreInteractionsOnMocks() {
        verifyNoMoreInteractions(listener);
    }

    public void testFailureCount() {
        TransformContext context = new TransformContext(null, null, 0, listener);
        assertThat(context.incrementAndGetFailureCount("some_exception"), is(equalTo(1)));
        assertThat(context.getFailureCount(), is(equalTo(1)));
        assertThat(context.incrementAndGetFailureCount("some_other_exception"), is(equalTo(2)));
        assertThat(context.getFailureCount(), is(equalTo(2)));
        context.resetReasonAndFailureCounter();
        assertThat(context.getFailureCount(), is(equalTo(0)));
        assertThat(context.getLastFailure(), is(nullValue()));

        // Verify that the listener is notified every time the failure count is incremented or reset
        verify(listener, times(3)).failureCountChanged();
    }

    public void testCheckpoint() {
        TransformContext context = new TransformContext(null, null, 13, listener);
        assertThat(context.getCheckpoint(), is(equalTo(13L)));
        assertThat(context.incrementAndGetCheckpoint(), is(equalTo(14L)));
        assertThat(context.getCheckpoint(), is(equalTo(14L)));
        context.setCheckpoint(25);
        assertThat(context.getCheckpoint(), is(equalTo(25L)));
        assertThat(context.incrementAndGetCheckpoint(), is(equalTo(26L)));
        assertThat(context.getCheckpoint(), is(equalTo(26L)));
    }

    public void testTaskState() {
        TransformContext context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.STARTED)));
        assertThat(context.setTaskState(TransformTaskState.STOPPED, TransformTaskState.STOPPED), is(false));
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.STARTED)));
        assertThat(context.setTaskState(TransformTaskState.STARTED, TransformTaskState.STOPPED), is(true));
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.STOPPED)));
        context.resetTaskState();
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.STARTED)));
        context.setTaskStateToFailed(null);
        assertThat(context.getTaskState(), is(equalTo(TransformTaskState.FAILED)));
    }

    public void testStateReason() {
        TransformContext context = new TransformContext(TransformTaskState.STARTED, null, 0, listener);
        assertThat(context.getStateReason(), is(nullValue()));
        context.setTaskStateToFailed("some-reason");
        assertThat(context.getStateReason(), is(equalTo("some-reason")));
        context.setTaskStateToFailed("some-other-reason");
        assertThat(context.getStateReason(), is(equalTo("some-other-reason")));
        context.resetTaskState();
        assertThat(context.getStateReason(), is(nullValue()));
        context.setTaskStateToFailed("yet-another-reason");
        assertThat(context.getStateReason(), is(equalTo("yet-another-reason")));
        context.resetReasonAndFailureCounter();
        assertThat(context.getStateReason(), is(nullValue()));

        verify(listener).failureCountChanged();
    }
}
