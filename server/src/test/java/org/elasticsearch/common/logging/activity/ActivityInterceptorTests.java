/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class ActivityInterceptorTests extends ESTestCase {

    private List<String> responses = new ArrayList<>();
    private List<Exception> failures = new ArrayList<>();
    private ActionListener<String> listener = ActionListener.wrap(responses::add, failures::add);

    @Before
    public void resetListener() {
        responses = new ArrayList<>();
        failures = new ArrayList<>();
        listener = ActionListener.wrap(responses::add, failures::add);
    }

    public void testWrapIsNoopWhenNoSinkIsEnabled() {
        var sink = new RecordingSink(false);

        assertThat(interceptor(List.of(sink.sink)).wrap(listener, new TestContextBuilder()), sameInstance(listener));
    }

    public void testWrapIsNoopWithoutSinks() {
        assertThat(interceptor(List.of()).wrap(listener, new TestContextBuilder()), sameInstance(listener));
    }

    public void testSinkSeesResponseAndDelegateStillCalled() {
        var sink = new RecordingSink(true);

        interceptor(List.of(sink.sink)).wrap(listener, new TestContextBuilder()).onResponse("done");

        assertThat(sink.accepted.size(), equalTo(1));
        assertTrue(sink.accepted.get(0).isSuccess());
        assertThat(responses, contains("done"));
    }

    public void testSinkSeesFailureAndDelegateStillCalled() {
        var sink = new RecordingSink(true);
        var failure = new IllegalStateException("boom");

        interceptor(List.of(sink.sink)).wrap(listener, new TestContextBuilder()).onFailure(failure);

        assertThat(sink.accepted.size(), equalTo(1));
        assertFalse(sink.accepted.get(0).isSuccess());
        assertThat(sink.accepted.get(0).getErrorMessage(), equalTo("boom"));
        assertThat(failures, contains(sameInstance(failure)));
    }

    public void testOnlyEnabledSinksAreCalled() {
        var disabled = new RecordingSink(false);
        var enabled = new RecordingSink(true);

        interceptor(List.of(disabled.sink, enabled.sink)).wrap(listener, new TestContextBuilder()).onResponse("done");

        assertThat(disabled.accepted, empty());
        assertThat(enabled.accepted.size(), equalTo(1));
    }

    public void testSinkCheckedForEnabledOnEveryAction() {
        var sink = new RecordingSink(false);
        var actionLogger = interceptor(List.of(sink.sink));
        var builder = new TestContextBuilder();

        actionLogger.wrap(listener, builder).onResponse("one");
        sink.enabled.set(true);
        actionLogger.wrap(listener, builder).onResponse("two");

        assertThat(sink.accepted.size(), equalTo(1));
        assertThat(responses, contains("one", "two"));
    }

    public void testFailingSinkDoesNotFailTheActionOrStarveOthers() {
        var failing = new ActivitySink<TestContext, TestContext>(() -> true, Optional::of, context -> {
            throw new IllegalStateException("sink failure");
        }, () -> "failing-sink");
        var after = new RecordingSink(true);

        interceptor(List.of(failing, after.sink)).wrap(listener, new TestContextBuilder()).onResponse("done");

        assertThat(after.accepted.size(), equalTo(1));
        assertThat(responses, contains("done"));
        assertThat(failures, empty());
    }

    public void testWrapAndRunReportsExceptionToSinkAndRethrows() {
        var sink = new RecordingSink(true);
        var failure = new IllegalStateException("thrown by the action");

        var thrown = expectThrows(
            IllegalStateException.class,
            () -> interceptor(List.of(sink.sink)).wrapAndRun(listener, new TestContextBuilder(), l -> {
                throw failure;
            })
        );

        assertThat(thrown, sameInstance(failure));
        assertThat(sink.accepted.size(), equalTo(1));
        assertFalse(sink.accepted.get(0).isSuccess());
    }

    private static ActivityInterceptor<TestContext> interceptor(List<ActivitySink<TestContext, ?>> sinks) {
        return new ActivityInterceptor<>(sinks);
    }

    /** A sink that records the contexts it is given. */
    private static class RecordingSink {
        final AtomicBoolean enabled;
        final List<TestContext> accepted = new ArrayList<>();
        final ActivitySink<TestContext, TestContext> sink;

        RecordingSink(boolean enabled) {
            this.enabled = new AtomicBoolean(enabled);
            this.sink = new ActivitySink<>(this.enabled::get, Optional::of, accepted::add, () -> "recording-sink");
        }
    }

    private static class TestContextBuilder extends ActivityCaptureContextBuilder<TestContext, String, String> {
        TestContextBuilder() {
            super(mock(Task.class), "request");
        }

        @Override
        public TestContext build(String response) {
            return new TestContext(100);
        }

        @Override
        public TestContext build(Exception e) {
            return new TestContext(e);
        }
    }

    private static class TestContext extends ActivityCaptureContext {
        TestContext(long tookInNanos) {
            super(mock(Task.class), "test", tookInNanos);
        }

        TestContext(Exception error) {
            super(mock(Task.class), "test", 0, error);
        }
    }
}
