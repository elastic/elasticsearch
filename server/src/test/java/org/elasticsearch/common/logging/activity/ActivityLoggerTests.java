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
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class ActivityLoggerTests extends ESTestCase {

    private final String loggerName = "test_logger";
    private ActivityLogProducer<TestContext> producer;
    private ActivityLogWriter writer;
    private ActionLoggingFields loggingFields;
    private ActivityLogger<TestContext> actionLogger;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        producer = mock(ActivityLogProducer.class);
        when(producer.loggerName()).thenReturn(loggerName);
        writer = mock(ActivityLogWriter.class);
        ActionLoggingFieldsProvider fieldProvider = mock(ActionLoggingFieldsProvider.class);
        loggingFields = mock(ActionLoggingFields.class);

        when(fieldProvider.create(any())).thenReturn(loggingFields);
        var writerProvider = mock(ActivityLogWriterProvider.class);
        when(writerProvider.getWriter(loggerName)).thenReturn(writer);

        actionLogger = new ActivityLogger<>(producer, writerProvider, fieldProvider);
    }

    private ESLogMessage randomMessage() {
        return new ESLogMessage().with(randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testLogActionDisabled() {
        TestContext context = new TestContext(100);
        actionLogger.logAction(context);
        verifyNoInteractions(writer);
    }

    public void testLogActionEnabled() {
        actionLogger.enabled = true;
        TestContext context = new TestContext(100);

        ESLogMessage randomMessage = randomMessage();

        when(producer.produce(context, loggingFields)).thenReturn(Optional.of(randomMessage));

        actionLogger.logAction(context);

        verify(writer).write(actionLogger.getLogLevel(), randomMessage);
    }

    public void testLogActionBelowThreshold() {
        actionLogger.enabled = true;
        actionLogger.threshold = TimeValue.timeValueMillis(100).nanos();

        TestContext context = new TestContext(TimeValue.timeValueMillis(50).nanos());
        actionLogger.logAction(context);

        verifyNoInteractions(writer);
    }

    public void testLogActionAboveThreshold() {
        actionLogger.enabled = true;
        actionLogger.threshold = TimeValue.timeValueMillis(100).nanos();

        TestContext context = new TestContext(TimeValue.timeValueMillis(150).nanos());
        ESLogMessage randomMessage = randomMessage();

        when(producer.produce(context, loggingFields)).thenReturn(Optional.of(randomMessage));

        actionLogger.logAction(context);

        verify(writer).write(actionLogger.getLogLevel(), randomMessage);
    }

    @SuppressWarnings("unchecked")
    public void testWrapDisabled() {
        ActionListener<String> listener = ActionListener.noop();
        ActivityLoggerContextBuilder<TestContext, String, String> builder = mock(ActivityLoggerContextBuilder.class);

        ActionListener<String> wrapped = actionLogger.wrap(listener, builder);

        assertThat(wrapped, sameInstance(listener));
        verifyNoInteractions(builder);
    }

    @SuppressWarnings("unchecked")
    public void testFollowersAreCalledWhenLoggingIsDisabled() {
        TestContext context = new TestContext(100);
        ActivityLoggerContextBuilder<TestContext, String, String> builder = mock(ActivityLoggerContextBuilder.class);
        when(builder.build("response")).thenReturn(context);
        List<TestContext> followed = new ArrayList<>();
        AtomicReference<String> response = new AtomicReference<>();

        ActionListener<String> wrapped = actionLogger.wrap(
            ActionListener.wrap(response::set, e -> fail("the action did not fail")),
            builder,
            List.of(followed::add)
        );
        wrapped.onResponse("response");

        assertThat(followed, contains(context));
        assertThat(response.get(), equalTo("response"));
        verifyNoInteractions(writer);
    }

    public void testFollowersAreCalledWhateverTheThreshold() {
        actionLogger.enabled = true;
        actionLogger.threshold = TimeValue.timeValueMillis(100).nanos();
        TestContext context = new TestContext(TimeValue.timeValueMillis(50).nanos());
        List<TestContext> followed = new ArrayList<>();

        actionLogger.logAction(context, List.of(followed::add));

        assertThat(followed, contains(context));
        verifyNoInteractions(writer);
    }

    @SuppressWarnings("unchecked")
    public void testFailingFollowerDoesNotFailTheAction() {
        ActivityLoggerContextBuilder<TestContext, String, String> builder = mock(ActivityLoggerContextBuilder.class);
        when(builder.build("response")).thenReturn(new TestContext(100));
        List<TestContext> followed = new ArrayList<>();
        AtomicReference<String> response = new AtomicReference<>();
        Consumer<TestContext> failing = context -> { throw new IllegalStateException("a listener that fails"); };

        ActionListener<String> wrapped = actionLogger.wrap(
            ActionListener.wrap(response::set, e -> fail("the action did not fail")),
            builder,
            List.of(failing, followed::add)
        );
        wrapped.onResponse("response");

        assertThat("the next follower is still called", followed, hasSize(1));
        assertThat("and so is the caller", response.get(), equalTo("response"));
    }

    private static class TestContext extends ActivityLoggerContext {
        TestContext(long tookInNanos) {
            super(mock(Task.class), "test", tookInNanos);
        }

        TestContext(Exception error) {
            super(mock(Task.class), "test", 0, error);
        }
    }
}
