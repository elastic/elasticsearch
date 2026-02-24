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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Optional;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class ActivityLoggerTests extends ESTestCase {

    private final String loggerName = "test_logger";
    private ClusterSettings clusterSettings;
    private ActivityLogProducer<TestContext> producer;
    private ActivityLogWriter writer;
    private ActionLoggingFields loggingFields;
    private ActivityLogger<TestContext> actionLogger;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        producer = mock(ActivityLogProducer.class);
        when(producer.loggerName()).thenReturn(loggerName);
        writer = mock(ActivityLogWriter.class);
        ActionLoggingFieldsProvider fieldProvider = mock(ActionLoggingFieldsProvider.class);
        loggingFields = mock(ActionLoggingFields.class);

        when(fieldProvider.create(any())).thenReturn(loggingFields);
        var writerProvider = mock(ActivityLogWriterProvider.class);
        when(writerProvider.getWriter(loggerName)).thenReturn(writer);

        actionLogger = new ActivityLogger<>(loggerName, clusterSettings, producer, writerProvider, fieldProvider);
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
        enableLogger();
        TestContext context = new TestContext(100);

        ESLogMessage randomMessage = randomMessage();

        when(producer.produce(context, loggingFields)).thenReturn(Optional.of(randomMessage));

        actionLogger.logAction(context);

        verify(writer).write(actionLogger.getLogLevel(), randomMessage);
    }

    public void testLogActionBelowThreshold() {
        setThreshold(TimeValue.timeValueMillis(100));

        TestContext context = new TestContext(TimeValue.timeValueMillis(50).nanos());
        actionLogger.logAction(context);

        verifyNoInteractions(writer);
    }

    public void testLogActionAboveThreshold() {
        setThreshold(TimeValue.timeValueMillis(100));

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

    private void enableLogger() {
        clusterSettings.applySettings(
            Settings.builder().put(ActivityLogger.ACTIVITY_LOGGER_ENABLED.getConcreteSettingForNamespace(loggerName).getKey(), true).build()
        );
    }

    // Setting the threshold includes enabling since there's no point to have it on disabled logger
    private void setThreshold(TimeValue threshold) {
        clusterSettings.applySettings(
            Settings.builder()
                .put(ActivityLogger.ACTIVITY_LOGGER_ENABLED.getConcreteSettingForNamespace(loggerName).getKey(), true)
                .put(ActivityLogger.ACTIVITY_LOGGER_THRESHOLD.getConcreteSettingForNamespace(loggerName).getKey(), threshold)
                .build()
        );
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
