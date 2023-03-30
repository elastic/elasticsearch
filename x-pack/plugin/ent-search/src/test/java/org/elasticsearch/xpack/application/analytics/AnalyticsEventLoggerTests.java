/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsEventLoggerTests extends ESTestCase {

    public void testLogEvent() throws IOException {
        AnalyticsEventLogger eventLogger = new AnalyticsEventLogger();

        AnalyticsEvent event = mock(AnalyticsEvent.class);
        when(event.toXContent(any(), any())).thenAnswer(i -> {
            XContentBuilder builder = i.getArgument(0, XContentBuilder.class);
            return builder.value(event.hashCode());
        });

        Logger logger = LogManager.getLogger(AnalyticsEventLogger.class);
        MockLogAppender mockLogAppender = new MockLogAppender();
        Loggers.addAppender(logger, mockLogAppender);
        mockLogAppender.start();

        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation("event message", logger.getName(), Level.INFO, String.valueOf(event.hashCode()))
        );

        eventLogger.logEvent(event);

        mockLogAppender.assertAllExpectationsMatched();

        mockLogAppender.stop();
        Loggers.removeAppender(logger, mockLogAppender);
    }
}
