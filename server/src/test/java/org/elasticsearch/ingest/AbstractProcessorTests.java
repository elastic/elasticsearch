/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractProcessorTests extends ESTestCase {

    public void testLogAndBuildException() {
        final LoggerContextFactory originalFactory = LogManager.getFactory();
        try {
            final String message = randomAlphaOfLength(100);
            final String throwableMessage = randomBoolean() ? null : randomAlphaOfLength(100);
            AtomicBoolean warnCalled = new AtomicBoolean(false);
            AtomicBoolean traceCalled = new AtomicBoolean(false);
            final Throwable throwable = randomFrom(
                new StackOverflowError(throwableMessage),
                new RuntimeException(throwableMessage),
                new IOException(throwableMessage)
            );

            {
                // Mock logging so that we can make sure we're logging what we expect:
                Logger mockLogger = mock(Logger.class);
                doAnswer(invocationOnMock -> {
                    warnCalled.set(true);
                    String logMessage = invocationOnMock.getArgument(0, String.class);
                    assertThat(logMessage, containsString(message));
                    if (throwableMessage != null) {
                        assertThat(logMessage, containsString(throwableMessage));
                    }
                    return null;
                }).when(mockLogger).warn(anyString());

                doAnswer(invocationOnMock -> {
                    traceCalled.set(true);
                    String logMessage = invocationOnMock.getArgument(0, String.class);
                    Throwable logThrowable = invocationOnMock.getArgument(1, Throwable.class);
                    assertThat(logMessage, containsString(message));
                    if (throwableMessage != null) {
                        assertThat(logMessage, not(containsString(throwableMessage)));
                    }
                    assertThat(logThrowable, equalTo(throwable));
                    return null;
                }).when(mockLogger).trace(anyString(), any(Throwable.class));

                final LoggerContext context = Mockito.mock(LoggerContext.class);
                when(context.getLogger(TestProcessor.class)).thenReturn(mockLogger);

                final LoggerContextFactory spy = Mockito.spy(originalFactory);
                Mockito.doReturn(context).when(spy).getContext(any(), any(), any(), anyBoolean());
                LogManager.setFactory(spy);
            }

            TestProcessor testProcessor = new TestProcessor();

            {
                // Run with trace logging disabled
                ElasticsearchException resultException = testProcessor.logAndBuildException(message, throwable);
                assertThat(resultException.getRootCause(), equalTo(resultException));
                String resultMessage = resultException.getMessage();
                assertNotNull(resultMessage);
                if (throwableMessage != null) {
                    assertThat(resultMessage, containsString(throwableMessage));
                }
                assertThat(resultMessage, containsString(message));

                assertThat("log.warn not called", warnCalled.get(), is(true));
                assertThat("log.trace called", traceCalled.get(), is(false));
            }

            // reset between tests:
            warnCalled.set(false);
            traceCalled.set(false);

            {
                // Now enable trace logging
                when(LogManager.getLogger(TestProcessor.class).isTraceEnabled()).thenReturn(true);
                ElasticsearchException resultException = testProcessor.logAndBuildException(message, throwable);
                assertThat(resultException.getRootCause(), equalTo(resultException));
                String resultMessage = resultException.getMessage();
                assertNotNull(resultMessage);
                if (throwableMessage != null) {
                    assertThat(resultMessage, containsString(throwableMessage));
                }
                assertThat(resultMessage, containsString(message));

                assertThat("log.warn called", warnCalled.get(), is(false));
                assertThat("log.trace not called", traceCalled.get(), is(true));
            }
        } finally {
            LogManager.setFactory(originalFactory);
        }
    }

    class TestProcessor extends AbstractProcessor {

        protected TestProcessor() {
            super("", "");

        }

        @Override
        public String getType() {
            return "test";
        }
    }
}
