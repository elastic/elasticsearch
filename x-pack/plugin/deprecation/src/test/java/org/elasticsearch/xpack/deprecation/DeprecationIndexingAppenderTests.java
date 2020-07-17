/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.DeprecatedMessage;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingAppender;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeprecationIndexingAppenderTests extends ESTestCase {

    private DeprecationIndexingAppender appender;
    private Consumer<IndexRequest> consumer;

    @Before
    @SuppressWarnings("unchecked")
    public void initialize() {
        consumer = mock(Consumer.class);
        appender = new DeprecationIndexingAppender(consumer, "a name", null);

        appender.setClusterUUID("cluster-uuid");
        appender.setNodeId("local-node-id");
    }

    /**
     * Checks that the service does not attempt to index messages when the service
     * is disabled.
     */
    public void testDoesNotWriteMessageWhenServiceDisabled() {
        appender.append(buildEvent());

        verify(consumer, never()).accept(any());
    }

    /**
     * Checks that the service can be disabled after being enabled.
     */
    public void testDoesNotWriteMessageWhenServiceEnabledAndDisabled() {
        appender.setEnabled(true);
        appender.setEnabled(false);

        appender.append(buildEvent());

        verify(consumer, never()).accept(any());
    }

    /**
     * Checks that messages are indexed in the correct shape when the service is enabled.
     */
    public void testWritesMessageWhenServiceEnabled() {
        appender.setEnabled(true);

        final Map<String, Object> payloadMap = getWriteRequest("a key", DeprecatedMessage.of(null, "a message"));

        assertThat(payloadMap, hasKey("@timestamp"));
        assertThat(payloadMap, hasEntry("key", "a key"));
        assertThat(payloadMap, hasEntry("message", "a message"));
        assertThat(payloadMap, hasEntry("cluster.uuid", "cluster-uuid"));
        assertThat(payloadMap, hasEntry("node.id", "local-node-id"));
        // Neither of these should exist since we passed null when writing the message
        assertThat(payloadMap, not(hasKey("x-opaque-id")));
    }

    /**
     * Check that if an xOpaqueId is set, then it is added to the index request payload.
     */
    public void testMessageIncludesOpaqueIdWhenSupplied() {
        appender.setEnabled(true);

        final Map<String, Object> payloadMap = getWriteRequest("a key", DeprecatedMessage.of("an ID", "a message"));

        assertThat(payloadMap, hasEntry("x-opaque-id", "an ID"));
    }

    /**
     * Check that if any arguments are set, then they substituted in the log message
     */
    public void testMessageSubstitutesArgumentsWhenSupplied() {
        appender.setEnabled(true);

        final Map<String, Object> payloadMap = getWriteRequest(
            "a key",
            DeprecatedMessage.of(null, "a {} and {} message", "first", "second")
        );

        assertThat(payloadMap, hasEntry("message", "a first and second message"));
    }

    /*
     * Wraps up the steps for extracting an index request payload from the mocks.
     */
    private Map<String, Object> getWriteRequest(String key, ESLogMessage message) {
        message.field("key", key);
        LogEvent logEvent = mock(LogEvent.class);
        when(logEvent.getMessage()).thenReturn(message);

        appender.append(logEvent);

        ArgumentCaptor<IndexRequest> argument = ArgumentCaptor.forClass(IndexRequest.class);

        verify(consumer).accept(argument.capture());

        final IndexRequest indexRequest = argument.getValue();
        return indexRequest.sourceAsMap();
    }

    private LogEvent buildEvent() {
        LogEvent logEvent = mock(LogEvent.class);
        when(logEvent.getMessage()).thenReturn(DeprecatedMessage.of(null, "a message").field("key", "a key"));
        return logEvent;
    }
}
