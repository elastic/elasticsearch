/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingAppender;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeprecationIndexingAppenderTests extends ESTestCase {

    private DeprecationIndexingAppender appender;
    private Layout<String> layout;
    private Consumer<IndexRequest> consumer;

    @Before
    @SuppressWarnings("unchecked")
    public void initialize() {
        layout = mock(Layout.class);
        consumer = mock(Consumer.class);
        appender = new DeprecationIndexingAppender("a name", null, layout, consumer);
    }

    /**
     * Checks that the service does not attempt to index messages when the service
     * is disabled.
     */
    public void testDoesNotWriteMessageWhenServiceDisabled() {
        appender.append(mock(LogEvent.class));

        verify(consumer, never()).accept(any());
    }

    /**
     * Checks that the service can be disabled after being enabled.
     */
    public void testDoesNotWriteMessageWhenServiceEnabledAndDisabled() {
        appender.setEnabled(true);
        appender.setEnabled(false);

        appender.append(mock(LogEvent.class));

        verify(consumer, never()).accept(any());
    }

    /**
     * Checks that messages are indexed in the correct shape when the service is enabled.
     * Formatted is handled entirely by the configured Layout, so that is not verified here.
     */
    public void testWritesMessageWhenServiceEnabled() {
        appender.setEnabled(true);

        when(layout.toByteArray(any())).thenReturn("{ \"some key\": \"some value\" }".getBytes(StandardCharsets.UTF_8));

        appender.append(mock(LogEvent.class));

        ArgumentCaptor<IndexRequest> argument = ArgumentCaptor.forClass(IndexRequest.class);

        verify(consumer).accept(argument.capture());

        final IndexRequest indexRequest = argument.getValue();
        final Map<String, Object> payloadMap = indexRequest.sourceAsMap();

        assertThat(payloadMap, hasEntry("some key", "some value"));
    }
}
