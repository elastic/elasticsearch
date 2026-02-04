/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActionLoggerProducerTests extends ESTestCase {

    ActionLoggerProducer<ActionLoggerContext> producer;

    @Before
    public void setup() {
        producer = new ActionLoggerProducer<>() {
            @Override
            public ESLogMessage produce(ActionLoggerContext ctx, ActionLoggingFields additionalFields) {
                return produceCommon(ctx, additionalFields);
            }

            @Override
            public String loggerName() {
                return "testLogger";
            }
        };
    }

    private static ActionLoggerContext makeSuccessContext() {
        ActionLoggerContext context = mock(ActionLoggerContext.class);
        when(context.getType()).thenReturn("testType");
        when(context.getOpaqueId()).thenReturn("test_task");
        when(context.getTookInNanos()).thenReturn(1_000_000L);
        when(context.isSuccess()).thenReturn(true);
        return context;
    }

    private static ActionLoggerContext makeFailContext() {
        ActionLoggerContext context = mock(ActionLoggerContext.class);
        when(context.getType()).thenReturn("failType");
        when(context.getOpaqueId()).thenReturn("test_task2");
        when(context.getTookInNanos()).thenReturn(1_000L);
        when(context.isSuccess()).thenReturn(false);
        when(context.getErrorType()).thenReturn("SomeError");
        when(context.getErrorMessage()).thenReturn("Something went wrong");
        return context;
    }

    private static ActionLoggingFields makeFields() {
        ActionLoggingFields fields = mock(ActionLoggingFields.class);
        when(fields.logFields()).thenReturn(Map.of("foo", "bar"));
        return fields;
    }

    public void testSuccess() {
        ESLogMessage message = producer.produce(makeSuccessContext(), makeFields());

        assertThat(message.get("x_opaque_id"), equalTo("test_task"));
        assertThat(message.get("took"), equalTo("1000000"));
        assertThat(message.get("took_millis"), equalTo(String.valueOf(TimeUnit.NANOSECONDS.toMillis(1_000_000L))));
        assertThat(message.get("success"), equalTo("true"));
        assertThat(message.get("type"), equalTo("testType"));
        assertThat(message.get("foo"), equalTo("bar"));
        assertNull(message.get("error.type"));
        assertNull(message.get("error.message"));
    }

    public void testProduceCommonFailure() {
        ESLogMessage message = producer.produce(makeFailContext(), makeFields());

        assertThat(message.get("x_opaque_id"), equalTo("test_task2"));
        assertThat(message.get("took"), equalTo("1000"));
        assertThat(message.get("took_millis"), equalTo(String.valueOf(TimeUnit.NANOSECONDS.toMillis(1_000L))));
        assertThat(message.get("success"), equalTo("false"));
        assertThat(message.get("type"), equalTo("failType"));
        assertThat(message.get("error.type"), equalTo("SomeError"));
        assertThat(message.get("error.message"), equalTo("Something went wrong"));
        assertThat(message.get("foo"), equalTo("bar"));
    }
}
