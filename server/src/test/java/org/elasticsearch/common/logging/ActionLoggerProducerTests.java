/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.action.ActionLoggerContext;
import org.elasticsearch.common.logging.action.ActionLoggerProducer;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActionLoggerProducerTests extends ESTestCase {

    ActionLoggerProducer<ActionLoggerContext> producer;

    @Before
    public void setup() {
        producer = new ActionLoggerProducer<>() {
            @Override
            public ESLogMessage produce(ActionLoggerContext ctx, SlowLogFields additionalFields) {
                return produceCommon(ctx, additionalFields);
            }

            @Override
            public Level logLevel(ActionLoggerContext ctx) {
                return Level.INFO;
            }
        };
    }

    private static ActionLoggerContext makeSuccessContext() {
        ActionLoggerContext context = mock(ActionLoggerContext.class);
        when(context.getType()).thenReturn("testType");
        when(context.getTaskId()).thenReturn("test_task");
        when(context.getTookInNanos()).thenReturn(1_000_000L);
        when(context.isSuccess()).thenReturn(true);
        return context;
    }

    private static ActionLoggerContext makeFailContext() {
        ActionLoggerContext context = mock(ActionLoggerContext.class);
        when(context.getType()).thenReturn("failType");
        when(context.getTaskId()).thenReturn("test_task2");
        when(context.getTookInNanos()).thenReturn(1_000L);
        when(context.isSuccess()).thenReturn(false);
        when(context.getErrorType()).thenReturn("SomeError");
        when(context.getErrorMessage()).thenReturn("Something went wrong");
        return context;
    }

    private static SlowLogFields makeFields() {
        SlowLogFields fields = mock(SlowLogFields.class);
        when(fields.queryFields()).thenReturn(Map.of("foo", "bar"));
        return fields;
    }

    public void testSuccess() {
        ESLogMessage message = producer.produce(makeSuccessContext(), makeFields());

        assertEquals("test_task", message.get("task_id"));
        assertEquals("1000000", message.get("took"));
        assertEquals(String.valueOf(TimeUnit.NANOSECONDS.toMillis(1_000_000L)), message.get("took_millis"));
        assertEquals("true", message.get("success"));
        assertEquals("testType", message.get("type"));
        assertEquals("bar", message.get("foo"));
        assertFalse(message.containsKey("error.type"));
        assertFalse(message.containsKey("error.message"));
    }

    public void testProduceCommonFailure() {
        ESLogMessage message = producer.produce(makeFailContext(), makeFields());

        assertEquals("test_task2", message.get("task_id"));
        assertEquals("1000", message.get("took"));
        assertEquals(String.valueOf(TimeUnit.NANOSECONDS.toMillis(1_000L)), message.get("took_millis"));
        assertEquals("false", message.get("success"));
        assertEquals("failType", message.get("type"));
        assertEquals("SomeError", message.get("error.type"));
        assertEquals("Something went wrong", message.get("error.message"));
        assertEquals("bar", message.get("foo"));
    }
}
