/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.test.ESTestCase;

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class TransformMessagesTests extends ESTestCase {

    public void testGetMessage_WithFormatStrings() {
        String formattedMessage = TransformMessages.getMessage(
            TransformMessages.REST_STOP_TRANSFORM_WAIT_FOR_COMPLETION_TIMEOUT,
            "30s",
            "my_transform"
        );
        assertEquals("Timed out after [30s] while waiting for transform [my_transform] to stop", formattedMessage);
    }

    public void testMessageProperFormat() throws IllegalArgumentException, IllegalAccessException {
        Field[] declaredFields = TransformMessages.class.getFields();
        int checkedMessages = 0;

        for (Field field : declaredFields) {
            int modifiers = field.getModifiers();
            if (java.lang.reflect.Modifier.isStatic(modifiers)
                && java.lang.reflect.Modifier.isFinal(modifiers)
                && field.getType().isAssignableFrom(String.class)) {

                assertSingleMessage((String) field.get(TransformMessages.class));
                ++checkedMessages;
            }
        }
        assertTrue(checkedMessages > 0);
        logger.info("Checked {} messages", checkedMessages);
    }

    public void testAssertSingleMessage() {
        expectThrows(RuntimeException.class, () -> innerAssertSingleMessage("missing zero position {1} {1}"));
        expectThrows(RuntimeException.class, () -> innerAssertSingleMessage("incomplete {}"));
        expectThrows(RuntimeException.class, () -> innerAssertSingleMessage("count from 1 {1}"));
    }

    private void assertSingleMessage(String message) {
        // for testing the test method, we can not assert directly, but wrap it with an exception, which also
        // nicely encapsulate parsing errors thrown by MessageFormat itself
        try {
            innerAssertSingleMessage(message);
        } catch (Exception e) {
            fail("message: " + message + " failure: " + e.getMessage());
        }
    }

    private void innerAssertSingleMessage(String message) {
        MessageFormat messageWithNoArguments = new MessageFormat(message, Locale.ROOT);
        int numberOfArguments = messageWithNoArguments.getFormats().length;

        List<String> args = new ArrayList<>();
        for (int i = 0; i < numberOfArguments; ++i) {
            args.add(randomAlphaOfLength(5));
        }

        String properFormatedMessage = new MessageFormat(message, Locale.ROOT).format(args.toArray(new String[0]));
        for (String arg : args) {
            if (properFormatedMessage.contains(arg) == false) {
                throw new RuntimeException("Message check: [" + message + "] failed, missing argument");
            }
        }
    }
}
