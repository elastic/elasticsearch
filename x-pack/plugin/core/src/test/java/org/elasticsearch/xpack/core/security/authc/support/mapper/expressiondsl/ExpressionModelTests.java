/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class ExpressionModelTests extends ESTestCase {

    @Before
    public void enableDebugLogging() {
        Loggers.setLevel(LogManager.getLogger(ExpressionModel.class), Level.DEBUG);
    }

    public void testCheckFailureAgainstUndefinedFieldLogsMessage() throws Exception {
        ExpressionModel model = new ExpressionModel();
        model.defineField("some_int", randomIntBetween(1, 99));

        MockLog.assertThatLogger(
            () -> assertThat(model.test("another_field", List.of(new FieldValue("bork"), new FieldValue("bork!"))), is(false)),
            ExpressionModel.class,
            new MockLog.SeenEventExpectation(
                "undefined field",
                model.getClass().getName(),
                Level.DEBUG,
                "Attempt to test field [another_field] against value(s) [bork,bork!],"
                    + " but the field [another_field] does not have a value on this object; known fields are [some_int]"
            )
        );
    }

    public void testCheckSuccessAgainstUndefinedFieldDoesNotLog() throws Exception {
        ExpressionModel model = new ExpressionModel();
        model.defineField("some_int", randomIntBetween(1, 99));

        MockLog.assertThatLogger(
            () -> assertThat(model.test("another_field", List.of(new FieldValue(null))), is(true)),
            ExpressionModel.class,
            new NoMessagesExpectation()
        );
    }

    public void testCheckAgainstDefinedFieldDoesNotLog() throws Exception {
        ExpressionModel model = new ExpressionModel();
        model.defineField("some_int", randomIntBetween(1, 99));

        MockLog.assertThatLogger(
            () -> assertThat(model.test("some_int", List.of(new FieldValue(randomIntBetween(100, 200)))), is(false)),
            ExpressionModel.class,
            new NoMessagesExpectation()
        );
    }

    private class NoMessagesExpectation implements MockLog.LoggingExpectation {

        private final List<Message> messages = new ArrayList<>();

        @Override
        public void match(LogEvent event) {
            messages.add(event.getMessage());
        }

        @Override
        public void assertMatched() {
            assertThat(messages, empty());
        }
    }

}
