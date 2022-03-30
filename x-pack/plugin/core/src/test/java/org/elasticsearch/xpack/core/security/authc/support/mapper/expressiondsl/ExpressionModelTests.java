/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.core.MockLogAppender;
import org.elasticsearch.logging.spi.AppenderSupport;
import org.elasticsearch.logging.spi.LogLevelSupport;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class ExpressionModelTests extends ESTestCase {

    @Before
    public void enableDebugLogging() {
        LogLevelSupport.provider().setLevel(LogManager.getLogger(ExpressionModel.class), Level.DEBUG);
    }

    public void testCheckFailureAgainstUndefinedFieldLogsMessage() throws Exception {
        ExpressionModel model = new ExpressionModel();
        model.defineField("some_int", randomIntBetween(1, 99));

        doWithLoggingExpectations(
            List.of(
                MockLogAppender.createSeenEventExpectation(
                    "undefined field",
                    model.getClass().getName(),
                    Level.DEBUG,
                    "Attempt to test field [another_field] against value(s) [bork,bork!],"
                        + " but the field [another_field] does not have a value on this object; known fields are [some_int]"
                )
            ),
            () -> assertThat(model.test("another_field", List.of(new FieldValue("bork"), new FieldValue("bork!"))), is(false))
        );
    }

    public void testCheckSuccessAgainstUndefinedFieldDoesNotLog() throws Exception {
        ExpressionModel model = new ExpressionModel();
        model.defineField("some_int", randomIntBetween(1, 99));

        // doWithLoggingExpectations(
        // List.of(new NoMessagesExpectation()),
        // () -> assertThat(model.test("another_field", List.of(new FieldValue(null))), is(true))
        // );
    }

    public void testCheckAgainstDefinedFieldDoesNotLog() throws Exception {
        ExpressionModel model = new ExpressionModel();
        model.defineField("some_int", randomIntBetween(1, 99));

        // doWithLoggingExpectations(
        // List.of(new NoMessagesExpectation()),
        // () -> assertThat(model.test("some_int", List.of(new FieldValue(randomIntBetween(100, 200)))), is(false))
        // );
    }

    private void doWithLoggingExpectations(List<? extends MockLogAppender.LoggingExpectation> expectations, CheckedRunnable<Exception> body)
        throws Exception {
        final Logger modelLogger = LogManager.getLogger(ExpressionModel.class);
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        try {
            AppenderSupport.provider().addAppender(modelLogger, mockAppender);
            expectations.forEach(mockAppender::addExpectation);

            body.run();

            mockAppender.assertAllExpectationsMatched();
        } finally {
            AppenderSupport.provider().removeAppender(modelLogger, mockAppender);
            mockAppender.stop();
        }
    }

    // private class NoMessagesExpectation implements MockLogAppender.LoggingExpectation {
    //
    // private List<Message> messages = new ArrayList<>();
    //
    // @Override
    // public void match(LogEvent event) {
    // messages.add(event.getMessage());
    // }
    //
    // @Override
    // public void assertMatched() {
    // assertThat(messages, empty());
    // }
    // }

}
