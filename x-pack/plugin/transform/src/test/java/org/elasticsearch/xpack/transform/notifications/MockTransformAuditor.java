/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.notifications;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.core.common.notifications.Level;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

/*
 * Test mock auditor to verify audit expectations.
 *
 * Shamelessly cop...inspired by {@link org.elasticsearch.test.MockLogAppender}
 *
 * TODO: ideally this would be a generalized  MockAuditor, but the current inheritance doesn't let us
 */
public class MockTransformAuditor extends TransformAuditor {

    private List<AuditExpectation> expectations;

    public MockTransformAuditor() {
        super(mock(Client.class), "mock_node_name");
        expectations = new CopyOnWriteArrayList<>();
    }

    public void addExpectation(AuditExpectation expectation) {
        expectations.add(expectation);
    }

    // we can dynamically change the auditor, like attaching and removing the log appender
    public void reset() {
        expectations.clear();
    }

    @Override
    public void info(String resourceId, String message) {
        audit(Level.INFO, resourceId, message);
    }

    @Override
    public void warning(String resourceId, String message) {
        audit(Level.WARNING, resourceId, message);
    }

    @Override
    public void error(String resourceId, String message) {
        audit(Level.ERROR, resourceId, message);
    }

    public void assertAllExpectationsMatched() {
        for (AuditExpectation expectation : expectations) {
            expectation.assertMatched();
        }
    }

    public interface AuditExpectation {
        void match(Level level, String resourceId, String message);

        void assertMatched();
    }

    public abstract static class AbstractAuditExpectation implements AuditExpectation {
        protected final String expectedName;
        protected final Level expectedLevel;
        protected final String expectedResourceId;
        protected final String expectedMessage;
        volatile boolean saw;

        public AbstractAuditExpectation(String expectedName, Level expectedLevel, String expectedResourceId, String expectedMessage) {
            this.expectedName = expectedName;
            this.expectedLevel = expectedLevel;
            this.expectedResourceId = expectedResourceId;
            this.expectedMessage = expectedMessage;
            this.saw = false;
        }

        @Override
        public void match(final Level level, final String resourceId, final String message) {
            if (level.equals(expectedLevel) && resourceId.equals(expectedResourceId) && innerMatch(level, resourceId, message)) {
                if (Regex.isSimpleMatchPattern(expectedMessage)) {
                    if (Regex.simpleMatch(expectedMessage, message)) {
                        saw = true;
                    }
                } else {
                    if (message.contains(expectedMessage)) {
                        saw = true;
                    }
                }
            }
        }

        public boolean innerMatch(final Level level, final String resourceId, final String message) {
            return true;
        }
    }

    public static class SeenAuditExpectation extends AbstractAuditExpectation {

        public SeenAuditExpectation(String expectedName, Level expectedLevel, String expectedResourceId, String expectedMessage) {
            super(expectedName, expectedLevel, expectedResourceId, expectedMessage);
        }

        @Override
        public void assertMatched() {
            assertThat("expected to see " + expectedName + " but did not", saw, equalTo(true));
        }
    }

    public static class UnseenAuditExpectation extends AbstractAuditExpectation {

        public UnseenAuditExpectation(String expectedName, Level expectedLevel, String expectedResourceId, String expectedMessage) {
            super(expectedName, expectedLevel, expectedResourceId, expectedMessage);
        }

        @Override
        public void assertMatched() {
            assertThat("expected not to see " + expectedName + " but did", saw, equalTo(false));
        }
    }


    private void audit(Level level, String resourceId, String message) {
        for (AuditExpectation expectation : expectations) {
            expectation.match(level, resourceId, message);
        }
    }

}
