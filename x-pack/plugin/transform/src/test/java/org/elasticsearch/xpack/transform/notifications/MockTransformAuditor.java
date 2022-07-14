/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.transform.notifications.TransformAuditMessage;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants.AUDIT_INDEX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/*
 * Test mock auditor to verify audit expectations.
 *
 * Shamelessly cop...inspired by {@link org.elasticsearch.test.MockLogAppender}
 *
 * TODO: ideally this would be a generalized  MockAuditor, but the current inheritance doesn't let us
 */
public class MockTransformAuditor extends TransformAuditor {

    private static final String MOCK_NODE_NAME = "mock_node_name";
    private static final Logger logger = LogManager.getLogger(MockTransformAuditor.class);

    @SuppressWarnings("unchecked")
    public static MockTransformAuditor createMockAuditor() {
        Map<String, IndexTemplateMetadata> templates = Map.of(AUDIT_INDEX, mock(IndexTemplateMetadata.class));
        Metadata metadata = mock(Metadata.class);
        when(metadata.getTemplates()).thenReturn(templates);
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(metadata);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        return new MockTransformAuditor(clusterService);
    }

    private final List<AuditExpectation> expectations;

    public MockTransformAuditor(ClusterService clusterService) {
        super(mock(Client.class), MOCK_NODE_NAME, clusterService);
        expectations = new CopyOnWriteArrayList<>();
    }

    /**
     * Adds an audit expectation.
     * Must be called *before* the code that uses auditor's {@code info}, {@code warning} or {@code error} methods.
     */
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
        protected final int expectedCount;
        volatile int count;

        public AbstractAuditExpectation(
            String expectedName,
            Level expectedLevel,
            String expectedResourceId,
            String expectedMessage,
            int expectedCount
        ) {
            this.expectedName = expectedName;
            this.expectedLevel = expectedLevel;
            this.expectedResourceId = expectedResourceId;
            this.expectedMessage = expectedMessage;
            this.expectedCount = expectedCount;
            this.count = 0;
        }

        @Override
        public void match(final Level level, final String resourceId, final String message) {
            if (level.equals(expectedLevel) && resourceId.equals(expectedResourceId) && innerMatch(level, resourceId, message)) {
                if (Regex.isSimpleMatchPattern(expectedMessage)) {
                    if (Regex.simpleMatch(expectedMessage, message)) {
                        ++count;
                    }
                } else {
                    if (message.contains(expectedMessage)) {
                        ++count;
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
            super(expectedName, expectedLevel, expectedResourceId, expectedMessage, 1);
        }

        @Override
        public void assertMatched() {
            assertThat("expected to see " + expectedName + " but did not", count, equalTo(expectedCount));
        }
    }

    public static class UnseenAuditExpectation extends AbstractAuditExpectation {

        public UnseenAuditExpectation(String expectedName, Level expectedLevel, String expectedResourceId, String expectedMessage) {
            super(expectedName, expectedLevel, expectedResourceId, expectedMessage, 0);
        }

        @Override
        public void assertMatched() {
            assertThat("expected not to see " + expectedName + " but did", count, equalTo(expectedCount));
        }
    }

    public static class MultipleSeenAuditExpectation extends AbstractAuditExpectation {

        public MultipleSeenAuditExpectation(
            String expectedName,
            Level expectedLevel,
            String expectedResourceId,
            String expectedMessage,
            int expectedCount
        ) {
            super(expectedName, expectedLevel, expectedResourceId, expectedMessage, expectedCount);
        }

        @Override
        public void assertMatched() {
            assertThat(
                "expected to see " + expectedName + " " + expectedCount + " times but saw it " + count + " times ",
                count,
                equalTo(expectedCount)
            );
        }
    }

    private void audit(Level level, String resourceId, String message) {
        logger.info("AUDIT: {}", new TransformAuditMessage(resourceId, message, level, new Date(), MOCK_NODE_NAME));

        for (AuditExpectation expectation : expectations) {
            expectation.match(level, resourceId, message);
        }
    }

}
