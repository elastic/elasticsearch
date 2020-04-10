/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;

import static org.hamcrest.Matchers.equalTo;

public class AbstractAuditMessageTests extends AbstractXContentTestCase<AbstractAuditMessageTests.TestAuditMessage> {

    static class TestAuditMessage extends AbstractAuditMessage {

        private static final ParseField TEST_ID = new ParseField("test_id");
        public static final ConstructingObjectParser<TestAuditMessage, Void> PARSER =
            createParser("test_audit_message", TestAuditMessage::new, TEST_ID);

        TestAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
            super(resourceId, message, level, timestamp, nodeName);
        }

        @Override
        public String getJobType() {
            return "test_type";
        }

        @Override
        protected String getResourceField() {
            return TEST_ID.getPreferredName();
        }
    }

    private static final String RESOURCE_ID = "foo";
    private static final String MESSAGE = "some message";
    private static final Date TIMESTAMP = new Date(123456789);
    private static final String NODE_NAME = "some_node";

    public void testGetResourceField() {
        TestAuditMessage message = new TestAuditMessage(RESOURCE_ID, MESSAGE, Level.INFO, TIMESTAMP, NODE_NAME);
        assertThat(message.getResourceField(), equalTo(TestAuditMessage.TEST_ID.getPreferredName()));
    }

    public void testGetJobType() {
        TestAuditMessage message = createTestInstance();
        assertThat(message.getJobType(), equalTo("test_type"));
    }

    public void testNewInfo() {
        TestAuditMessage message = new TestAuditMessage(RESOURCE_ID, MESSAGE, Level.INFO, TIMESTAMP, NODE_NAME);
        assertThat(message.getResourceId(), equalTo(RESOURCE_ID));
        assertThat(message.getMessage(), equalTo(MESSAGE));
        assertThat(message.getLevel(), equalTo(Level.INFO));
        assertThat(message.getTimestamp(), equalTo(TIMESTAMP));
        assertThat(message.getNodeName(), equalTo(NODE_NAME));
    }

    public void testNewWarning() {
        TestAuditMessage message = new TestAuditMessage(RESOURCE_ID, MESSAGE, Level.WARNING, TIMESTAMP, NODE_NAME);
        assertThat(message.getResourceId(), equalTo(RESOURCE_ID));
        assertThat(message.getMessage(), equalTo(MESSAGE));
        assertThat(message.getLevel(), equalTo(Level.WARNING));
        assertThat(message.getTimestamp(), equalTo(TIMESTAMP));
        assertThat(message.getNodeName(), equalTo(NODE_NAME));
    }

    public void testNewError() {
        TestAuditMessage message = new TestAuditMessage(RESOURCE_ID, MESSAGE, Level.ERROR, TIMESTAMP, NODE_NAME);
        assertThat(message.getResourceId(), equalTo(RESOURCE_ID));
        assertThat(message.getMessage(), equalTo(MESSAGE));
        assertThat(message.getLevel(), equalTo(Level.ERROR));
        assertThat(message.getTimestamp(), equalTo(TIMESTAMP));
        assertThat(message.getNodeName(), equalTo(NODE_NAME));
    }

    @Override
    protected TestAuditMessage doParseInstance(XContentParser parser) {
        return TestAuditMessage.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected TestAuditMessage createTestInstance() {
        return new TestAuditMessage(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomAlphaOfLengthBetween(1, 20),
            randomFrom(Level.values()),
            new Date(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)
        );
    }
}
