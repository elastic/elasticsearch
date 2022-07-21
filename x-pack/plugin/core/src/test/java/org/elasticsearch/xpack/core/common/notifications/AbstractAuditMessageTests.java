/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Date;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AbstractAuditMessageTests extends AbstractXContentTestCase<AbstractAuditMessageTests.TestAuditMessage> {

    static class TestAuditMessage extends AbstractAuditMessage {

        private static final ParseField TEST_ID = new ParseField("test_id");
        public static final ConstructingObjectParser<TestAuditMessage, Void> PARSER = createParser(
            "test_audit_message",
            TestAuditMessage::new,
            TEST_ID
        );

        TestAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
            super(resourceId, message, level, timestamp, nodeName);
        }

        @Override
        public String getJobType() {
            return "test_type";
        }

        @Override
        protected Optional<String> getResourceField() {
            return Optional.of(TEST_ID.getPreferredName());
        }
    }

    private static final String RESOURCE_ID = "foo";
    private static final String MESSAGE = "some message";
    private static final Date TIMESTAMP = new Date(123456789);
    private static final String NODE_NAME = "some_node";

    public void testGetResourceField() {
        TestAuditMessage message = new TestAuditMessage(RESOURCE_ID, MESSAGE, Level.INFO, TIMESTAMP, NODE_NAME);
        assertThat(message.getResourceField().get(), equalTo(TestAuditMessage.TEST_ID.getPreferredName()));
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

    public void testLongMessageIsTruncated() throws IOException {
        AbstractAuditMessage longMessage = new AbstractAuditMessage(
            randomBoolean() ? null : randomAlphaOfLength(10),
            "thisis17charslong".repeat(490),
            randomFrom(Level.values()),
            new Date(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)
        ) {
            @Override
            public String getJobType() {
                return "unused";
            }

            @Override
            protected Optional<String> getResourceField() {
                return Optional.of("unused");
            }
        };

        assertThat(longMessage.getMessage().length(), greaterThan(AbstractAuditMessage.MAX_AUDIT_MESSAGE_CHARS));

        // serialise the message and check the new message is truncated
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalXContent = XContentHelper.toXContent(longMessage, xContentType, randomBoolean());
        XContentParser parser = createParser(XContentFactory.xContent(xContentType), originalXContent);
        AbstractAuditMessage parsed = doParseInstance(parser);
        assertThat(parsed.getMessage().length(), equalTo(AbstractAuditMessage.MAX_AUDIT_MESSAGE_CHARS));
    }

    public void testTruncateString() {
        String message = "a short message short message short message short message short message";
        String truncated = AbstractAuditMessage.truncateMessage(message, 20);
        assertEquals("a ... (truncated)", truncated);
        assertThat(truncated.length(), lessThanOrEqualTo(20));

        truncated = AbstractAuditMessage.truncateMessage(message, 23);
        assertEquals("a short ... (truncated)", truncated);
        assertThat(truncated.length(), lessThanOrEqualTo(23));

        truncated = AbstractAuditMessage.truncateMessage(message, 31);
        assertEquals("a short message ... (truncated)", truncated);
        assertThat(truncated.length(), lessThanOrEqualTo(31));

        truncated = AbstractAuditMessage.truncateMessage(message, 32);
        assertEquals("a short message ... (truncated)", truncated);
        assertThat(truncated.length(), lessThanOrEqualTo(32));
    }

    public void testTruncateString_noSpaceChar() {
        String message = "ashortmessageshortmessageshortmessageshortmessageshortmessage";
        String truncated = AbstractAuditMessage.truncateMessage(message, 20);
        assertEquals("ashor... (truncated)", truncated);
        assertEquals(20, truncated.length());
        truncated = AbstractAuditMessage.truncateMessage(message, 25);
        assertEquals("ashortmess... (truncated)", truncated);
        assertEquals(25, truncated.length());
    }

    public void testTruncateString_tabsInsteadOfSpaces() {
        String truncated = AbstractAuditMessage.truncateMessage("a\tshort\tmessage\tshort\tmessage", 25);
        assertEquals("a\tshort\tme... (truncated)", truncated);
        assertEquals(25, truncated.length());
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
