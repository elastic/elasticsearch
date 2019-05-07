/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.ml.utils.time.TimeUtils;
import org.junit.Before;

import java.util.Date;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class AbstractAuditMessageTests extends AbstractXContentTestCase<AbstractAuditMessageTests.TestAuditMessage> {
    private long startMillis;

    static class TestAuditMessage extends AbstractAuditMessage {
        private static final ParseField ID = new ParseField("test_id");
        public static final ConstructingObjectParser<TestAuditMessage, Void> PARSER = new ConstructingObjectParser<>(
            AbstractAuditMessage.TYPE.getPreferredName(),
            true,
            a -> new TestAuditMessage((String)a[0], (String)a[1], (Level)a[2], (Date)a[3], (String)a[4]));

        static {
            PARSER.declareString(optionalConstructorArg(), ID);
            PARSER.declareString(constructorArg(), MESSAGE);
            PARSER.declareField(constructorArg(), p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return Level.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, LEVEL, ObjectParser.ValueType.STRING);
            PARSER.declareField(constructorArg(), parser -> {
                if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return new Date(parser.longValue());
                } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return new Date(TimeUtils.dateStringToEpoch(parser.text()));
                }
                throw new IllegalArgumentException(
                    "unexpected token [" + parser.currentToken() + "] for [" + TIMESTAMP.getPreferredName() + "]");
            }, TIMESTAMP, ObjectParser.ValueType.VALUE);
            PARSER.declareString(optionalConstructorArg(), NODE_NAME);
        }

        TestAuditMessage(String resourceId, String message, Level level, String nodeName) {
            super(resourceId, message, level, nodeName);
        }

        TestAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
            super(resourceId, message, level, timestamp, nodeName);
        }

        @Override
        protected String getResourceField() {
            return "test_id";
        }

        static AbstractAuditMessage.AbstractBuilder<TestAuditMessage> newBuilder() {
            return new AbstractBuilder<TestAuditMessage>() {
                @Override
                protected TestAuditMessage newMessage(Level level, String resourceId, String message, String nodeName) {
                    return new TestAuditMessage(resourceId, message, level, nodeName);
                }
            };
        }
    }

    @Before
    public void setStartTime() {
        startMillis = System.currentTimeMillis();
    }

    public void testNewInfo() {
        TestAuditMessage info = TestAuditMessage.newBuilder().info("foo", "some info", "some_node");
        assertEquals("foo", info.getResourceId());
        assertEquals("some info", info.getMessage());
        assertEquals(Level.INFO, info.getLevel());
        assertDateBetweenStartAndNow(info.getTimestamp());
    }

    public void testNewWarning() {
        TestAuditMessage warning = TestAuditMessage.newBuilder().warning("bar", "some warning", "some_node");
        assertEquals("bar", warning.getResourceId());
        assertEquals("some warning", warning.getMessage());
        assertEquals(Level.WARNING, warning.getLevel());
        assertDateBetweenStartAndNow(warning.getTimestamp());
    }


    public void testNewError() {
        TestAuditMessage error = TestAuditMessage.newBuilder().error("foo", "some error", "some_node");
        assertEquals("foo", error.getResourceId());
        assertEquals("some error", error.getMessage());
        assertEquals(Level.ERROR, error.getLevel());
        assertDateBetweenStartAndNow(error.getTimestamp());
    }

    private void assertDateBetweenStartAndNow(Date timestamp) {
        long timestampMillis = timestamp.getTime();
        assertTrue(timestampMillis >= startMillis);
        assertTrue(timestampMillis <= System.currentTimeMillis());
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
        return new TestAuditMessage(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 200),
                randomFrom(Level.values()), randomAlphaOfLengthBetween(1, 20));
    }
}
