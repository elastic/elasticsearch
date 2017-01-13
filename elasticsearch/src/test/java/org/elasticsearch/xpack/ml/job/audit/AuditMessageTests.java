/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.audit;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;
import org.junit.Before;

import java.util.Date;

public class AuditMessageTests extends AbstractSerializingTestCase<AuditMessage> {
    private long startMillis;

    @Before
    public void setStartTime() {
        startMillis = System.currentTimeMillis();
    }

    public void testDefaultConstructor() {
        AuditMessage auditMessage = new AuditMessage();
        assertNull(auditMessage.getMessage());
        assertNull(auditMessage.getLevel());
        assertNull(auditMessage.getTimestamp());
    }

    public void testNewInfo() {
        AuditMessage info = AuditMessage.newInfo("foo", "some info");
        assertEquals("foo", info.getJobId());
        assertEquals("some info", info.getMessage());
        assertEquals(Level.INFO, info.getLevel());
        assertDateBetweenStartAndNow(info.getTimestamp());
    }

    public void testNewWarning() {
        AuditMessage warning = AuditMessage.newWarning("bar", "some warning");
        assertEquals("bar", warning.getJobId());
        assertEquals("some warning", warning.getMessage());
        assertEquals(Level.WARNING, warning.getLevel());
        assertDateBetweenStartAndNow(warning.getTimestamp());
    }


    public void testNewError() {
        AuditMessage error = AuditMessage.newError("foo", "some error");
        assertEquals("foo", error.getJobId());
        assertEquals("some error", error.getMessage());
        assertEquals(Level.ERROR, error.getLevel());
        assertDateBetweenStartAndNow(error.getTimestamp());
    }

    public void testNewActivity() {
        AuditMessage error = AuditMessage.newActivity("foo", "some error");
        assertEquals("foo", error.getJobId());
        assertEquals("some error", error.getMessage());
        assertEquals(Level.ACTIVITY, error.getLevel());
        assertDateBetweenStartAndNow(error.getTimestamp());
    }

    private void assertDateBetweenStartAndNow(Date timestamp) {
        long timestampMillis = timestamp.getTime();
        assertTrue(timestampMillis >= startMillis);
        assertTrue(timestampMillis <= System.currentTimeMillis());
    }

    @Override
    protected AuditMessage parseInstance(XContentParser parser) {
        return AuditMessage.PARSER.apply(parser, null);
    }

    @Override
    protected AuditMessage createTestInstance() {
        AuditMessage message = new AuditMessage();
        if (randomBoolean()) {
            message.setJobId(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            message.setMessage(randomAsciiOfLengthBetween(1, 200));
        }
        if (randomBoolean()) {
            message.setLevel(randomFrom(Level.values()));
        }
        if (randomBoolean()) {
            message.setTimestamp(new Date(TimeUtils.dateStringToEpoch(randomTimeValue())));
        }
        return message;
    }

    @Override
    protected Reader<AuditMessage> instanceReader() {
        return AuditMessage::new;
    }
}
