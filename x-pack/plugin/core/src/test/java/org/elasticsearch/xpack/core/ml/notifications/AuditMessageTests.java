/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.util.Date;

public class AuditMessageTests extends AbstractSerializingTestCase<AuditMessage> {
    private long startMillis;

    @Before
    public void setStartTime() {
        startMillis = System.currentTimeMillis();
    }

    public void testNewInfo() {
        AuditMessage info = AuditMessage.newInfo("foo", "some info", "some_node");
        assertEquals("foo", info.getJobId());
        assertEquals("some info", info.getMessage());
        assertEquals(Level.INFO, info.getLevel());
        assertDateBetweenStartAndNow(info.getTimestamp());
    }

    public void testNewWarning() {
        AuditMessage warning = AuditMessage.newWarning("bar", "some warning", "some_node");
        assertEquals("bar", warning.getJobId());
        assertEquals("some warning", warning.getMessage());
        assertEquals(Level.WARNING, warning.getLevel());
        assertDateBetweenStartAndNow(warning.getTimestamp());
    }


    public void testNewError() {
        AuditMessage error = AuditMessage.newError("foo", "some error", "some_node");
        assertEquals("foo", error.getJobId());
        assertEquals("some error", error.getMessage());
        assertEquals(Level.ERROR, error.getLevel());
        assertDateBetweenStartAndNow(error.getTimestamp());
    }

    public void testNewActivity() {
        AuditMessage error = AuditMessage.newActivity("foo", "some error", "some_node");
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
    protected AuditMessage doParseInstance(XContentParser parser) {
        return AuditMessage.PARSER.apply(parser, null);
    }

    @Override
    protected AuditMessage createTestInstance() {
        return new AuditMessage(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 200),
                randomFrom(Level.values()), randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected Reader<AuditMessage> instanceReader() {
        return AuditMessage::new;
    }
}
