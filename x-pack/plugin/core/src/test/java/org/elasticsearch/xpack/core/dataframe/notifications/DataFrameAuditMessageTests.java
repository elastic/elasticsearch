/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.notifications;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.junit.Before;

import java.util.Date;

public class DataFrameAuditMessageTests extends AbstractSerializingTestCase<DataFrameAuditMessage> {
    private long startMillis;

    @Before
    public void setStartTime() {
        startMillis = System.currentTimeMillis();
    }

    public void testNewInfo() {
        DataFrameAuditMessage info = DataFrameAuditMessage.messageBuilder().info("foo", "some info", "some_node");
        assertEquals("foo", info.getResourceId());
        assertEquals("some info", info.getMessage());
        assertEquals(Level.INFO, info.getLevel());
        assertDateBetweenStartAndNow(info.getTimestamp());
    }

    public void testNewWarning() {
        DataFrameAuditMessage warning = DataFrameAuditMessage.messageBuilder().warning("bar", "some warning", "some_node");
        assertEquals("bar", warning.getResourceId());
        assertEquals("some warning", warning.getMessage());
        assertEquals(Level.WARNING, warning.getLevel());
        assertDateBetweenStartAndNow(warning.getTimestamp());
    }


    public void testNewError() {
        DataFrameAuditMessage error = DataFrameAuditMessage.messageBuilder().error("foo", "some error", "some_node");
        assertEquals("foo", error.getResourceId());
        assertEquals("some error", error.getMessage());
        assertEquals(Level.ERROR, error.getLevel());
        assertDateBetweenStartAndNow(error.getTimestamp());
    }

    public void testNewActivity() {
        DataFrameAuditMessage error = DataFrameAuditMessage.messageBuilder().activity("foo", "some error", "some_node");
        assertEquals("foo", error.getResourceId());
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
    protected DataFrameAuditMessage doParseInstance(XContentParser parser) {
        return DataFrameAuditMessage.PARSER.apply(parser, null);
    }

    @Override
    protected DataFrameAuditMessage createTestInstance() {
        DataFrameAuditMessage auditMessage = new DataFrameAuditMessage();
        auditMessage.setLevel(randomFrom(Level.values()));
        auditMessage.setMessage(randomAlphaOfLengthBetween(1, 20));
        auditMessage.setNodeName(randomAlphaOfLengthBetween(1, 20));
        auditMessage.setResourceId(randomAlphaOfLengthBetween(1, 20));
        return auditMessage;
    }

    @Override
    protected Reader<DataFrameAuditMessage> instanceReader() {
        return DataFrameAuditMessage::new;
    }
}
