/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.logging;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

public class CppLogMessageTests extends AbstractSerializingTestCase<CppLogMessage> {

    public void testDefaultConstructor() {
        CppLogMessage msg = new CppLogMessage();
        assertEquals("", msg.getLogger());
        assertTrue(msg.getTimestamp().toString(), msg.getTimestamp().getTime() > 0);
        assertEquals("", msg.getLevel());
        assertEquals(0, msg.getPid());
        assertEquals("", msg.getThread());
        assertEquals("", msg.getMessage());
        assertEquals("", msg.getClazz());
        assertEquals("", msg.getMethod());
        assertEquals("", msg.getFile());
        assertEquals(0, msg.getLine());
    }

    @Override
    protected CppLogMessage createTestInstance() {
        CppLogMessage msg = new CppLogMessage();
        msg.setLogger("autodetect");
        msg.setLevel("INFO");
        msg.setPid(12345);
        msg.setThread("0x123456789");
        msg.setMessage("Very informative");
        msg.setClazz("CAnomalyDetector");
        msg.setMethod("detectAnomalies");
        msg.setFile("CAnomalyDetector.cc");
        msg.setLine(123);
        return msg;
    }

    @Override
    protected Reader<CppLogMessage> instanceReader() {
        return CppLogMessage::new;
    }

    @Override
    protected CppLogMessage parseInstance(XContentParser parser) {
        return CppLogMessage.PARSER.apply(parser, null);
    }
}