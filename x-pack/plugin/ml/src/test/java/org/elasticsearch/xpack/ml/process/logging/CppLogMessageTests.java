/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process.logging;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;

public class CppLogMessageTests extends AbstractSerializingTestCase<CppLogMessage> {

    public void testDefaultConstructor() {
        CppLogMessage msg = new CppLogMessage(Instant.ofEpochSecond(1494422876L));
        assertEquals("", msg.getLogger());
        assertEquals(Instant.ofEpochSecond(1494422876L), msg.getTimestamp());
        assertEquals("", msg.getLevel());
        assertEquals(0, msg.getPid());
        assertEquals("", msg.getThread());
        assertEquals("", msg.getMessage());
        assertEquals("", msg.getClazz());
        assertEquals("", msg.getMethod());
        assertEquals("", msg.getFile());
        assertEquals(0, msg.getLine());
    }

    public void testParseWithMissingTimestamp() throws IOException {
        XContent xContent = XContentFactory.xContent(XContentType.JSON);
        Instant before = Instant.ofEpochMilli(Instant.now().toEpochMilli());

        String input = """
            {
              "logger": "controller",
              "level": "INFO",
              "pid": 42,
              "thread": "0x7fff7d2a8000",
              "message": "message 1",
              "class": "ml",
              "method": "core::SomeNoiseMaker",
              "file": "Noisemaker.cc",
              "line": 333
            }""";
        XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, input);
        CppLogMessage msg = CppLogMessage.PARSER.apply(parser, null);

        Instant after = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        assertTrue(before.isBefore(msg.getTimestamp()) || before.equals(msg.getTimestamp()));
        assertTrue(after.isAfter(msg.getTimestamp()) || after.equals(msg.getTimestamp()));
    }

    @Override
    protected CppLogMessage createTestInstance() {
        CppLogMessage msg = new CppLogMessage(Instant.ofEpochSecond(1494422876L));
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
    protected CppLogMessage doParseInstance(XContentParser parser) {
        return CppLogMessage.PARSER.apply(parser, null);
    }
}
