/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;

public class SimdJsonParserPoolTests extends SimdJsonTestCase {

    private static final int MAX_DOC_BYTES = 4096;

    private SimdJsonParserPool newPool() {
        return new SimdJsonParserPool(MAX_DOC_BYTES);
    }

    public void testParseDocumentMatchesDirectWalk() {
        String json = "{\"a\":1,\"b\":\"x\"}";
        byte[] buffer = json.getBytes(UTF_8);

        RecordingHandler handler = new RecordingHandler(false);
        newPool().forCurrentThread().parseDocument(buffer, buffer.length, handler);

        assertEquals(walkJson(json), handler.events);
    }

    public void testParseDocumentWithNonZeroOffset() {
        String json = "{\"k\":42}";
        byte[] buffer = ("  " + json + "  ").getBytes(UTF_8);

        RecordingHandler handler = new RecordingHandler(false);
        newPool().forCurrentThread().parseDocument(buffer, 2, json.length(), handler);

        assertEquals(walkJson(json), handler.events);
    }

    public void testParseDocumentRejectsOversizedDocument() {
        JsonDocumentParser docParser = newPool().forCurrentThread();
        int tooLarge = docParser.maxDocumentBytes() + 1;
        byte[] buffer = new byte[tooLarge];
        buffer[0] = '{';
        buffer[tooLarge - 1] = '}';

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> docParser.parseDocument(buffer, tooLarge, new RecordingHandler(false))
        );
        assertThat(e.getMessage(), containsString("exceeds maximum"));
    }

    /** A thread always gets the same parser, so its structural index buffer and name cache stay warm. */
    public void testSameParserForSameThread() {
        SimdJsonParserPool pool = newPool();
        assertSame("a thread must reuse its own parser", pool.forCurrentThread(), pool.forCurrentThread());
    }

    /** Different threads get different parsers, since a parser is not thread-safe. */
    public void testDistinctParserPerThread() throws Exception {
        SimdJsonParserPool pool = newPool();
        JsonDocumentParser onTestThread = pool.forCurrentThread();

        AtomicReference<JsonDocumentParser> onOtherThread = new AtomicReference<>();
        Thread other = new Thread(() -> onOtherThread.set(pool.forCurrentThread()));
        other.start();
        other.join();

        assertNotSame("threads must not share a parser", onTestThread, onOtherThread.get());
    }

    /**
     * Field names learned on one thread reach a parser created later on another thread through the
     * pool's shared table: the second thread gets the identical String instance for the same name.
     */
    public void testFieldNamesAreSharedAcrossThreads() throws Exception {
        SimdJsonParserPool pool = newPool();
        byte[] buffer = "{\"shared_field_name\":1}".getBytes(UTF_8);

        // Parsing one document freezes this thread's name cache and publishes it to the shared table.
        NameCapturingHandler first = new NameCapturingHandler();
        JsonDocumentParser docParser = pool.forCurrentThread();
        docParser.parseDocument(buffer, buffer.length, first);
        docParser.publishFieldNames();

        NameCapturingHandler second = new NameCapturingHandler();
        Thread other = new Thread(() -> pool.forCurrentThread().parseDocument(buffer, buffer.length, second));
        other.start();
        other.join();

        assertEquals("shared_field_name", first.name);
        assertSame("field name must come from the pool's shared table", first.name, second.name);
    }

    /** Records the field name instance handed to the handler, to check name canonicalization. */
    private static class NameCapturingHandler extends RecordingHandler {
        volatile String name;

        @Override
        public void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen) {
            super.longField(fieldName, value, fitsInt, srcBuf, srcOff, srcLen);
            name = fieldName;
        }
    }
}
