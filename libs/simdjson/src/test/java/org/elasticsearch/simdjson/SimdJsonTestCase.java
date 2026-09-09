/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.foreign.Platform;
import org.elasticsearch.simdjson.internal.SimdJsonNativeSupport;
import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Base class and shared helpers for simdjson tests.
 *
 * <p>Subclasses that exercise native code inherit platform-aware gating via {@link #nativeRequirement()}:
 * unsupported platforms (Windows x64, Darwin x64) skip; supported platforms fail if native is missing.
 */
public abstract class SimdJsonTestCase extends ESTestCase {

    /**
     * How much of the simdjson stack a test suite requires before running.
     */
    protected enum NativeRequirement {
        /** Native {@code libsimdjson} only (FFI / {@link org.elasticsearch.simdjson.internal.StructuralIndexer}). */
        LIBRARY,
        /** Full {@link SimdJsonSupport#isSupported()} (native library and vector API). */
        FULL_SUPPORT
    }

    @Before
    public void requireNativeSimdjson() {
        switch (nativeRequirement()) {
            case LIBRARY -> requireNativeLibrary();
            case FULL_SUPPORT -> requireSimdJsonSupported();
        }
    }

    /**
     * Override in FFI-only suites that do not need the incubating vector API.
     */
    protected NativeRequirement nativeRequirement() {
        return NativeRequirement.FULL_SUPPORT;
    }

    protected static void requireNativeLibrary() {
        if (SimdJsonNativeSupport.isNativeLibSupported() == false) {
            assumeTrue("Native simdjson not supported on [" + Platform.current() + "]", false);
            return;
        }
        assertNotNull("Native simdjson library must be available on [" + Platform.current() + "]", SimdJsonNativeSupport.library());
    }

    protected static void requireSimdJsonSupported() {
        requireNativeLibrary();
        assertTrue("simdjson must be supported on [" + Platform.current() + "]", SimdJsonSupport.isSupported());
    }

    protected SimdJsonParser newParser(int capacity) {
        return new SimdJsonParser(capacity);
    }

    // End-to-end walk: stage1 + prepareDocumentWindow + SimdJsonDirectWalker.
    protected List<String> walkJson(String json) {
        return walkJson(json, false);
    }

    protected List<String> walkJson(String json, boolean normalizeEmptyObject) {
        byte[] buffer = json.getBytes(UTF_8);
        int len = buffer.length;

        try (SimdJsonParser parser = new SimdJsonParser(len)) {
            FrozenFieldNameTable parent = new FrozenFieldNameTable();
            FrozenFieldNameTable.Child child = parent.makeChild();
            SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);

            RecordingHandler handler = new RecordingHandler(normalizeEmptyObject);
            parser.stage1(buffer, 0, len);
            parser.prepareDocumentWindow(0, len);
            walker.walkDocument(buffer, parser, handler);
            return handler.events;
        }
    }

    // ---- Recording handler (test-only JsonDocumentHandler) ----

    /**
     * {@link JsonDocumentHandler} that records all events as strings in a list.
     * Two variants of {@link #emptyObject}: the default emits a single
     * {@code emptyObject(name)} event; pass {@code normalizeEmptyObject=true}
     * to the constructor to emit {@code startObject/endObject} instead (useful
     * when comparing against Jackson, which always emits start/end).
     */
    protected static class RecordingHandler implements JsonDocumentHandler {
        public final List<String> events = new ArrayList<>();
        private final boolean normalizeEmptyObject;

        public RecordingHandler() {
            this(false);
        }

        public RecordingHandler(boolean normalizeEmptyObject) {
            this.normalizeEmptyObject = normalizeEmptyObject;
        }

        @Override
        public void startObject(String fieldName) {
            events.add("startObject(" + fieldName + ")");
        }

        @Override
        public void endObject() {
            events.add("endObject()");
        }

        @Override
        public void emptyObject(String fieldName) {
            if (normalizeEmptyObject) {
                events.add("startObject(" + fieldName + ")");
                events.add("endObject()");
            } else {
                events.add("emptyObject(" + fieldName + ")");
            }
        }

        @Override
        public void stringField(String fieldName, byte[] buf, int off, int len) {
            events.add("string(" + fieldName + "=" + new String(buf, off, len, UTF_8) + ")");
        }

        @Override
        public void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("long(" + fieldName + "=" + value + ",fitsInt=" + fitsInt + ")");
        }

        @Override
        public void bigIntegerField(String fieldName, BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("bigInteger(" + fieldName + "=" + value + ")");
        }

        @Override
        public void doubleField(String fieldName, double value, boolean fitsFloat, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("double(" + fieldName + "=" + value + ",fitsFloat=" + fitsFloat + ")");
        }

        @Override
        public void booleanField(String fieldName, boolean value, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("bool(" + fieldName + "=" + value + ")");
        }

        @Override
        public void nullField(String fieldName) {
            events.add("null(" + fieldName + ")");
        }

        @Override
        public void startArray(String fieldName) {
            events.add("startArray(" + fieldName + ")");
        }

        @Override
        public void endArray() {
            events.add("endArray()");
        }

        @Override
        public void arrayElemString(byte[] buf, int off, int len) {
            events.add("arrayElemString(" + new String(buf, off, len, UTF_8) + ")");
        }

        @Override
        public void arrayElemLong(long value, boolean fitsInt) {
            events.add("arrayElemLong(" + value + ",fitsInt=" + fitsInt + ")");
        }

        @Override
        public void arrayElemBigInteger(BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("arrayElemBigInteger(" + value + ")");
        }

        @Override
        public void arrayElemDouble(double value, boolean fitsFloat) {
            events.add("arrayElemDouble(" + value + ",fitsFloat=" + fitsFloat + ")");
        }

        @Override
        public void arrayElemBoolean(boolean value) {
            events.add("arrayElemBoolean(" + value + ")");
        }

        @Override
        public void arrayElemNull() {
            events.add("arrayElemNull()");
        }

        @Override
        public void arrayElemStartObject() {
            events.add("arrayElemStartObject()");
        }

        @Override
        public void arrayElemEndObject() {
            events.add("arrayElemEndObject()");
        }

        @Override
        public void arrayElemStartArray() {
            events.add("arrayElemStartArray()");
        }

        @Override
        public void arrayElemEndArray() {
            events.add("arrayElemEndArray()");
        }
    }

    // ---- Buffer helpers ----

    /** Converts a string to a UTF-8 byte array. */
    public static byte[] toBytes(String s) {
        return s.getBytes(UTF_8);
    }

    /** Creates a byte array with the content placed at {@code offset}. */
    public static byte[] toBytesAtOffset(String content, int offset) {
        byte[] raw = content.getBytes(UTF_8);
        byte[] buf = new byte[offset + raw.length];
        System.arraycopy(raw, 0, buf, offset, raw.length);
        return buf;
    }

    /** Wraps a string in JSON quotes: {@code "content"}. */
    public static byte[] makeJsonString(String content) {
        return ("\"" + content + "\"").getBytes(UTF_8);
    }

    // ---- Batch buffer helpers (multi-document NDJSON batches) ----

    /** Concatenates JSON docs into a single buffer. */
    protected static byte[] buildBatchBuffer(String... jsonDocs) {
        List<byte[]> docBytes = new ArrayList<>();
        int total = 0;
        for (String doc : jsonDocs) {
            byte[] b = doc.getBytes(UTF_8);
            docBytes.add(b);
            total += b.length;
        }
        byte[] buffer = new byte[total];
        int pos = 0;
        for (byte[] b : docBytes) {
            System.arraycopy(b, 0, buffer, pos, b.length);
            pos += b.length;
        }
        return buffer;
    }

    /** Returns the byte offset of each doc within a concatenated batch. */
    protected static int[] computeOffsets(String... jsonDocs) {
        int[] offsets = new int[jsonDocs.length];
        int pos = 0;
        for (int i = 0; i < jsonDocs.length; i++) {
            offsets[i] = pos;
            pos += jsonDocs[i].getBytes(UTF_8).length;
        }
        return offsets;
    }

    /** Returns the byte length of each doc. */
    protected static int[] computeLengths(String... jsonDocs) {
        int[] lengths = new int[jsonDocs.length];
        for (int i = 0; i < jsonDocs.length; i++) {
            lengths[i] = jsonDocs[i].getBytes(UTF_8).length;
        }
        return lengths;
    }

    /** Sums an array of lengths. */
    protected static int totalLen(int[] lengths) {
        int total = 0;
        for (int len : lengths) {
            total += len;
        }
        return total;
    }

    // ---- Random JSON generation ----

    private static final java.util.Random SHARED_RANDOM = new java.util.Random();

    /** Generates a random JSON object with up to {@code maxFields} fields and {@code maxDepth} nesting. */
    public static String generateRandomJsonObject(int maxFields, int maxDepth) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        int fieldCount = 1 + SHARED_RANDOM.nextInt(maxFields);
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append("field").append(i).append('"').append(':');
            appendRandomValue(sb, maxFields, maxDepth, 0);
        }
        sb.append('}');
        return sb.toString();
    }

    private static void appendRandomValue(StringBuilder sb, int maxFields, int maxDepth, int depth) {
        int type = SHARED_RANDOM.nextInt(depth < maxDepth ? 7 : 5);
        switch (type) {
            case 0 -> sb.append('"').append("str").append(SHARED_RANDOM.nextInt(1000)).append('"');
            case 1 -> sb.append(SHARED_RANDOM.nextInt(200000) - 100000);
            case 2 -> sb.append(SHARED_RANDOM.nextDouble() * 1000);
            case 3 -> sb.append(SHARED_RANDOM.nextBoolean());
            case 4 -> sb.append("null");
            case 5 -> {
                sb.append('{');
                int fc = 1 + SHARED_RANDOM.nextInt(Math.max(1, maxFields - 1));
                for (int i = 0; i < fc; i++) {
                    if (i > 0) sb.append(',');
                    sb.append('"').append("f").append(depth).append('_').append(i).append('"').append(':');
                    appendRandomValue(sb, maxFields, maxDepth, depth + 1);
                }
                sb.append('}');
            }
            case 6 -> {
                sb.append('[');
                int ac = SHARED_RANDOM.nextInt(5);
                for (int i = 0; i < ac; i++) {
                    if (i > 0) sb.append(',');
                    appendRandomValue(sb, maxFields, maxDepth, depth + 1);
                }
                sb.append(']');
            }
        }
    }
}
