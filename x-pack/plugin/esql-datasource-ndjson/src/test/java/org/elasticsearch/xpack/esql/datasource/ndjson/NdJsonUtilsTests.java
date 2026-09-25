/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

/**
 * Unit tests for {@link NdJsonUtils#JSON_FACTORY}.
 *
 * <p>The shared factory carries non-default tuning that the streaming-parallel NDJSON path
 * relies on; pin those settings here so accidental drift (e.g. a refactor to the central
 * {@code ESJsonFactory}) is caught at build time rather than as a runtime regression.
 */
public class NdJsonUtilsTests extends ESTestCase {

    public void testFactoryDisablesAutoCloseSource() {
        assertFalse(
            "AUTO_CLOSE_SOURCE must be off so recovery from JsonParseException does not close a wrapping codec stream",
            NdJsonUtils.JSON_FACTORY.isEnabled(StreamReadFeature.AUTO_CLOSE_SOURCE)
        );
    }

    public void testFactoryEnablesFastDoubleParser() {
        assertTrue(
            "USE_FAST_DOUBLE_PARSER must be on for numeric-column throughput",
            NdJsonUtils.JSON_FACTORY.isEnabled(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
        );
    }

    public void testFactoryDisablesIncludeSourceInLocation() {
        assertFalse(
            "INCLUDE_SOURCE_IN_LOCATION must be off; we never echo the source payload back on parse errors",
            NdJsonUtils.JSON_FACTORY.isEnabled(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        );
    }

    public void testFactoryDisablesInternFieldNames() {
        assertFalse(
            "INTERN_FIELD_NAMES must be off so parallel parsers do not serialize on String.intern()'s monitor",
            NdJsonUtils.JSON_FACTORY.isEnabled(JsonFactory.Feature.INTERN_FIELD_NAMES)
        );
    }

    /**
     * {@link NdJsonPageDecoder}'s identity-keyed field-name cache only avoids the {@code
     * HashMap} probe when {@link com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer} returns
     * stable {@code String} instances per name across records. That stability is the default and
     * is driven by {@link JsonFactory.Feature#CANONICALIZE_FIELD_NAMES} being on; pin it here so
     * a future tuning change has to surface the consequence explicitly.
     */
    public void testFactoryEnablesCanonicalizeFieldNames() {
        assertTrue(
            "CANONICALIZE_FIELD_NAMES must be on; NdJsonPageDecoder's identity-keyed field-name cache depends on it",
            NdJsonUtils.JSON_FACTORY.isEnabled(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
        );
    }

    public void testFactoryEnablesStrictDuplicateDetection() {
        assertTrue(
            "STRICT_DUPLICATE_DETECTION must be on so a record naming one field twice is rejected rather than merged",
            NdJsonUtils.JSON_FACTORY.isEnabled(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        );
    }

    /**
     * Behavioural check for {@code STRICT_DUPLICATE_DETECTION = true}, and the pin for the message text that
     * {@code NdJsonPageDecoder}'s failure-kind label matches on. Jackson gives duplicate keys and syntax errors
     * the same exception type, so an upgrade that rewords this would silently relabel the failure "Malformed".
     */
    public void testParserRejectsRepeatedFieldNameInOneObject() throws IOException {
        try (JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser("{\"a.b\":1,\"a.b\":2}")) {
            JsonParseException e = expectThrows(JsonParseException.class, () -> {
                while (parser.nextToken() != null) {
                    // drain: the rejection lands on the repeated name, not at the end of the object
                }
            });
            assertThat(e.getOriginalMessage(), startsWith("Duplicate field"));
            assertThat(e.getOriginalMessage(), containsString("a.b"));
        }
    }

    /**
     * The scope of the rejection above: a repeat is per object instance, so the same name in two elements of an
     * array is not one. {@code NdJsonPageDecoder} merges those into a multivalue, so a false positive here would
     * drop ordinary records.
     */
    public void testParserAcceptsSameFieldNameInDifferentObjects() throws IOException {
        try (JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser("{\"a\":[{\"b\":1},{\"b\":2}]}")) {
            while (parser.nextToken() != null) {
                // drain: no rejection expected
            }
        }
    }

    // --- LineTerminatorTrackingStream direct unit tests ---

    /**
     * No bytes delivered yet: {@code pos = totalDelivered - releasedSize - 1 = -1 < 0}.
     * The guard must return {@code false} rather than reading at a negative index.
     */
    public void testTrackingStreamNoBytesDelivered() {
        NdJsonUtils.LineTerminatorTrackingStream tracker = new NdJsonUtils.LineTerminatorTrackingStream(
            new ByteArrayInputStream(new byte[0])
        );
        assertFalse("no bytes delivered: must return false", tracker.wasLastConsumedByteTerminator(0));
    }

    /**
     * Normal case: the last byte delivered is '\n'.
     * {@code releasedSize = 0} → {@code pos = totalDelivered - 1} → the '\n'.
     * {@code releasedSize = 1} → {@code pos = totalDelivered - 2} → the byte before '\n' (not a terminator).
     */
    public void testTrackingStreamLastByteIsNewline() throws IOException {
        byte[] data = "hello\n".getBytes(StandardCharsets.UTF_8);
        NdJsonUtils.LineTerminatorTrackingStream tracker = new NdJsonUtils.LineTerminatorTrackingStream(new ByteArrayInputStream(data));
        byte[] buf = new byte[data.length];
        int n = tracker.read(buf, 0, buf.length);
        assertEquals(data.length, n);
        assertTrue("last byte is '\\n': releasedSize=0 must return true", tracker.wasLastConsumedByteTerminator(0));
        assertFalse("byte before '\\n' is 'o': releasedSize=1 must return false", tracker.wasLastConsumedByteTerminator(1));
    }

    /**
     * Ring wrap-around: after exactly {@code RING_SIZE} bytes the ring is full; the
     * ({@code RING_SIZE + 1})-th byte wraps to index 0 in the ring. Verifies
     * {@code wasLastConsumedByteTerminator} reads from the wrapped position correctly.
     */
    public void testTrackingStreamRingWrapAround() throws IOException {
        // RING_SIZE is package-private via the class; we know it is 8193.
        int ringSize = 8193;
        byte[] data = new byte[ringSize + 1];
        Arrays.fill(data, (byte) 'a');
        data[ringSize] = '\n'; // the byte that wraps to ring[0]

        NdJsonUtils.LineTerminatorTrackingStream tracker = new NdJsonUtils.LineTerminatorTrackingStream(new ByteArrayInputStream(data));

        // Read first ringSize bytes (fills the ring without wrapping)
        byte[] buf = new byte[ringSize];
        int read = 0;
        while (read < ringSize) {
            int n = tracker.read(buf, read, ringSize - read);
            if (n < 0) break;
            read += n;
        }
        // totalDelivered == ringSize; last consumed byte is 'a' → false
        assertFalse("last byte is 'a' before wrap: must return false", tracker.wasLastConsumedByteTerminator(0));

        // Read the (ringSize+1)-th byte: '\n', stored at ring[0]
        byte[] one = new byte[1];
        assertEquals(1, tracker.read(one, 0, 1));
        // totalDelivered == ringSize + 1; pos = ringSize → ring[ringSize % ringSize] = ring[0] = '\n'
        assertTrue("last byte '\\n' at wrapped index 0: must return true", tracker.wasLastConsumedByteTerminator(0));
    }

    /**
     * The single-byte {@link NdJsonUtils.LineTerminatorTrackingStream#read()} override has independent
     * ring-write logic; verify it records the byte and the terminator check sees it.
     */
    public void testTrackingStreamSingleByteRead() throws IOException {
        NdJsonUtils.LineTerminatorTrackingStream tracker = new NdJsonUtils.LineTerminatorTrackingStream(
            new ByteArrayInputStream(new byte[] { '\n' })
        );
        assertEquals('\n', tracker.read());
        assertTrue("single-byte read of '\\n': must return true", tracker.wasLastConsumedByteTerminator(0));
    }

    /**
     * Out-of-range guard: {@code pos < totalDelivered - RING_SIZE}. This fires when
     * {@code releasedSize >= RING_SIZE}, a condition that cannot occur in practice (Jackson's
     * buffer is {@code < RING_SIZE}) but is defended against. Verifies the guard returns {@code false}.
     */
    public void testTrackingStreamOutOfRangeGuard() throws IOException {
        int ringSize = 8193;
        // Deliver ringSize + 1 bytes so totalDelivered > ringSize
        byte[] data = new byte[ringSize + 1];
        Arrays.fill(data, (byte) 'a');
        NdJsonUtils.LineTerminatorTrackingStream tracker = new NdJsonUtils.LineTerminatorTrackingStream(new ByteArrayInputStream(data));
        byte[] buf = new byte[data.length];
        int total = 0;
        int n;
        while ((n = tracker.read(buf, total, buf.length - total)) > 0) {
            total += n;
        }
        // pos = (ringSize+1) - ringSize - 1 = 0; totalDelivered - RING_SIZE = 1; 0 < 1 → guard fires
        assertFalse("pos falls outside ring: out-of-range guard must return false", tracker.wasLastConsumedByteTerminator(ringSize));
    }

    /**
     * {@link NdJsonUtils.LineTerminatorTrackingStream#reset} must clear {@code totalDelivered}
     * so the next parser starts fresh. After reset, the tracker behaves identically to a newly
     * constructed one wrapping the supplied stream.
     */
    public void testTrackingStreamReset() throws IOException {
        // Deliver one byte, then reset with a new stream containing '\n'.
        NdJsonUtils.LineTerminatorTrackingStream tracker = new NdJsonUtils.LineTerminatorTrackingStream(
            new ByteArrayInputStream(new byte[] { 'a' })
        );
        assertEquals('a', tracker.read());
        // Before reset: totalDelivered = 1, last byte is 'a' (not a terminator).
        assertFalse("last byte 'a': must return false before reset", tracker.wasLastConsumedByteTerminator(0));

        // Reset with a new stream.
        tracker.reset(new ByteArrayInputStream(new byte[] { '\n' }));
        // After reset: totalDelivered = 0, no bytes consumed yet.
        assertFalse("no bytes after reset: must return false", tracker.wasLastConsumedByteTerminator(0));

        assertEquals('\n', tracker.read());
        assertTrue("last byte '\\n' after reset: must return true", tracker.wasLastConsumedByteTerminator(0));
    }

    /**
     * {@link NdJsonUtils.LineTerminatorTrackingStream#skip} must route through {@link
     * NdJsonUtils.LineTerminatorTrackingStream#read} so the ring buffer is updated. Without the
     * override, {@link java.io.FilterInputStream#skip} delegates to {@code in.skip()}, bypassing
     * the ring entirely and leaving {@code wasLastConsumedByteTerminator} unable to see the skipped
     * bytes.
     */
    public void testTrackingStreamSkipUpdatesRing() throws IOException {
        // Stream: "hello\n" — skip all but the last byte.
        byte[] data = "hello\n".getBytes(StandardCharsets.UTF_8);
        NdJsonUtils.LineTerminatorTrackingStream tracker = new NdJsonUtils.LineTerminatorTrackingStream(new ByteArrayInputStream(data));
        long skipped = tracker.skip(data.length - 1); // skip "hello"
        assertEquals(data.length - 1, skipped);
        // Now read the '\n'.
        assertEquals('\n', tracker.read());
        assertTrue("last byte '\\n' after skip: must return true", tracker.wasLastConsumedByteTerminator(0));
    }

    /**
     * When a single bulk read returns more than {@code RING_SIZE} bytes, the ring must not
     * attempt to write all {@code n} bytes (which would overflow). Only the tail (last
     * {@code RING_SIZE} bytes) is stored.
     */
    public void testTrackingStreamBulkReadLargerThanRing() throws IOException {
        int ringSize = 8193;
        // Produce ringSize + 10 bytes: ringSize bytes of 'a', then 10 bytes of '\n'.
        byte[] data = new byte[ringSize + 10];
        Arrays.fill(data, 0, ringSize, (byte) 'a');
        Arrays.fill(data, ringSize, ringSize + 10, (byte) '\n');

        // Wrap with a stream that returns ALL bytes in one read call.
        NdJsonUtils.LineTerminatorTrackingStream tracker = new NdJsonUtils.LineTerminatorTrackingStream(new ByteArrayInputStream(data));
        byte[] buf = new byte[data.length];
        int n = tracker.read(buf, 0, buf.length);
        assertEquals(data.length, n);

        // Last byte delivered is '\n': releasedSize=0 → pos = totalDelivered-1 = last '\n'.
        assertTrue("last byte '\\n': must return true", tracker.wasLastConsumedByteTerminator(0));
        // 10 bytes back is still '\n'.
        assertTrue("9 bytes back is still '\\n': must return true", tracker.wasLastConsumedByteTerminator(9));
        // 11 bytes back lands in the 'a' region.
        assertFalse("11 bytes back is 'a': must return false", tracker.wasLastConsumedByteTerminator(10));
    }

    /**
     * Behavioural check for {@code AUTO_CLOSE_SOURCE = false}: closing the parser must not
     * close the underlying stream. Schema inference and parse-error recovery rely on this.
     */
    public void testParserCloseDoesNotCloseUnderlyingStream() throws IOException {
        AtomicBoolean closed = new AtomicBoolean(false);
        InputStream raw = new FilterInputStream(new ByteArrayInputStream("{\"a\":1}\n".getBytes(StandardCharsets.UTF_8))) {
            @Override
            public void close() throws IOException {
                closed.set(true);
                super.close();
            }
        };
        try (JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser(raw)) {
            parser.nextToken();
        }
        assertFalse("Closing the parser must not close the wrapping stream when AUTO_CLOSE_SOURCE is disabled", closed.get());
    }
}
