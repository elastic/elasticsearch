/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class HeaderPrefixProbeTests extends ESTestCase {

    private static final byte[] FILE = "abcdefghij".getBytes(StandardCharsets.UTF_8);

    /** A reader that stops inside the prefix has an answer that the prefix did not cut short. */
    public void testReadStoppingInsideThePrefixDoesNotReachItsEnd() throws Exception {
        HeaderPrefixProbe probe = new HeaderPrefixProbe(new Bytes(FILE), 0, 8);
        try (InputStream in = probe.newStream()) {
            assertEquals('a', in.read());
            assertEquals(3, in.read(new byte[3], 0, 3));
        }
        assertFalse(probe.reachedEnd());
    }

    /** A reader that runs off the prefix's end may have been cut mid-record, so the caller must not trust its answer. */
    public void testReadingToThePrefixEndIsRecorded() throws Exception {
        HeaderPrefixProbe probe = new HeaderPrefixProbe(new Bytes(FILE), 2, 4);
        try (InputStream in = probe.newStream()) {
            assertEquals("cdef", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertTrue(probe.reachedEnd());
    }

    /** The single-byte read reports the end too. */
    public void testSingleByteReadToThePrefixEndIsRecorded() throws Exception {
        HeaderPrefixProbe probe = new HeaderPrefixProbe(new Bytes(FILE), 0, 1);
        try (InputStream in = probe.newStream()) {
            assertEquals('a', in.read());
            assertFalse(probe.reachedEnd());
            assertEquals(-1, in.read());
        }
        assertTrue(probe.reachedEnd());
    }

    /** The delegate is handed the stream it opened, not the probe's wrapper, so a provider can abort it without draining. */
    public void testAbortReachesTheDelegateWithItsOwnStream() throws Exception {
        Bytes delegate = new Bytes(FILE);
        HeaderPrefixProbe probe = new HeaderPrefixProbe(delegate, 0, 8);
        InputStream in = probe.newStream();
        probe.abortStream(in);
        assertEquals(1, delegate.opened.size());
        assertEquals(1, delegate.aborted.size());
        assertSame(delegate.opened.get(0), delegate.aborted.get(0));
    }

    private static final class Bytes extends AbstractTestStorageObject {
        private final byte[] data;
        final List<InputStream> opened = new ArrayList<>();
        final List<InputStream> aborted = new ArrayList<>();

        Bytes(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            return remember(new ByteArrayInputStream(data));
        }

        @Override
        public InputStream newStream(long position, long length) {
            return remember(new ByteArrayInputStream(data, (int) position, (int) length));
        }

        private InputStream remember(InputStream stream) {
            opened.add(stream);
            return stream;
        }

        @Override
        public void abortStream(InputStream stream) {
            aborted.add(stream);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("s3://bucket/probe.csv");
        }
    }
}
