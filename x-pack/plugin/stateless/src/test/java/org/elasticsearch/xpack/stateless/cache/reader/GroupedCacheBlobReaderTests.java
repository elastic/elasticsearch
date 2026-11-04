/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GroupedCacheBlobReaderTests extends ESTestCase {

    /**
     * A reader over an in-memory blob, which records the object store reads it is asked for
     */
    private static class RecordingReader implements CacheBlobReader {
        private final byte[] blob;
        final List<long[]> reads = new ArrayList<>();

        RecordingReader(byte[] blob) {
            this.blob = blob;
        }

        @Override
        public ByteRange getRange(long position, int length, long remainingFileLength) {
            return ByteRange.of(position, position + length);
        }

        @Override
        public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
            reads.add(new long[] { position, length });
            final int start = (int) Math.min(position, blob.length);
            final int end = (int) Math.min(position + length, blob.length);
            listener.onResponse(new ByteArrayInputStream(blob, start, end - start));
        }

        @Override
        public boolean readsFromObjectStore() {
            return true;
        }
    }

    private static byte[] blob(int length) {
        final byte[] blob = new byte[length];
        for (int i = 0; i < length; i++) {
            blob[i] = (byte) i;
        }
        return blob;
    }

    private static byte[] read(CacheBlobReader reader, long position, int length) throws IOException {
        final var future = new PlainActionFuture<InputStream>();
        reader.getRangeInputStream(position, length, future);
        try (var in = future.actionGet()) {
            return in.readAllBytes();
        }
    }

    public void testConsecutiveRegionsShareOneRead() throws IOException {
        final var blob = blob(1000);
        final var delegate = new RecordingReader(blob);
        try (var grouped = new GroupedCacheBlobReader(delegate, 1000)) {
            assertThat(read(grouped, 0, 250), equalTo(Arrays.copyOfRange(blob, 0, 250)));
            assertThat(read(grouped, 250, 250), equalTo(Arrays.copyOfRange(blob, 250, 500)));
            assertThat(read(grouped, 500, 500), equalTo(Arrays.copyOfRange(blob, 500, 1000)));
        }
        assertThat(delegate.reads, hasSize(1));
        assertThat(delegate.reads.get(0), equalTo(new long[] { 0, 1000 }));
    }

    public void testRegionCachedAfterPlanningIsSkippedWithinTheSameRead() throws IOException {
        final var blob = blob(1000);
        final var delegate = new RecordingReader(blob);
        try (var grouped = new GroupedCacheBlobReader(delegate, 1000)) {
            assertThat(read(grouped, 0, 250), equalTo(Arrays.copyOfRange(blob, 0, 250)));
            assertThat(read(grouped, 500, 250), equalTo(Arrays.copyOfRange(blob, 500, 750)));
        }
        assertThat(delegate.reads, hasSize(1));
    }

    public void testPositionBeforeTheStreamOpensANewRead() throws IOException {
        final var blob = blob(1000);
        final var delegate = new RecordingReader(blob);
        try (var grouped = new GroupedCacheBlobReader(delegate, 1000)) {
            assertThat(read(grouped, 500, 250), equalTo(Arrays.copyOfRange(blob, 500, 750)));
            assertThat(read(grouped, 100, 100), equalTo(Arrays.copyOfRange(blob, 100, 200)));
        }
        assertThat(delegate.reads.stream().map(r -> r[0]).toList(), contains(500L, 100L));
    }

    public void testPartiallyConsumedStreamIsSkippedToTheNextRequest() throws IOException {
        final var blob = blob(1000);
        final var delegate = new RecordingReader(blob);
        try (var grouped = new GroupedCacheBlobReader(delegate, 1000)) {
            final var future = new PlainActionFuture<InputStream>();
            grouped.getRangeInputStream(0, 250, future);
            try (var in = future.actionGet()) {
                assertThat(in.readNBytes(10), equalTo(Arrays.copyOfRange(blob, 0, 10)));
            }
            // the first 10 bytes were consumed, the next 90 are discarded to reach position 100
            assertThat(read(grouped, 100, 100), equalTo(Arrays.copyOfRange(blob, 100, 200)));
        }
        assertThat(delegate.reads, hasSize(1));
    }

    public void testReadIsLimitedToTheRequestedLength() throws IOException {
        final var blob = blob(1000);
        final var delegate = new RecordingReader(blob);
        try (var grouped = new GroupedCacheBlobReader(delegate, 1000)) {
            assertThat(read(grouped, 0, 10).length, equalTo(10));
        }
    }

    public void testBlobShorterThanTheGroup() throws IOException {
        final var blob = blob(300);
        final var delegate = new RecordingReader(blob);
        try (var grouped = new GroupedCacheBlobReader(delegate, 1000)) {
            assertThat(read(grouped, 0, 250), equalTo(Arrays.copyOfRange(blob, 0, 250)));
            assertThat(read(grouped, 750, 250).length, equalTo(0));
        }
    }
}
