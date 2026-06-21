/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PrefetchingCentroidIteratorTests extends ESTestCase {

    /**
     * With a budget large enough to cover every posting list, all posting lists are prefetched up
     * front (in a single pass) and then returned, in order, exactly once each.
     */
    public void testReturnsAllPostingsInOrderAndPrefetchesEachOnce() throws IOException {
        List<PostingMetadata> postings = postings(8, 100);
        try (Directory dir = new ByteBuffersDirectory(); RecordingIndexInput recording = openRecordingInput(dir)) {
            // budget covers every posting list with room to spare
            long budget = 100L * postings.size() + 1;
            PrefetchingCentroidIterator it = new PrefetchingCentroidIterator(listIterator(postings), recording, budget);

            // everything was prefetched while constructing the iterator
            assertEquals(postings.size(), recording.prefetches.size());

            List<PostingMetadata> returned = drain(it);
            assertEquals(postings, returned);
            assertPrefetchesMatch(postings, recording.prefetches);
        }
    }

    /**
     * A non-positive budget still prefetches exactly one posting list ahead of the consumer,
     * preserving the original prefetch-by-one behaviour.
     */
    public void testZeroBudgetPrefetchesOneAhead() throws IOException {
        List<PostingMetadata> postings = postings(5, 100);
        try (Directory dir = new ByteBuffersDirectory(); RecordingIndexInput recording = openRecordingInput(dir)) {
            PrefetchingCentroidIterator it = new PrefetchingCentroidIterator(listIterator(postings), recording, 0);

            // only the first posting list is prefetched before any is consumed
            assertEquals(1, recording.prefetches.size());

            int consumed = 0;
            while (it.hasNext()) {
                it.nextPosting();
                consumed++;
                // we stay exactly one posting list ahead until the delegate is exhausted
                assertEquals(Math.min(consumed + 1, postings.size()), recording.prefetches.size());
            }
            assertEquals(postings.size(), consumed);
        }
    }

    /**
     * The prefetch depth adapts to the byte budget: the iterator keeps prefetching ahead until the
     * in-flight posting list bytes reach the budget, then maintains that window as it advances.
     */
    public void testPrefetchDepthAdaptsToByteBudget() throws IOException {
        int length = 100;
        List<PostingMetadata> postings = postings(10, length);
        try (Directory dir = new ByteBuffersDirectory(); RecordingIndexInput recording = openRecordingInput(dir)) {
            // budget spans between 2 and 3 posting lists, so we prefetch 3 ahead (smallest window >= budget)
            long budget = 250;
            PrefetchingCentroidIterator it = new PrefetchingCentroidIterator(listIterator(postings), recording, budget);

            assertEquals(3, recording.prefetches.size());

            // consuming one posting list drops below the budget again and pulls in exactly one more
            it.nextPosting();
            assertEquals(4, recording.prefetches.size());

            List<PostingMetadata> returned = new ArrayList<>();
            returned.add(postings.get(0));
            returned.addAll(drain(it));
            assertEquals(postings, returned);
            assertPrefetchesMatch(postings, recording.prefetches);
        }
    }

    public void testEmptyDelegate() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); RecordingIndexInput recording = openRecordingInput(dir)) {
            PrefetchingCentroidIterator it = new PrefetchingCentroidIterator(listIterator(List.of()), recording, randomLongBudget());
            assertFalse(it.hasNext());
            assertTrue(recording.prefetches.isEmpty());
            expectThrows(IllegalStateException.class, it::nextPosting);
        }
    }

    public void testNextOnExhaustedIteratorThrows() throws IOException {
        List<PostingMetadata> postings = postings(3, 100);
        try (Directory dir = new ByteBuffersDirectory(); RecordingIndexInput recording = openRecordingInput(dir)) {
            PrefetchingCentroidIterator it = new PrefetchingCentroidIterator(listIterator(postings), recording, randomLongBudget());
            drain(it);
            assertFalse(it.hasNext());
            expectThrows(IllegalStateException.class, it::nextPosting);
        }
    }

    private long randomLongBudget() {
        return randomFrom(0L, 1L, 100L, 1024L, Long.MAX_VALUE);
    }

    private static List<PostingMetadata> postings(int count, int length) {
        List<PostingMetadata> postings = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            postings.add(new PostingMetadata((long) i * length, length, i, i));
        }
        return postings;
    }

    private static List<PostingMetadata> drain(PrefetchingCentroidIterator it) throws IOException {
        List<PostingMetadata> returned = new ArrayList<>();
        while (it.hasNext()) {
            returned.add(it.nextPosting());
        }
        return returned;
    }

    private static void assertPrefetchesMatch(List<PostingMetadata> expected, List<long[]> prefetches) {
        assertEquals(expected.size(), prefetches.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("prefetch offset at " + i, expected.get(i).offset(), prefetches.get(i)[0]);
            assertEquals("prefetch length at " + i, expected.get(i).length(), prefetches.get(i)[1]);
        }
    }

    private static CentroidIterator listIterator(List<PostingMetadata> postings) {
        return new CentroidIterator() {
            private int idx = 0;

            @Override
            public boolean hasNext() {
                return idx < postings.size();
            }

            @Override
            public PostingMetadata nextPosting() {
                return postings.get(idx++);
            }
        };
    }

    private static RecordingIndexInput openRecordingInput(Directory dir) throws IOException {
        try (IndexOutput out = dir.createOutput("postings", IOContext.DEFAULT)) {
            out.writeByte((byte) 0);
        }
        return new RecordingIndexInput(dir.openInput("postings", IOContext.DEFAULT));
    }

    /**
     * Wraps a real {@link IndexInput} and records the arguments of every {@link #prefetch} call so the
     * test can assert which posting lists were prefetched, and in what order.
     */
    private static class RecordingIndexInput extends FilterIndexInput {
        final List<long[]> prefetches = new ArrayList<>();

        RecordingIndexInput(IndexInput in) {
            super("recording", in);
        }

        @Override
        public void prefetch(long offset, long length) throws IOException {
            prefetches.add(new long[] { offset, length });
        }
    }
}
