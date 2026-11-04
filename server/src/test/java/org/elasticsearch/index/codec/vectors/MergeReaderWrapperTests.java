/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * {@link MergeReaderWrapper} serves searches from one reader and merges from another. Lucene brackets
 * a merge with {@code getMergeInstance()} and {@code finishMerge()} on the reader it is handed, so
 * both calls have to reach the reader that actually performs the merge.
 */
public class MergeReaderWrapperTests extends ESTestCase {

    /** Minimal reader that only records the lifecycle calls it receives. */
    private static class RecordingReader extends FlatVectorsReader {
        int getMergeInstanceCalls;
        int finishMergeCalls;
        int closeCalls;
        final FlatVectorsReader mergeInstance;
        final Map<String, Long> offHeap;

        RecordingReader(FlatVectorsReader mergeInstance) {
            this(mergeInstance, Map.of());
        }

        RecordingReader(FlatVectorsReader mergeInstance, Map<String, Long> offHeap) {
            this.mergeInstance = mergeInstance == null ? this : mergeInstance;
            this.offHeap = offHeap;
        }

        @Override
        public FlatVectorsReader getMergeInstance() {
            getMergeInstanceCalls++;
            return mergeInstance;
        }

        @Override
        public void finishMerge() {
            finishMergeCalls++;
        }

        @Override
        public FlatVectorsScorer getFlatVectorScorer(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, float[] target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkIntegrity() {}

        @Override
        public FloatVectorValues getFloatVectorValues(String field) {
            return null;
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) {
            return null;
        }

        @Override
        public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {}

        @Override
        public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {}

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
            return offHeap;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    public void testGetMergeInstanceIsDelegatedToTheMergeReader() throws IOException {
        RecordingReader mergeInstance = new RecordingReader(null);
        RecordingReader mergeReader = new RecordingReader(mergeInstance);
        RecordingReader mainReader = new RecordingReader(null);

        try (MergeReaderWrapper wrapper = new MergeReaderWrapper(mainReader, () -> mergeReader, randomBoolean())) {
            assertSame(mergeInstance, wrapper.getMergeInstance());
        }

        assertEquals(1, mergeReader.getMergeInstanceCalls);
        assertEquals("the search reader must not be asked for a merge instance", 0, mainReader.getMergeInstanceCalls);
    }

    public void testFinishMergeIsDelegatedToTheMergeReader() throws IOException {
        RecordingReader mergeReader = new RecordingReader(null);
        RecordingReader mainReader = new RecordingReader(null);

        try (MergeReaderWrapper wrapper = new MergeReaderWrapper(mainReader, () -> mergeReader, randomBoolean())) {
            wrapper.getMergeInstance();
            wrapper.finishMerge();
        }

        assertEquals(1, mergeReader.finishMergeCalls);
        assertEquals("the search reader takes no part in the merge", 0, mainReader.finishMergeCalls);
    }

    public void testMergeReaderIsCreatedOnceAndClosedWithTheWrapper() throws IOException {
        RecordingReader mergeReader = new RecordingReader(null);
        RecordingReader mainReader = new RecordingReader(null);
        int[] created = new int[1];

        MergeReaderWrapper wrapper = new MergeReaderWrapper(mainReader, () -> {
            created[0]++;
            return mergeReader;
        }, randomBoolean());
        assertEquals("the merge reader is created lazily", 0, created[0]);
        wrapper.getMergeInstance();
        wrapper.getMergeInstance();
        assertEquals("the merge reader is created once", 1, created[0]);

        wrapper.close();
        wrapper.close();
        assertEquals(1, mainReader.closeCalls);
        assertEquals("a merge reader that was created must be closed with the wrapper", 1, mergeReader.closeCalls);
        expectThrows(AlreadyClosedException.class, wrapper::getMergeInstance);
        assertEquals("a merge reader must not be created after close", 1, created[0]);
    }

    public void testCloseWithoutMergeClosesOnlyTheSearchReader() throws IOException {
        RecordingReader mainReader = new RecordingReader(null);
        int[] created = new int[1];

        try (MergeReaderWrapper wrapper = new MergeReaderWrapper(mainReader, () -> {
            created[0]++;
            return new RecordingReader(null);
        }, randomBoolean())) {
            wrapper.finishMerge(); // a no-op when no merge began
        }

        assertEquals(1, mainReader.closeCalls);
        assertEquals("no merge, no merge reader", 0, created[0]);
    }

    public void testOffHeapByteSizeComesFromTheSearchReaderUnlessItReadsWithDirectIO() {
        Map<String, Long> offHeap = Map.of("vec", 1234L);
        RecordingReader mainReader = new RecordingReader(null, offHeap);
        RecordingReader mergeReader = new RecordingReader(null, Map.of("vec", 999L));

        MergeReaderWrapper mmapSearches = new MergeReaderWrapper(mainReader, () -> mergeReader, false);
        assertEquals("a memory-mapped search reader keeps reporting its off-heap bytes", offHeap, mmapSearches.getOffHeapByteSize(null));

        MergeReaderWrapper directIOSearches = new MergeReaderWrapper(mainReader, () -> mergeReader, true);
        assertEquals("a direct I/O search reader has no off-heap bytes", Map.of(), directIOSearches.getOffHeapByteSize(null));
    }
}
