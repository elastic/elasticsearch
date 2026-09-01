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
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * A {@link FlatVectorsReader} that serves searches from one reader and merges from a second,
 * lazily-created reader, so that the two can use different I/O strategies (such as different
 * direct I/O buffer sizes) without sharing input state.
 */
public class MergeReaderWrapper extends FlatVectorsReader {

    private final FlatVectorsReader mainReader;
    private final IOSupplier<FlatVectorsReader> mergeReaderSupplier;
    private final boolean mainReaderUsesDirectIO;
    private FlatVectorsReader mergeReader;
    private boolean closed;

    /**
     * @param mainReader the reader that serves searches
     * @param mergeReaderSupplier creates the reader that serves merges, the first time a merge asks
     * @param mainReaderUsesDirectIO whether {@code mainReader} reads with direct I/O rather than
     *        memory-mapping, which decides whether it has off-heap bytes to report
     */
    public MergeReaderWrapper(
        FlatVectorsReader mainReader,
        IOSupplier<FlatVectorsReader> mergeReaderSupplier,
        boolean mainReaderUsesDirectIO
    ) {
        this.mainReader = mainReader;
        this.mergeReaderSupplier = mergeReaderSupplier;
        this.mainReaderUsesDirectIO = mainReaderUsesDirectIO;
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer(String field) throws IOException {
        return mainReader.getFlatVectorScorer(field);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        return mainReader.getRandomVectorScorer(field, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        return mainReader.getRandomVectorScorer(field, target);
    }

    @Override
    public void checkIntegrity() throws IOException {
        mainReader.checkIntegrity();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return mainReader.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return mainReader.getByteVectorValues(field);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        mainReader.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        mainReader.search(field, target, knnCollector, acceptDocs);
    }

    // the merge thread calls getMergeInstance() and finishMerge(); close() comes from whichever thread
    // releases the last reference to the pooled reader, so the lazily-created merge reader is guarded:
    // it must not be created after close() has run, and close() must see it once it exists

    @Override
    public synchronized FlatVectorsReader getMergeInstance() throws IOException {
        if (closed) {
            throw new AlreadyClosedException("this MergeReaderWrapper is closed");
        }
        // created lazily: most segments are never merged during a reader's lifetime, and the
        // merge reader holds direct I/O resources
        if (mergeReader == null) {
            mergeReader = mergeReaderSupplier.get();
        }
        // delegate so the reader can prepare itself for merging, e.g. Lucene99FlatVectorsReader
        // switches its data input to sequential read advice
        return mergeReader.getMergeInstance();
    }

    @Override
    public synchronized void finishMerge() throws IOException {
        // the merge reader exists iff a merge began
        if (mergeReader != null) {
            mergeReader.finishMerge();
        }
    }

    @Override
    public long ramBytesUsed() {
        return mainReader.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return mainReader.getChildResources();
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        if (mainReaderUsesDirectIO) {
            // TODO: https://github.com/elastic/elasticsearch/issues/128672
            // return mainReader.getOffHeapByteSize(fieldInfo);
            return Map.of(); // no off-heap when using direct IO
        }
        // a memory-mapped search reader has the same off-heap footprint whether or not merges
        // use direct I/O; the merge reader is short-lived and not part of what searches hold
        return mainReader.getOffHeapByteSize(fieldInfo);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.close(mainReader, mergeReader);
    }
}
