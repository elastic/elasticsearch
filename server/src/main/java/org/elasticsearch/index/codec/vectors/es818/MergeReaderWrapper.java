/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

class MergeReaderWrapper extends FlatVectorsReader {

    private final FlatVectorsReader mainReader;
    private final FlatVectorsReader mergeReader;

    protected MergeReaderWrapper(FlatVectorsReader mainReader, FlatVectorsReader mergeReader) {
        super(mainReader.getFlatVectorScorer());
        this.mainReader = mainReader;
        this.mergeReader = mergeReader;
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
    public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        mainReader.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        mainReader.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public FlatVectorsReader getMergeInstance() {
        return mergeReader;
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
        // TODO: https://github.com/elastic/elasticsearch/issues/128672
        // return mainReader.getOffHeapByteSize(fieldInfo);
        return Map.of(); // no off-heap when using direct IO
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(mainReader, mergeReader);
    }
}
