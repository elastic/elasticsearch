/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * Internal class that mocks a single doc read from the transaction log as a leaf reader.
 */
public final class TranslogLeafReader extends LeafReader {

    private final Translog.Index operation;
    private static final FieldInfo FAKE_SOURCE_FIELD
        = new FieldInfo(SourceFieldMapper.NAME, 1, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, 0, 0, VectorSimilarityFunction.EUCLIDEAN, false);
    private static final FieldInfo FAKE_ROUTING_FIELD
        = new FieldInfo(RoutingFieldMapper.NAME, 2, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, 0, 0, VectorSimilarityFunction.EUCLIDEAN, false);
    private static final FieldInfo FAKE_ID_FIELD
        = new FieldInfo(IdFieldMapper.NAME, 3, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(),
        0, 0, 0, 0, VectorSimilarityFunction.EUCLIDEAN, false);
    public static Set<String> ALL_FIELD_NAMES = Sets.newHashSet(FAKE_SOURCE_FIELD.name, FAKE_ROUTING_FIELD.name, FAKE_ID_FIELD.name);

    TranslogLeafReader(Translog.Index operation) {
        this.operation = operation;
    }
    @Override
    public CacheHelper getCoreCacheHelper() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Terms terms(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NumericDocValues getNormValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VectorValues getVectorValues(String field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TopDocs searchNearestVectors(String field, float[] target, int k) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldInfos getFieldInfos() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bits getLiveDocs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PointValues getPointValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkIntegrity() {

    }

    @Override
    public LeafMetaData getMetaData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TermVectors getTermVectorsReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int numDocs() {
        return 1;
    }

    @Override
    public int maxDoc() {
        return 1;
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        if (docID != 0) {
            throw new IllegalArgumentException("no such doc ID " + docID);
        }
        if (visitor.needsField(FAKE_SOURCE_FIELD) == StoredFieldVisitor.Status.YES) {
            assert operation.source().toBytesRef().offset == 0;
            assert operation.source().toBytesRef().length == operation.source().toBytesRef().bytes.length;
            visitor.binaryField(FAKE_SOURCE_FIELD, operation.source().toBytesRef().bytes);
        }
        if (operation.routing() != null && visitor.needsField(FAKE_ROUTING_FIELD) == StoredFieldVisitor.Status.YES) {
            visitor.stringField(FAKE_ROUTING_FIELD, operation.routing());
        }
        if (visitor.needsField(FAKE_ID_FIELD) == StoredFieldVisitor.Status.YES) {
            BytesRef bytesRef = Uid.encodeId(operation.id());
            final byte[] id = new byte[bytesRef.length];
            System.arraycopy(bytesRef.bytes, bytesRef.offset, id, 0, bytesRef.length);
            visitor.binaryField(FAKE_ID_FIELD, id);
        }
    }

    @Override
    protected void doClose() {

    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        throw new UnsupportedOperationException();
    }
}
