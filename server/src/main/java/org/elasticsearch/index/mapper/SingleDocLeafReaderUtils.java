/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Factory helpers shared by {@link DocumentLeafReader} and {@link ColumnarStoredLeafReader} for building
 * single-document doc-values iterators from in-memory value lists.
 */
class SingleDocLeafReaderUtils {

    private SingleDocLeafReaderUtils() {}

    static FieldInfo fieldInfo(String name) {
        return new FieldInfo(
            name,
            0,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    static NumericDocValues numericDocValues(Number value) {
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new NumericDocValues() {
            @Override
            public long longValue() {
                return value.longValue();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }

    static SortedNumericDocValues sortedNumericDocValues(List<Number> values) {
        if (values.isEmpty()) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new SortedNumericDocValues() {

            int i = -1;

            @Override
            public long nextValue() {
                i++;
                return values.get(i).longValue();
            }

            @Override
            public int docValueCount() {
                return values.size();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                i = -1;
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                i = -1;
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                i = -1;
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }

    static BinaryDocValues binaryDocValues(BytesRef value) {
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new BinaryDocValues() {
            @Override
            public BytesRef binaryValue() {
                return value;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }

    static SortedDocValues sortedDocValues(BytesRef value) {
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new SortedDocValues() {

            @Override
            public int ordValue() {
                return 0;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return value;
            }

            @Override
            public int getValueCount() {
                return 1;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }

    static SortedSetDocValues sortedSetDocValues(List<BytesRef> values) {
        if (values.isEmpty()) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new SortedSetDocValues() {

            int i = -1;

            @Override
            public long nextOrd() {
                i++;
                assert i < values.size();
                return i;
            }

            @Override
            public int docValueCount() {
                return values.size();
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return values.get((int) ord);
            }

            @Override
            public long getValueCount() {
                return values.size();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                i = -1;
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                i = -1;
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                i = -1;
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }
}
