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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.Function;

public class SingletonBinaryDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private SingletonBinaryDocValuesSyntheticFieldLoader loader;
    private final String name;
    private final Function<LeafReader, BinaryDocValues> docValuesSupplier;

    public SingletonBinaryDocValuesSyntheticFieldLoaderLayer(String name, Function<LeafReader, BinaryDocValues> docValuesSupplier) {
        this.name = name;
        this.docValuesSupplier = docValuesSupplier;
    }

    @Override
    public long valueCount() {
        return loader != null && loader.hasValue() ? 1 : 0;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        var docValues = docValuesSupplier.apply(leafReader);
        if (docValues == null) {
            return null;
        }
        loader = new SingletonBinaryDocValuesSyntheticFieldLoader(docValues);
        return loader;
    }

    @Override
    public boolean hasValue() {
        return loader != null && loader.hasValue();
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (loader != null) {
            loader.write(b);
        }
    }

    @Override
    public String fieldName() {
        return name;
    }

    private static class SingletonBinaryDocValuesSyntheticFieldLoader implements DocValuesLoader {
        private final BinaryDocValues docValues;
        private boolean hasValue = false;

        SingletonBinaryDocValuesSyntheticFieldLoader(BinaryDocValues docValues) {
            this.docValues = docValues;
        }

        public boolean hasValue() {
            assert docValues.docID() != DocIdSetIterator.NO_MORE_DOCS;
            return hasValue;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            return hasValue = docValues.advanceExact(docId);
        }

        public void write(XContentBuilder b) throws IOException {
            if (hasValue) {
                b.value(docValues.binaryValue().utf8ToString());
            }
        }
    }
}
