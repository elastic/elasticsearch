/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

class PatternTextSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private PatternTextSyntheticFieldLoader loader;
    private final String name;
    private final IOFunction<LeafReader, BinaryDocValues> docValuesSupplier;

    PatternTextSyntheticFieldLoaderLayer(String name, IOFunction<LeafReader, BinaryDocValues> docValuesSupplier) {
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
        loader = new PatternTextSyntheticFieldLoader(docValues);
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

    private static class PatternTextSyntheticFieldLoader implements DocValuesLoader {
        private final BinaryDocValues docValues;
        private boolean hasValue = false;

        PatternTextSyntheticFieldLoader(BinaryDocValues docValues) {
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
