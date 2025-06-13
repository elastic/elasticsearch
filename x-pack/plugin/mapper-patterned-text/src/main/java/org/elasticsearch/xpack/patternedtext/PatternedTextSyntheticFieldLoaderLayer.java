/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

class PatternedTextSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String name;
    private final String templateFieldName;
    private final String argsFieldName;
    private PatternedTextSyntheticFieldLoader loader;

    PatternedTextSyntheticFieldLoaderLayer(String name, String templateFieldName, String argsFieldName) {
        this.name = name;
        this.templateFieldName = templateFieldName;
        this.argsFieldName = argsFieldName;
    }

    @Override
    public long valueCount() {
        return loader != null && loader.hasValue() ? 1 : 0;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        var docValues = PatternedTextDocValues.from(leafReader, templateFieldName, argsFieldName);
        if (docValues == null) {
            return null;
        }
        loader = new PatternedTextSyntheticFieldLoader(docValues);
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

    private static class PatternedTextSyntheticFieldLoader implements DocValuesLoader {
        private final PatternedTextDocValues docValues;
        private boolean hasValue = false;

        PatternedTextSyntheticFieldLoader(PatternedTextDocValues docValues) {
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
