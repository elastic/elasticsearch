/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

class PatternedTextSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String templateFieldName;
    private final String argsFieldName;

    PatternedTextSyntheticFieldLoaderLayer(String templateFieldName, String argsFieldName) {
        this.templateFieldName = templateFieldName;
        this.argsFieldName = argsFieldName;
    }

    private PatternedTextSyntheticFieldLoader loader;

    @Override
    public long valueCount() {
        return loader != null ? loader.count() : 0;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        var docValues = PatternedTextDocValues.from(leafReader, templateFieldName, argsFieldName);
        if (docValues == null || docValues.getValueCount() == 0) {
            return null;
        }
        loader = new PatternedTextSyntheticFieldLoader(docValues);
        return loader;
    }

    @Override
    public boolean hasValue() {
        return loader != null && loader.count() > 0;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (loader != null) {
            loader.write(b);
        }
    }

    @Override
    public String fieldName() {
        return "";
    }

    private record PatternedTextSyntheticFieldLoader(PatternedTextDocValues docValues) implements DocValuesLoader {

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            return docValues.advanceExact(docId);
        }

        public int count() {
            return docValues.docValueCount();
        }

        public void write(XContentBuilder b) throws IOException {
            if (docValues.getValueCount() == 0) {
                return;
            }
            for (int i = 0; i < count(); i++) {
                b.value(docValues.lookupOrdAsString(docValues.nextOrd()));
            }
        }
    }
}
