/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public final class BinaryDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String name;
    private SortedBinaryDocValues bytesValues;
    private boolean hasValue;

    public BinaryDocValuesSyntheticFieldLoaderLayer(String name) {
        this.name = name;
    }

    @Override
    public long valueCount() {
        return hasValue ? bytesValues.docValueCount() : 0;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        var docValues = leafReader.getBinaryDocValues(name);
        if (docValues == null) {
            bytesValues = null;
            hasValue = false;
            return null;
        }

        bytesValues = new MultiValuedSortedBinaryDocValues(docValues);

        return docId -> {
            hasValue = bytesValues.advanceExact(docId);
            return hasValue;
        };
    }

    @Override
    public boolean hasValue() {
        return hasValue;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (hasValue == false) {
            return;
        }

        for (int i = 0; i < bytesValues.docValueCount(); ++i) {
            BytesRef value = bytesValues.nextValue();
            b.utf8Value(value.bytes, value.offset, value.length);
        }
    }

    @Override
    public String fieldName() {
        return name;
    }
}
