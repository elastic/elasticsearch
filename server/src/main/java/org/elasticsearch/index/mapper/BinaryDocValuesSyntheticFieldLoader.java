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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class BinaryDocValuesSyntheticFieldLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
    private final String name;
    private BinaryDocValues values;
    private boolean hasValue;

    protected BinaryDocValuesSyntheticFieldLoader(String name) {
        this.name = name;
    }

    protected abstract void writeValue(XContentBuilder b, BytesRef value) throws IOException;

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        values = leafReader.getBinaryDocValues(name);
        if (values == null) {
            return null;
        }
        return docId -> {
            hasValue = values.advanceExact(docId);
            return hasValue;
        };
    }

    @Override
    public boolean hasValue() {
        return hasValue;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (false == hasValue) {
            return;
        }

        writeValue(b, values.binaryValue());
    }

    @Override
    public String fieldName() {
        return name;
    }
}
