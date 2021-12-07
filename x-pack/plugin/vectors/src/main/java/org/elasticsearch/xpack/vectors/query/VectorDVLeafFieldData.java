/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.Version;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.DocValuesField;
import org.elasticsearch.xpack.vectors.query.BinaryDenseVectorScriptDocValues.BinaryDenseVectorSupplier;
import org.elasticsearch.xpack.vectors.query.DenseVectorScriptDocValues.DenseVectorSupplier;
import org.elasticsearch.xpack.vectors.query.KnnDenseVectorScriptDocValues.KnnDenseVectorSupplier;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.xpack.vectors.query.DenseVectorScriptDocValues.MISSING_VECTOR_FIELD_MESSAGE;

final class VectorDVLeafFieldData implements LeafFieldData {

    private final LeafReader reader;
    private final String field;
    private final Version indexVersion;
    private final int dims;
    private final boolean indexed;

    VectorDVLeafFieldData(LeafReader reader, String field, Version indexVersion, int dims, boolean indexed) {
        this.reader = reader;
        this.field = field;
        this.indexVersion = indexVersion;
        this.dims = dims;
        this.indexed = indexed;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by Lucene
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("String representation of doc values for vector fields is not supported");
    }

    @Override
    public DocValuesField<?> getScriptField(String name) {
        try {
            if (indexed) {
                VectorValues values = reader.getVectorValues(field);
                if (values == null || values == VectorValues.EMPTY) {
                    return new DelegateDocValuesField(DenseVectorScriptDocValues.empty(new DenseVectorSupplier<float[]>() {
                        @Override
                        public float[] getInternal() {
                            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
                        }

                        @Override
                        public void setNextDocId(int docId) throws IOException {
                            // do nothing
                        }

                        @Override
                        public int size() {
                            return 0;
                        }
                    }, dims), name);
                }
                return new DelegateDocValuesField(new KnnDenseVectorScriptDocValues(new KnnDenseVectorSupplier(values), dims), name);
            } else {
                BinaryDocValues values = DocValues.getBinary(reader, field);
                return new DelegateDocValuesField(
                    new BinaryDenseVectorScriptDocValues(new BinaryDenseVectorSupplier(values), indexVersion, dims),
                    name
                );
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values for vector field!", e);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
