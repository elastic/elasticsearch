/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.document.column.VectorColumn;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;

import static org.elasticsearch.escf.EscfColumn.windowValidity;

/**
 * A batch column of vectors for a {@code dense_vector} field, handed to Lucene's columnar {@code IndexWriter.addBatch} as a
 * {@link VectorColumn} and to the row-oriented indexing path as one KNN vector field per document.
 *
 * <p>{@code T} is the vector array type and must match the field type's {@link VectorEncoding}, as {@link VectorColumn}
 * requires: {@code float[]} for {@link VectorEncoding#FLOAT32}, built with {@link #floats}, and {@code byte[]} for
 * {@link VectorEncoding#BYTE}, which byte and bit vectors will use once they support the batch path.
 *
 * <p>The vectors are materialized per document by {@link DenseVectorFieldMapper} from the batch's ESCF source column, because
 * the source column holds the parsed JSON numbers while Lucene needs one vector array per document. A {@code null} entry means
 * the document has no vector (the field is absent or an explicit {@code null}).
 *
 * @param <T> the vector array type, {@code float[]} or {@code byte[]}
 */
final class DenseVectorColumn<T> extends VectorColumn<T> implements LuceneColumn {

    /** Builds the row path's Lucene field for one document's vector, such as {@link KnnFloatVectorField}. */
    @FunctionalInterface
    interface VectorFieldFactory<T> {
        IndexableField create(String name, T vector, FieldType fieldType);
    }

    private final FieldType luceneFieldType;
    private final T[] vectors;
    private final VectorFieldFactory<T> fieldFactory;
    /** Index into {@link #vectors} of this column's first document; slices share the array. */
    private final int offset;
    private final int count;
    /** Optional filter over this column's {@code count} documents; a cleared bit hides the document from Lucene. */
    private final FixedBitSet filter;

    /** A column of {@code float[]} vectors for a {@link VectorEncoding#FLOAT32} field, indexed as {@link KnnFloatVectorField}. */
    static DenseVectorColumn<float[]> floats(String name, FieldType fieldType, float[][] vectors) {
        assert fieldType.vectorEncoding() == VectorEncoding.FLOAT32 : "expected FLOAT32 encoding, got " + fieldType.vectorEncoding();
        return new DenseVectorColumn<>(name, fieldType, vectors, KnnFloatVectorField::new, 0, vectors.length, null);
    }

    private DenseVectorColumn(
        String name,
        FieldType fieldType,
        T[] vectors,
        VectorFieldFactory<T> fieldFactory,
        int offset,
        int count,
        FixedBitSet filter
    ) {
        super(name, fieldType, densityOf(vectors, offset, count, filter));
        assert filter == null || filter.length() == count;
        this.luceneFieldType = fieldType;
        this.vectors = vectors;
        this.fieldFactory = fieldFactory;
        this.offset = offset;
        this.count = count;
        this.filter = filter;
    }

    private static Density densityOf(Object[] vectors, int offset, int count, FixedBitSet filter) {
        if (filter != null) {
            return Density.SPARSE;
        }
        for (int i = offset; i < offset + count; i++) {
            if (vectors[i] == null) {
                return Density.SPARSE;
            }
        }
        return Density.DENSE;
    }

    /** The next document at or after {@code from} that has a vector and passes the filter, or {@link DocIdSetIterator#NO_MORE_DOCS}. */
    private int nextPresent(int from) {
        for (int doc = from; doc < count; doc++) {
            if (vectors[offset + doc] != null && (filter == null || filter.get(doc))) {
                return doc;
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public ObjectTupleCursor<T> tuples() {
        return new ObjectTupleCursor<>() {
            private int doc = -1;

            @Override
            public int nextDoc() {
                return doc = nextPresent(doc + 1);
            }

            @Override
            public T value() {
                return vectors[offset + doc];
            }
        };
    }

    @Override
    public DenseVectorColumn<T> slice(int from, int sliceCount) {
        assert from >= 0 && sliceCount >= 0 && from + sliceCount <= count;
        return new DenseVectorColumn<>(
            name(),
            luceneFieldType,
            vectors,
            fieldFactory,
            offset + from,
            sliceCount,
            windowValidity(filter, from, sliceCount)
        );
    }

    @Override
    public DenseVectorColumn<T> withFilter(FixedBitSet newFilter) {
        assert newFilter == null || newFilter.length() == count;
        return new DenseVectorColumn<>(
            name(),
            luceneFieldType,
            vectors,
            fieldFactory,
            offset,
            count,
            LuceneColumn.singleFilter(filter, newFilter)
        );
    }

    @Override
    public Column toLuceneColumn() {
        return this;
    }

    @Override
    public RowFieldCursor rowFieldCursor() {
        return new RowFieldCursor() {
            private int doc = -1;

            @Override
            public int nextDoc() {
                return doc = nextPresent(doc + 1);
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                out.add(fieldFactory.create(name(), vectors[offset + doc], luceneFieldType));
            }
        };
    }
}
