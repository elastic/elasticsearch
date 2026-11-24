/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.document.column.TokenStreamColumn;
import org.apache.lucene.document.column.VectorColumn;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Views of filtered Lucene columns in which the remaining documents are renumbered densely, for implementations of
 * {@link LuceneColumn#toLuceneColumn(int[])}.
 *
 * <p>Lucene adds one document per batch-local doc id of a column batch, so the documents that a filter excludes must not keep a doc
 * id. A filtered column is always {@link Column.Density#SPARSE}, so Lucene only ever reads it through its tuple cursor, and that
 * cursor already skips the excluded documents: renumbering the doc ids it returns is all that is left to do, whatever the column is
 * backed by. There is one method per type of column that Lucene takes in a batch, so that a column gets the view of its own type by
 * passing itself.
 */
public final class CompactedColumns {

    private CompactedColumns() {}

    /**
     * @param compactedDocIds the doc id each document of the column's window takes in the batch, {@code -1} for the excluded ones
     */
    public static LongColumn of(LongColumn column, int[] compactedDocIds) {
        assertFiltered(column);
        return new LongColumn(column.name(), column.fieldType(), Column.Density.SPARSE, column.numericKind()) {
            @Override
            public LongTupleCursor tuples() {
                final LongTupleCursor in = column.tuples();
                return new LongTupleCursor() {
                    @Override
                    public int nextDoc() {
                        return compactedDocId(in.nextDoc(), compactedDocIds);
                    }

                    @Override
                    public long longValue() {
                        return in.longValue();
                    }
                };
            }
        };
    }

    /**
     * @param compactedDocIds the doc id each document of the column's window takes in the batch, {@code -1} for the excluded ones
     */
    public static BinaryColumn of(BinaryColumn column, int[] compactedDocIds) {
        assertFiltered(column);
        return new BinaryColumn(column.name(), column.fieldType(), Column.Density.SPARSE) {
            @Override
            public StoredValue.Type storedType() {
                return column.storedType();
            }

            @Override
            public ObjectTupleCursor<BytesRef> tuples() {
                return compact(column.tuples(), compactedDocIds);
            }
        };
    }

    /**
     * @param compactedDocIds the doc id each document of the column's window takes in the batch, {@code -1} for the excluded ones
     */
    public static TokenStreamColumn of(TokenStreamColumn column, int[] compactedDocIds) {
        assertFiltered(column);
        return new TokenStreamColumn(column.name(), column.fieldType(), Column.Density.SPARSE) {
            @Override
            public ObjectTupleCursor<TokenStream> tuples() {
                return compact(column.tuples(), compactedDocIds);
            }
        };
    }

    /**
     * @param compactedDocIds the doc id each document of the column's window takes in the batch, {@code -1} for the excluded ones
     */
    public static <T> VectorColumn<T> of(VectorColumn<T> column, int[] compactedDocIds) {
        assertFiltered(column);
        return new VectorColumn<>(column.name(), column.fieldType(), Column.Density.SPARSE) {
            @Override
            public ObjectTupleCursor<T> tuples() {
                return compact(column.tuples(), compactedDocIds);
            }
        };
    }

    private static void assertFiltered(Column column) {
        assert column.density() == Column.Density.SPARSE : "filtered column [" + column.name() + "] must be sparse";
    }

    private static <T> ObjectTupleCursor<T> compact(ObjectTupleCursor<T> in, int[] compactedDocIds) {
        return new ObjectTupleCursor<>() {
            @Override
            public int nextDoc() {
                return compactedDocId(in.nextDoc(), compactedDocIds);
            }

            @Override
            public T value() {
                return in.value();
            }
        };
    }

    private static int compactedDocId(int doc, int[] compactedDocIds) {
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            return doc;
        }
        assert compactedDocIds[doc] >= 0 : "column emitted filtered-out doc [" + doc + "]";
        return compactedDocIds[doc];
    }
}
