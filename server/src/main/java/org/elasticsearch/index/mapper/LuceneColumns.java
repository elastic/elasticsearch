/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Adapts plain in-memory arrays to Lucene's column-oriented batch indexing API
 * ({@code org.apache.lucene.document.column}), for metadata columns built by the bulk columnar
 * batch-mapping path (see {@link ShardBatchMapper}). Metadata mappers hold no EICF/ESCF-backed
 * column — id/routing/source values come straight from the bulk requests, and engine-assigned
 * values ({@code _seq_no}/{@code _primary_term}/{@code _version}) come from mutable arrays the
 * engine fills after mapping — so only the array-backed factories are needed here.
 *
 * <p>Field (non-metadata) mappers converting an EICF/ESCF source column to a Lucene column are a
 * follow-up; that adapter can live alongside these once field mappers support columnar parsing.
 */
public final class LuceneColumns {

    private LuceneColumns() {}

    /**
     * A {@link LongColumn} backed by a plain {@code long[]}. The array may be mutated by the caller
     * (e.g. the engine filling {@code _seq_no}/{@code _version}) up until a cursor is requested.
     * Always {@link org.apache.lucene.document.column.Column.Density#DENSE}.
     */
    public static LongColumn arrayLongColumn(long[] values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        return new ArrayLongColumn(values, name, fieldType, kind);
    }

    /**
     * A {@link BinaryColumn} backed by a {@code BytesRef[]}. A {@code null} entry marks an absent
     * document; the column is {@link org.apache.lucene.document.column.Column.Density#DENSE} only
     * when every entry is present.
     */
    public static BinaryColumn arrayBinaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType) {
        return new ArrayBinaryColumn(values, name, fieldType);
    }

    private static final class ArrayLongColumn extends LongColumn {
        private final long[] values;

        ArrayLongColumn(long[] values, String name, IndexableFieldType fieldType, NumericKind kind) {
            super(name, fieldType, Density.DENSE, kind);
            this.values = values;
        }

        @Override
        public LongTupleCursor tuples() {
            return new LongTupleCursor() {
                private int doc = -1;

                @Override
                public int nextDoc() {
                    return ++doc < values.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
                }

                @Override
                public long longValue() {
                    return values[doc];
                }
            };
        }

        @Override
        public LongValuesCursor values() {
            return new LongValuesCursor(values.length) {
                private int pos;

                @Override
                public long nextLong() {
                    if (pos >= size()) {
                        throw new IllegalStateException("nextLong() called more than size()=" + size() + " times");
                    }
                    return values[pos++];
                }

                @Override
                public void fillDocValues(long[] dst, int offset, int length) {
                    if (pos + length > size()) {
                        throw new IllegalStateException("fill of " + length + " from pos " + pos + " exceeds size()=" + size());
                    }
                    System.arraycopy(values, pos, dst, offset, length);
                    pos += length;
                }
            };
        }
    }

    private static final class ArrayBinaryColumn extends BinaryColumn {
        private final BytesRef[] values;
        private final boolean dense;

        ArrayBinaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType) {
            super(name, fieldType, allPresent(values) ? Density.DENSE : Density.SPARSE);
            this.values = values;
            this.dense = allPresent(values);
        }

        private static boolean allPresent(BytesRef[] values) {
            for (BytesRef v : values) {
                if (v == null) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public ObjectTupleCursor<BytesRef> tuples() {
            return new ObjectTupleCursor<>() {
                private int doc = -1;

                @Override
                public int nextDoc() {
                    int next = doc + 1;
                    while (next < values.length && values[next] == null) {
                        next++;
                    }
                    doc = next;
                    return next < values.length ? next : DocIdSetIterator.NO_MORE_DOCS;
                }

                @Override
                public BytesRef value() {
                    return values[doc];
                }
            };
        }

        @Override
        public BytesRefValuesCursor values() {
            if (dense == false) {
                return super.values(); // throws; never consulted for SPARSE columns
            }
            return new BytesRefValuesCursor(values.length) {
                private int pos;

                @Override
                public BytesRef nextValue() {
                    if (pos >= size()) {
                        throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
                    }
                    return values[pos++];
                }
            };
        }
    }
}
