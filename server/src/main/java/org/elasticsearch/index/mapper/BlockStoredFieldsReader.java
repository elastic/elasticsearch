/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader.BytesRefBuilder;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Loads values from {@link LeafReader#storedFields}. This whole process is very slow
 * and cast-tastic, so it doesn't really try to avoid megamorphic invocations. It's
 * just going to be slow.
 *
 * Note that this extends {@link BlockDocValuesReader} because it pretends to load
 * doc values because, for now, ESQL only knows how to load things in a doc values
 * order.
 */
public abstract class BlockStoredFieldsReader implements BlockLoader.RowStrideReader {
    @Override
    public boolean canReuse(int startingDocID) {
        return true;
    }

    private abstract static class StoredFieldsBlockLoader implements BlockLoader {
        protected final String field;

        StoredFieldsBlockLoader(String field) {
            this.field = field;
        }

        @Override
        public final ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) {
            return null;
        }

        @Override
        public final StoredFieldsSpec rowStrideStoredFieldSpec() {
            return new StoredFieldsSpec(false, false, Set.of(field));
        }

        @Override
        public final boolean supportsOrdinals() {
            return false;
        }

        @Override
        public final SortedSetDocValues ordinals(LeafReaderContext context) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Load {@link BytesRef} blocks from stored {@link BytesRef}s.
     */
    public static class BytesFromBytesRefsBlockLoader extends StoredFieldsBlockLoader {
        public BytesFromBytesRefsBlockLoader(String field) {
            super(field);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            return new Bytes(field) {
                @Override
                protected BytesRef toBytesRef(Object v) {
                    return (BytesRef) v;
                }
            };
        }
    }

    /**
     * Load {@link BytesRef} blocks from stored {@link String}s.
     */
    public static class BytesFromStringsBlockLoader extends StoredFieldsBlockLoader {
        public BytesFromStringsBlockLoader(String field) {
            super(field);
        }

        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            return new Bytes(field) {
                private final BytesRef scratch = new BytesRef();

                @Override
                protected BytesRef toBytesRef(Object v) {
                    return BlockSourceReader.toBytesRef(scratch, (String) v);
                }
            };
        }
    }

    private abstract static class Bytes extends BlockStoredFieldsReader {
        private final String field;

        Bytes(String field) {
            this.field = field;
        }

        protected abstract BytesRef toBytesRef(Object v);

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
            List<Object> values = storedFields.storedFields().get(field);
            if (values == null) {
                builder.appendNull();
                return;
            }
            if (values.size() == 1) {
                ((BytesRefBuilder) builder).appendBytesRef(toBytesRef(values.get(0)));
                return;
            }
            builder.beginPositionEntry();
            for (Object v : values) {
                ((BytesRefBuilder) builder).appendBytesRef(toBytesRef(v));
            }
            builder.endPositionEntry();
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.Bytes";
        }
    }

    /**
     * Load {@link BytesRef} blocks from stored {@link String}s.
     */
    public static class IdBlockLoader extends StoredFieldsBlockLoader {
        public IdBlockLoader() {
            super(IdFieldMapper.NAME);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            return new Id();
        }
    }

    private static class Id extends BlockStoredFieldsReader {
        private final BytesRef scratch = new BytesRef();

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
            ((BytesRefBuilder) builder).appendBytesRef(BlockSourceReader.toBytesRef(scratch, storedFields.id()));
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.Id";
        }
    }
}
