/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads values from {@code _source}. This whole process is very slow and cast-tastic,
 * so it doesn't really try to avoid megamorphic invocations. It's just going to be
 * slow.
 */
public abstract class BlockSourceReader implements BlockLoader.RowStrideReader {
    private final ValueFetcher fetcher;
    private final List<Object> ignoredValues = new ArrayList<>();

    BlockSourceReader(ValueFetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public final void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        List<Object> values = fetcher.fetchValues(storedFields.source(), docId, ignoredValues);
        ignoredValues.clear();  // TODO do something with these?
        if (values == null || values.isEmpty()) {
            builder.appendNull();
            return;
        }
        if (values.size() == 1) {
            append(builder, values.get(0));
            return;
        }
        builder.beginPositionEntry();
        for (Object v : values) {
            append(builder, v);
        }
        builder.endPositionEntry();
    }

    protected abstract void append(BlockLoader.Builder builder, Object v);

    @Override
    public boolean canReuse(int startingDocID) {
        return true;
    }

    private abstract static class SourceBlockLoader implements BlockLoader {
        @Override
        public final ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
            return null;
        }

        @Override
        public final StoredFieldsSpec rowStrideStoredFieldSpec() {
            return StoredFieldsSpec.NEEDS_SOURCE;
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

    public static class BooleansBlockLoader extends SourceBlockLoader {
        private final ValueFetcher fetcher;

        public BooleansBlockLoader(ValueFetcher fetcher) {
            this.fetcher = fetcher;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.booleans(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) {
            return new Booleans(fetcher);
        }
    }

    private static class Booleans extends BlockSourceReader {
        Booleans(ValueFetcher fetcher) {
            super(fetcher);
        }

        @Override
        protected void append(BlockLoader.Builder builder, Object v) {
            ((BlockLoader.BooleanBuilder) builder).appendBoolean((Boolean) v);
        }

        @Override
        public String toString() {
            return "BlockSourceReader.Booleans";
        }
    }

    public static class BytesRefsBlockLoader extends SourceBlockLoader {
        private final ValueFetcher fetcher;

        public BytesRefsBlockLoader(ValueFetcher fetcher) {
            this.fetcher = fetcher;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) {
            return new BytesRefs(fetcher);
        }
    }

    private static class BytesRefs extends BlockSourceReader {
        BytesRef scratch = new BytesRef();

        BytesRefs(ValueFetcher fetcher) {
            super(fetcher);
        }

        @Override
        protected void append(BlockLoader.Builder builder, Object v) {
            ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(toBytesRef(scratch, (String) v));
        }

        @Override
        public String toString() {
            return "BlockSourceReader.Bytes";
        }
    }

    public static class DoublesBlockLoader extends SourceBlockLoader {
        private final ValueFetcher fetcher;

        public DoublesBlockLoader(ValueFetcher fetcher) {
            this.fetcher = fetcher;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.doubles(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) {
            return new Doubles(fetcher);
        }
    }

    private static class Doubles extends BlockSourceReader {
        Doubles(ValueFetcher fetcher) {
            super(fetcher);
        }

        @Override
        protected void append(BlockLoader.Builder builder, Object v) {
            ((BlockLoader.DoubleBuilder) builder).appendDouble(((Number) v).doubleValue());
        }

        @Override
        public String toString() {
            return "BlockSourceReader.Doubles";
        }
    }

    public static class IntsBlockLoader extends SourceBlockLoader {
        private final ValueFetcher fetcher;

        public IntsBlockLoader(ValueFetcher fetcher) {
            this.fetcher = fetcher;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.ints(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) {
            return new Ints(fetcher);
        }
    }

    private static class Ints extends BlockSourceReader {
        Ints(ValueFetcher fetcher) {
            super(fetcher);
        }

        @Override
        protected void append(BlockLoader.Builder builder, Object v) {
            ((BlockLoader.IntBuilder) builder).appendInt(((Number) v).intValue());
        }

        @Override
        public String toString() {
            return "BlockSourceReader.Ints";
        }
    }

    public static class LongsBlockLoader extends SourceBlockLoader {
        private final ValueFetcher fetcher;

        public LongsBlockLoader(ValueFetcher fetcher) {
            this.fetcher = fetcher;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.longs(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) {
            return new Longs(fetcher);
        }
    }

    private static class Longs extends BlockSourceReader {
        Longs(ValueFetcher fetcher) {
            super(fetcher);
        }

        @Override
        protected void append(BlockLoader.Builder builder, Object v) {
            ((BlockLoader.LongBuilder) builder).appendLong(((Number) v).longValue());
        }

        @Override
        public String toString() {
            return "BlockSourceReader.Longs";
        }
    }

    /**
     * Convert a {@link String} into a utf-8 {@link BytesRef}.
     */
    static BytesRef toBytesRef(BytesRef scratch, String v) {
        int len = UnicodeUtil.maxUTF8Length(v.length());
        if (scratch.bytes.length < len) {
            scratch.bytes = new byte[len];
        }
        scratch.length = UnicodeUtil.UTF16toUTF8(v, 0, v.length(), scratch.bytes);
        return scratch;
    }
}
