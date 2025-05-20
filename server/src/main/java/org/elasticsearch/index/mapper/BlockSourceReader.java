/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Loads values from {@code _source}. This whole process is very slow and cast-tastic,
 * so it doesn't really try to avoid megamorphic invocations. It's just going to be
 * slow.
 */
public abstract class BlockSourceReader implements BlockLoader.RowStrideReader {
    private final ValueFetcher fetcher;
    private final List<Object> ignoredValues = new ArrayList<>();
    private final DocIdSetIterator iter;
    private final Thread creationThread;
    private int docId = -1;

    private BlockSourceReader(ValueFetcher fetcher, DocIdSetIterator iter) {
        this.fetcher = fetcher;
        this.iter = iter;
        this.creationThread = Thread.currentThread();
    }

    @Override
    public final void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        if (canSkipLoading(docId)) {
            builder.appendNull();
            return;
        }
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

    /**
     * Returns {@code true} if we are <strong>sure</strong> there are no values
     * for this field.
     */
    private boolean canSkipLoading(int docId) throws IOException {
        assert docId >= this.docId;
        this.docId = docId;
        if (docId == iter.docID()) {
            return false;
        }
        return (docId > iter.docID() && iter.advance(docId) == docId) == false;
    }

    @Override
    public final boolean canReuse(int startingDocID) {
        return creationThread == Thread.currentThread() && docId <= startingDocID;
    }

    public interface LeafIteratorLookup {
        DocIdSetIterator lookup(LeafReaderContext ctx) throws IOException;
    }

    private abstract static class SourceBlockLoader implements BlockLoader {
        protected final ValueFetcher fetcher;
        private final LeafIteratorLookup lookup;

        private SourceBlockLoader(ValueFetcher fetcher, LeafIteratorLookup lookup) {
            this.fetcher = fetcher;
            this.lookup = lookup;
        }

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

        @Override
        public final RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            DocIdSetIterator iter = lookup.lookup(context);
            if (iter == null) {
                return new ConstantNullsReader();
            }
            return rowStrideReader(context, iter);
        }

        protected abstract RowStrideReader rowStrideReader(LeafReaderContext context, DocIdSetIterator iter) throws IOException;

        @Override
        public final String toString() {
            return "BlockSourceReader." + name() + "[" + lookup + "]";
        }

        protected abstract String name();
    }

    /**
     * Load {@code boolean}s from {@code _source}.
     */
    public static class BooleansBlockLoader extends SourceBlockLoader {
        public BooleansBlockLoader(ValueFetcher fetcher, LeafIteratorLookup lookup) {
            super(fetcher, lookup);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.booleans(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context, DocIdSetIterator iter) {
            return new Booleans(fetcher, iter);
        }

        @Override
        protected String name() {
            return "Booleans";
        }
    }

    private static class Booleans extends BlockSourceReader {
        Booleans(ValueFetcher fetcher, DocIdSetIterator iter) {
            super(fetcher, iter);
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

    /**
     * Load {@link BytesRef}s from {@code _source}.
     */
    public static class BytesRefsBlockLoader extends SourceBlockLoader {
        public BytesRefsBlockLoader(ValueFetcher fetcher, LeafIteratorLookup lookup) {
            super(fetcher, lookup);
        }

        @Override
        public final Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        protected RowStrideReader rowStrideReader(LeafReaderContext context, DocIdSetIterator iter) throws IOException {
            return new BytesRefs(fetcher, iter);
        }

        @Override
        protected String name() {
            return "Bytes";
        }
    }

    public static class GeometriesBlockLoader extends SourceBlockLoader {
        public GeometriesBlockLoader(ValueFetcher fetcher, LeafIteratorLookup lookup) {
            super(fetcher, lookup);
        }

        @Override
        public final Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        protected RowStrideReader rowStrideReader(LeafReaderContext context, DocIdSetIterator iter) {
            return new Geometries(fetcher, iter);
        }

        @Override
        protected String name() {
            return "Geometries";
        }
    }

    private static class BytesRefs extends BlockSourceReader {
        private final BytesRef scratch = new BytesRef();

        BytesRefs(ValueFetcher fetcher, DocIdSetIterator iter) {
            super(fetcher, iter);
        }

        @Override
        protected void append(BlockLoader.Builder builder, Object v) {
            ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(toBytesRef(scratch, Objects.toString(v)));
        }

        @Override
        public String toString() {
            return "BlockSourceReader.Bytes";
        }
    }

    private static class Geometries extends BlockSourceReader {

        Geometries(ValueFetcher fetcher, DocIdSetIterator iter) {
            super(fetcher, iter);
        }

        @Override
        protected void append(BlockLoader.Builder builder, Object v) {
            if (v instanceof byte[] wkb) {
                ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(new BytesRef(wkb));
            } else {
                throw new IllegalArgumentException("Unsupported source type for spatial geometry: " + v.getClass().getSimpleName());
            }
        }

        @Override
        public String toString() {
            return "BlockSourceReader.Geometries";
        }
    }

    /**
     * Load {@code double}s from {@code _source}.
     */
    public static class DoublesBlockLoader extends SourceBlockLoader {
        public DoublesBlockLoader(ValueFetcher fetcher, LeafIteratorLookup lookup) {
            super(fetcher, lookup);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.doubles(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context, DocIdSetIterator iter) {
            return new Doubles(fetcher, iter);
        }

        @Override
        protected String name() {
            return "Doubles";
        }
    }

    private static class Doubles extends BlockSourceReader {
        Doubles(ValueFetcher fetcher, DocIdSetIterator iter) {
            super(fetcher, iter);
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

    /**
     * Load {@code int}s from {@code _source}.
     */
    public static class IntsBlockLoader extends SourceBlockLoader {
        public IntsBlockLoader(ValueFetcher fetcher, LeafIteratorLookup lookup) {
            super(fetcher, lookup);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.ints(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context, DocIdSetIterator iter) throws IOException {
            return new Ints(fetcher, iter);
        }

        @Override
        protected String name() {
            return "Ints";
        }
    }

    private static class Ints extends BlockSourceReader {
        Ints(ValueFetcher fetcher, DocIdSetIterator iter) {
            super(fetcher, iter);
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

    /**
     * Load {@code long}s from {@code _source}.
     */
    public static class LongsBlockLoader extends SourceBlockLoader {
        public LongsBlockLoader(ValueFetcher fetcher, LeafIteratorLookup lookup) {
            super(fetcher, lookup);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.longs(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context, DocIdSetIterator iter) {
            return new Longs(fetcher, iter);
        }

        @Override
        protected String name() {
            return "Longs";
        }
    }

    private static class Longs extends BlockSourceReader {
        Longs(ValueFetcher fetcher, DocIdSetIterator iter) {
            super(fetcher, iter);
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
     * Load {@code ip}s from {@code _source}.
     */
    public static class IpsBlockLoader extends SourceBlockLoader {
        public IpsBlockLoader(ValueFetcher fetcher, LeafIteratorLookup lookup) {
            super(fetcher, lookup);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context, DocIdSetIterator iter) {
            return new Ips(fetcher, iter);
        }

        @Override
        protected String name() {
            return "Ips";
        }
    }

    private static class Ips extends BlockSourceReader {
        Ips(ValueFetcher fetcher, DocIdSetIterator iter) {
            super(fetcher, iter);
        }

        @Override
        protected void append(BlockLoader.Builder builder, Object v) {
            ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(new BytesRef(InetAddressPoint.encode((InetAddress) v)));
        }

        @Override
        public String toString() {
            return "BlockSourceReader.Ips";
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

    /**
     * Build a {@link LeafIteratorLookup} which checks for norms of a text field.
     */
    public static LeafIteratorLookup lookupMatchingAll() {
        return new LeafIteratorLookup() {
            @Override
            public DocIdSetIterator lookup(LeafReaderContext ctx) throws IOException {
                return DocIdSetIterator.all(ctx.reader().maxDoc());
            }

            @Override
            public String toString() {
                return "All";
            }
        };
    }

    /**
     * Build a {@link LeafIteratorLookup} which checks for the field in the
     * {@link FieldNamesFieldMapper field names field}.
     */
    public static LeafIteratorLookup lookupFromFieldNames(FieldNamesFieldMapper.FieldNamesFieldType fieldNames, String fieldName) {
        if (false == fieldNames.isEnabled()) {
            return lookupMatchingAll();
        }
        return new LeafIteratorLookup() {
            private final BytesRef name = new BytesRef(fieldName);

            @Override
            public DocIdSetIterator lookup(LeafReaderContext ctx) throws IOException {
                Terms terms = ctx.reader().terms(FieldNamesFieldMapper.NAME);
                if (terms == null) {
                    return null;
                }
                TermsEnum termsEnum = terms.iterator();
                if (termsEnum.seekExact(name) == false) {
                    return null;
                }
                return termsEnum.postings(null, PostingsEnum.NONE);
            }

            @Override
            public String toString() {
                return "FieldName";
            }
        };
    }

    /**
     * Build a {@link LeafIteratorLookup} which checks for norms of a text field.
     */
    public static LeafIteratorLookup lookupFromNorms(String fieldName) {
        return new LeafIteratorLookup() {
            @Override
            public DocIdSetIterator lookup(LeafReaderContext ctx) throws IOException {
                return ctx.reader().getNormValues(fieldName);
            }

            @Override
            public String toString() {
                return "Norms";
            }
        };
    }
}
