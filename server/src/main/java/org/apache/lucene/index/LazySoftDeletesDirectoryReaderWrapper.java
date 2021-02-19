/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;

/**
 * This is a modified version of {@link SoftDeletesDirectoryReaderWrapper} that materializes the liveDocs
 * bitset lazily. In contrast to {@link SoftDeletesDirectoryReaderWrapper}, this wrapper can only be used
 * for non-NRT readers.
 *
 * This reader filters out documents that have a doc values value in the given field and treat these
 * documents as soft deleted. Hard deleted documents will also be filtered out in the live docs of this reader.
 * @see IndexWriterConfig#setSoftDeletesField(String)
 * @see IndexWriter#softUpdateDocument(Term, Iterable, Field...)
 * @see SoftDeletesRetentionMergePolicy
 */
public final class LazySoftDeletesDirectoryReaderWrapper extends FilterDirectoryReader {
    private final String field;
    private final CacheHelper readerCacheHelper;
    /**
     * Creates a new soft deletes wrapper.
     * @param in the incoming directory reader
     * @param field the soft deletes field
     */
    public LazySoftDeletesDirectoryReaderWrapper(DirectoryReader in, String field) throws IOException {
        this(in, new LazySoftDeletesSubReaderWrapper(Collections.emptyMap(), field));
    }

    private LazySoftDeletesDirectoryReaderWrapper(DirectoryReader in, LazySoftDeletesSubReaderWrapper wrapper) throws IOException {
        super(in, wrapper);
        this.field = wrapper.field;
        readerCacheHelper = in.getReaderCacheHelper() == null ? null : new DelegatingCacheHelper(in.getReaderCacheHelper());
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        Map<CacheKey, LeafReader> readerCache = new HashMap<>();
        for (LeafReader reader : getSequentialSubReaders()) {
            // we try to reuse the live docs instances here if the reader cache key didn't change
            if (reader instanceof LazySoftDeletesFilterCodecReader && reader.getReaderCacheHelper() != null) {
                readerCache.put(((LazySoftDeletesFilterCodecReader) reader).reader.getReaderCacheHelper().getKey(), reader);
            }

        }
        return new LazySoftDeletesDirectoryReaderWrapper(in, new LazySoftDeletesSubReaderWrapper(readerCache, field));
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }

    private static class LazySoftDeletesSubReaderWrapper extends SubReaderWrapper {
        private final Map<CacheKey, LeafReader> mapping;
        private final String field;

        LazySoftDeletesSubReaderWrapper(Map<CacheKey, LeafReader> oldReadersCache, String field) {
            Objects.requireNonNull(field, "Field must not be null");
            assert oldReadersCache != null;
            this.mapping = oldReadersCache;
            this.field = field;
        }

        protected LeafReader[] wrap(List<? extends LeafReader> readers) {
            List<LeafReader> wrapped = new ArrayList<>(readers.size());
            for (LeafReader reader : readers) {
                LeafReader wrap = wrap(reader);
                assert wrap != null;
                if (wrap.numDocs() != 0) {
                    wrapped.add(wrap);
                }
            }
            return wrapped.toArray(new LeafReader[0]);
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            CacheHelper readerCacheHelper = reader.getReaderCacheHelper();
            if (readerCacheHelper != null && mapping.containsKey(readerCacheHelper.getKey())) {
                // if the reader cache helper didn't change and we have it in the cache don't bother creating a new one
                return mapping.get(readerCacheHelper.getKey());
            }
            try {
                return LazySoftDeletesDirectoryReaderWrapper.wrap(reader, field);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static LeafReader wrap(LeafReader reader, String field) throws IOException {
        final SegmentReader segmentReader = Lucene.segmentReader(reader);
        assert segmentReader.isNRT == false : "expected non-NRT reader";
        final SegmentCommitInfo segmentInfo = segmentReader.getSegmentInfo();
        final int numSoftDeletes = segmentInfo.getSoftDelCount();
        if (numSoftDeletes == 0) {
            return reader;
        }
        final DocIdSetIterator iterator = DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(field, reader);
        if (iterator == null) {
            return reader;
        }
        final int maxDoc = reader.maxDoc();
        final int numDocs = maxDoc - segmentInfo.getDelCount() - segmentInfo.getSoftDelCount();
        final Bits lazyBits = new Bits() {

            volatile Bits materializedBits = null;

            @Override
            public boolean get(int index) {
                if (materializedBits == null) {
                    synchronized (this) {
                        try {
                            if (materializedBits == null) {
                                materializedBits = init();
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
                return materializedBits.get(index);
            }

            @Override
            public int length() {
                return maxDoc;
            }

            private Bits init() throws IOException {
                assert Thread.holdsLock(this);

                DocIdSetIterator iterator = DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(field, reader);
                assert iterator != null;
                Bits liveDocs = reader.getLiveDocs();
                final FixedBitSet bits;
                if (liveDocs != null) {
                    bits = FixedBitSet.copyOf(liveDocs);
                } else {
                    bits = new FixedBitSet(maxDoc);
                    bits.set(0, maxDoc);
                }
                int numComputedSoftDeletes = PendingSoftDeletes.applySoftDeletes(iterator, bits);
                assert numComputedSoftDeletes == numSoftDeletes :
                    "numComputedSoftDeletes: " + numComputedSoftDeletes + " expected: " + numSoftDeletes;

                int numDeletes = reader.numDeletedDocs() + numComputedSoftDeletes;
                int computedNumDocs = reader.maxDoc() - numDeletes;
                assert computedNumDocs == numDocs : "computedNumDocs: " + computedNumDocs + " expected: " + numDocs;
                return bits;
            }
        };
        return reader instanceof CodecReader ? new LazySoftDeletesFilterCodecReader((CodecReader) reader, lazyBits, numDocs)
            : new LazySoftDeletesFilterLeafReader(reader, lazyBits, numDocs);
    }

    static final class LazySoftDeletesFilterLeafReader extends FilterLeafReader {
        private final LeafReader reader;
        private final Bits bits;
        private final int numDocs;
        private final CacheHelper readerCacheHelper;

        private LazySoftDeletesFilterLeafReader(LeafReader reader, Bits bits, int numDocs) {
            super(reader);
            this.reader = reader;
            this.bits = bits;
            this.numDocs = numDocs;
            this.readerCacheHelper = reader.getReaderCacheHelper() == null ? null :
                new DelegatingCacheHelper(reader.getReaderCacheHelper());
        }

        @Override
        public Bits getLiveDocs() {
            return bits;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return reader.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return readerCacheHelper;
        }
    }

    static final class LazySoftDeletesFilterCodecReader extends FilterCodecReader {
        private final LeafReader reader;
        private final Bits bits;
        private final int numDocs;
        private final CacheHelper readerCacheHelper;

        private LazySoftDeletesFilterCodecReader(CodecReader reader, Bits bits, int numDocs) {
            super(reader);
            this.reader = reader;
            this.bits = bits;
            this.numDocs = numDocs;
            this.readerCacheHelper = reader.getReaderCacheHelper() == null ? null :
                new DelegatingCacheHelper(reader.getReaderCacheHelper());
        }

        @Override
        public Bits getLiveDocs() {
            return bits;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return reader.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return readerCacheHelper;
        }
    }

    private static class DelegatingCacheHelper implements CacheHelper {
        private final CacheHelper delegate;
        private final CacheKey cacheKey = new CacheKey();

        DelegatingCacheHelper(CacheHelper delegate) {
            this.delegate = delegate;
        }

        @Override
        public CacheKey getKey() {
            return cacheKey;
        }

        @Override
        public void addClosedListener(ClosedListener listener) {
            // here we wrap the listener and call it with our cache key
            // this is important since this key will be used to cache the reader and otherwise we won't free caches etc.
            delegate.addClosedListener(unused -> listener.onClose(cacheKey));
        }
    }
}
