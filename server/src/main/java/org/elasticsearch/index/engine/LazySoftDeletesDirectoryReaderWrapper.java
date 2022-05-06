/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
    private final CacheHelper readerCacheHelper;

    /**
     * Creates a new soft deletes wrapper.
     * @param in the incoming directory reader
     * @param field the soft deletes field
     */
    public LazySoftDeletesDirectoryReaderWrapper(DirectoryReader in, String field) throws IOException {
        super(in, new LazySoftDeletesSubReaderWrapper(field));
        readerCacheHelper = in.getReaderCacheHelper() == null ? null : new DelegatingCacheHelper(in.getReaderCacheHelper());
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }

    private static class LazySoftDeletesSubReaderWrapper extends SubReaderWrapper {
        private final String field;

        LazySoftDeletesSubReaderWrapper(String field) {
            Objects.requireNonNull(field, "Field must not be null");
            this.field = field;
        }

        @Override
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
            return LazySoftDeletesDirectoryReaderWrapper.wrap(reader, field);
        }
    }

    static LeafReader wrap(LeafReader reader, String field) {
        final SegmentReader segmentReader = Lucene.segmentReader(reader);
        final SegmentCommitInfo segmentInfo = segmentReader.getSegmentInfo();
        final int numSoftDeletes = segmentInfo.getSoftDelCount();
        if (numSoftDeletes == 0) {
            return reader;
        }
        final int maxDoc = reader.maxDoc();
        final int numDocs = maxDoc - segmentInfo.getDelCount() - segmentInfo.getSoftDelCount();
        final LazyBits lazyBits = new LazyBits(maxDoc, field, reader, numSoftDeletes, numDocs);
        return reader instanceof CodecReader
            ? new LazySoftDeletesFilterCodecReader((CodecReader) reader, lazyBits, numDocs)
            : new LazySoftDeletesFilterLeafReader(reader, lazyBits, numDocs);
    }

    public static final class LazySoftDeletesFilterLeafReader extends FilterLeafReader {
        private final LeafReader reader;
        private final LazyBits bits;
        private final int numDocs;
        private final CacheHelper readerCacheHelper;

        public LazySoftDeletesFilterLeafReader(LeafReader reader, LazyBits bits, int numDocs) {
            super(reader);
            this.reader = reader;
            this.bits = bits;
            this.numDocs = numDocs;
            this.readerCacheHelper = reader.getReaderCacheHelper() == null
                ? null
                : new DelegatingCacheHelper(reader.getReaderCacheHelper());
        }

        @Override
        public LazyBits getLiveDocs() {
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

    public static final class LazySoftDeletesFilterCodecReader extends FilterCodecReader {
        private final LeafReader reader;
        private final LazyBits bits;
        private final int numDocs;
        private final CacheHelper readerCacheHelper;

        public LazySoftDeletesFilterCodecReader(CodecReader reader, LazyBits bits, int numDocs) {
            super(reader);
            this.reader = reader;
            this.bits = bits;
            this.numDocs = numDocs;
            this.readerCacheHelper = reader.getReaderCacheHelper() == null
                ? null
                : new DelegatingCacheHelper(reader.getReaderCacheHelper());
        }

        @Override
        public LazyBits getLiveDocs() {
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

    public static class LazyBits implements Bits {

        private final int maxDoc;
        private final String field;
        private final LeafReader reader;
        private final int numSoftDeletes;
        private final int numDocs;
        volatile Bits materializedBits;

        public LazyBits(int maxDoc, String field, LeafReader reader, int numSoftDeletes, int numDocs) {
            this.maxDoc = maxDoc;
            this.field = field;
            this.reader = reader;
            this.numSoftDeletes = numSoftDeletes;
            this.numDocs = numDocs;
            materializedBits = null;
            assert numSoftDeletes > 0;
        }

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

            DocIdSetIterator iterator = getDocValuesDocIdSetIterator(field, reader);
            assert iterator != null;
            Bits liveDocs = reader.getLiveDocs();
            final FixedBitSet bits;
            if (liveDocs != null) {
                bits = FixedBitSet.copyOf(liveDocs);
            } else {
                bits = new FixedBitSet(maxDoc);
                bits.set(0, maxDoc);
            }
            int numComputedSoftDeletes = applySoftDeletes(iterator, bits);
            assert numComputedSoftDeletes == numSoftDeletes
                : "numComputedSoftDeletes: " + numComputedSoftDeletes + " expected: " + numSoftDeletes;

            int numDeletes = reader.numDeletedDocs() + numComputedSoftDeletes;
            int computedNumDocs = reader.maxDoc() - numDeletes;
            assert computedNumDocs == numDocs : "computedNumDocs: " + computedNumDocs + " expected: " + numDocs;
            return bits;
        }

        public boolean initialized() {
            return materializedBits != null;
        }
    }

    static int applySoftDeletes(DocIdSetIterator iterator, FixedBitSet bits) throws IOException {
        assert iterator != null;
        int newDeletes = 0;
        int docID;

        while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (bits.get(docID)) { // doc is live - clear it
                bits.clear(docID);
                newDeletes++;
                // now that we know we deleted it and we fully control the hard deletes we can do correct
                // accounting
                // below.
            }
        }
        return newDeletes;
    }

    private static class DelegatingCacheHelper extends org.apache.lucene.index.FilterDirectoryReader.DelegatingCacheHelper {
        DelegatingCacheHelper(CacheHelper delegate) {
            super(delegate);
        }
    }

    /**
     * Returns a {@link DocIdSetIterator} from the given field or null if the field doesn't exist in
     * the reader or if the reader has no doc values for the field.
     */
    private static DocIdSetIterator getDocValuesDocIdSetIterator(String field, LeafReader reader) throws IOException {
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        final DocIdSetIterator iterator;
        if (fieldInfo != null) {
            switch (fieldInfo.getDocValuesType()) {
                case NONE:
                    iterator = null;
                    break;
                case NUMERIC:
                    iterator = reader.getNumericDocValues(field);
                    break;
                case BINARY:
                    iterator = reader.getBinaryDocValues(field);
                    break;
                case SORTED:
                    iterator = reader.getSortedDocValues(field);
                    break;
                case SORTED_NUMERIC:
                    iterator = reader.getSortedNumericDocValues(field);
                    break;
                case SORTED_SET:
                    iterator = reader.getSortedSetDocValues(field);
                    break;
                default:
                    throw new AssertionError();
            }
            return iterator;
        }
        return null;
    }
}
