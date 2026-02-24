/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.SYNTHETIC_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;

/**
 * Produces synthetic _id terms that are computed at runtime from the doc values of other fields like _tsid, @timestamp and
 * _ts_routing_hash.
 */
public class TSDBSyntheticIdFieldsProducer extends FieldsProducer {

    private static final Set<String> FIELDS_NAMES = Set.of(SYNTHETIC_ID);

    private final DocValuesProducer docValuesProducer;
    private final FieldInfos fieldInfos;
    private final int maxDocs;

    public TSDBSyntheticIdFieldsProducer(SegmentReadState state, DocValuesProducer docValuesProducer) {
        this(state.fieldInfos, docValuesProducer, state.segmentInfo.maxDoc());
    }

    private TSDBSyntheticIdFieldsProducer(FieldInfos fieldInfos, DocValuesProducer docValuesProducer, int maxDocs) {
        assert assertFieldInfosExist(fieldInfos, SYNTHETIC_ID, TIMESTAMP, TS_ID);
        this.docValuesProducer = Objects.requireNonNull(docValuesProducer);
        this.fieldInfos = fieldInfos;
        this.maxDocs = maxDocs;
    }

    @Override
    public int size() {
        return FIELDS_NAMES.size();
    }

    @Override
    public Iterator<String> iterator() {
        return FIELDS_NAMES.iterator();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(docValuesProducer);
    }

    @Override
    public void checkIntegrity() throws IOException {}

    @Override
    public Terms terms(String field) throws IOException {
        assert FIELDS_NAMES.contains(field) : field;
        return new Terms() {
            @Override
            public TermsEnum iterator() {
                return new SyntheticIdTermsEnum();
            }

            @Override
            public int getDocCount() {
                return maxDocs; // All docs have a synthetic id
            }

            @Override
            public long size() {
                return -1; // Number of terms is unknown
            }

            @Override
            public long getSumTotalTermFreq() {
                return 0;
            }

            @Override
            public long getSumDocFreq() {
                return 0;
            }

            @Override
            public boolean hasFreqs() {
                return false;
            }

            @Override
            public boolean hasOffsets() {
                return false;
            }

            @Override
            public boolean hasPositions() {
                return false;
            }

            @Override
            public boolean hasPayloads() {
                return false;
            }
        };
    }

    /**
     * {@link TermsEnum} to iterate over documents synthetic _ids.
     */
    class SyntheticIdTermsEnum extends BaseTermsEnum {

        /**
         * Holds all doc values that composed the synthetic _id
         */
        private final TSDBSyntheticIdDocValuesHolder docValues;

        /**
         * Current document ID the enum is positioned on. The value is -1 if the terms enum has not been seek ({@link #seekCeil(BytesRef)},
         * {@link #seekExact(BytesRef)}) or step through ({@link #next}). Once the enumeration is exhausted the docID is equal to
         * {@link DocIdSetIterator#NO_MORE_DOCS}.
         */
        private int docID;

        /**
         * Hold the _tsid ordinal and timestamp associated with the current document ID. We keep them around if we needed to fetch them
         * for seeking so that we won't have to fetch them again to build the term.
         */
        private @Nullable Integer docTsIdOrd;
        private @Nullable Long docTimestamp;

        private SyntheticIdTermsEnum() {
            this.docValues = new TSDBSyntheticIdDocValuesHolder(fieldInfos, docValuesProducer);
            resetDocID(-1);
        }

        private void resetDocID(int docID) {
            this.docID = docID;
            this.docTsIdOrd = null;
            this.docTimestamp = null;
        }

        private void ensurePositioned() {
            if (docID == -1 || docID == DocIdSetIterator.NO_MORE_DOCS) {
                assert false;
                throw new IllegalStateException("Method should not be called when unpositioned");
            }
        }

        private boolean assertNoMoreDocs() {
            assert docID == DocIdSetIterator.NO_MORE_DOCS : docID;
            assert docTsIdOrd == null : docTsIdOrd;
            assert docTimestamp == null : docTimestamp;
            return true;
        }

        @Override
        public BytesRef next() throws IOException {
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                assert assertNoMoreDocs();
                return null;
            }

            int nextDocID = docID + 1;
            if (maxDocs <= nextDocID) {
                resetDocID(DocIdSetIterator.NO_MORE_DOCS);
                return null;
            }
            resetDocID(nextDocID);
            return term();
        }

        @Override
        public SeekStatus seekCeil(BytesRef id) throws IOException {

            assert id != null;
            assert Long.BYTES + Integer.BYTES < id.length : id.length;
            if (id == null || id.length <= Long.BYTES + Integer.BYTES) {
                return SeekStatus.NOT_FOUND;
            }

            // Extract the _tsid
            final BytesRef tsId = TsidExtractingIdFieldMapper.extractTimeSeriesIdFromSyntheticId(id);
            int tsIdOrd = docValues.lookupTsIdTerm(tsId);

            // _tsid not found
            if (tsIdOrd < 0) {
                tsIdOrd = -tsIdOrd - 1;
                // set the terms enum on the first non-matching document
                if (tsIdOrd < docValues.getTsIdValueCount()) {
                    int firstDocID = (tsIdOrd == 0) ? 0 : docValues.findFirstDocWithTsIdOrdinalEqualOrGreaterThan(tsIdOrd);
                    assert firstDocID != DocIdSetIterator.NO_MORE_DOCS;
                    docID = firstDocID;
                    docTsIdOrd = tsIdOrd;
                    docTimestamp = null;
                    return SeekStatus.NOT_FOUND;
                }
                // no docs/terms to iterate on
                resetDocID(DocIdSetIterator.NO_MORE_DOCS);
                return SeekStatus.END;
            }

            // Find the first document ID matching the _tsid
            int firstDocID = docValues.findFirstDocWithTsIdOrdinalEqualTo(tsIdOrd);
            assert firstDocID != DocIdSetIterator.NO_MORE_DOCS;
            assert firstDocID >= 0 : firstDocID;

            // Extract the timestamp
            final long timestamp = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(id);

            // Use doc values skipper on timestamp to early exit or skip to the first document matching the timestamp
            var skipper = docValues.docValuesSkipperForTimestamp();
            if (timestamp > skipper.maxValue()) {
                // timestamp is greater than the global maximum value in the segment, so the first docID matching the _tsid is guaranteed to
                // have a smaller timestamp that the one we're looking for and we can early exit at the current docID position. Note that
                // synthetic ids are generated so that the resulting array of bytes has a natural order that reflects the order of docs in
                // the segment (_tsid asc then @timestamp desc). So if timestamp > skipper.maxValue(), it means that the next doc has a
                // @timestamp smaller than what we're looking for.
                docID = firstDocID;
                docTsIdOrd = tsIdOrd;
                docTimestamp = null;
                return SeekStatus.NOT_FOUND;
            }
            if (skipper.minValue() > timestamp) {
                // timestamp is smaller than the global minimum value in the segment, so no docs matching the _tsid will also match the
                // timestamp, so we can early exit at the position of the next _tsid (if there is such one).
                int nextDocTsIdOrd = tsIdOrd + 1;
                if (nextDocTsIdOrd < docValues.getTsIdValueCount()) {
                    docID = docValues.findFirstDocWithTsIdOrdinalEqualTo(nextDocTsIdOrd);
                    docTsIdOrd = nextDocTsIdOrd;
                    docTimestamp = null;
                    return SeekStatus.NOT_FOUND;
                }
                // no docs/terms to iterate on
                resetDocID(DocIdSetIterator.NO_MORE_DOCS);
                return SeekStatus.END;
            }
            skipper.advance(firstDocID);
            skipper.advance(timestamp, Long.MAX_VALUE);

            int nextDocID;
            if (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
                nextDocID = Math.max(firstDocID, skipper.minDocID(0));
            } else {
                // we exhausted the doc values skipper, scan all docs from first doc matching _tsid
                nextDocID = firstDocID;
            }
            int nextDocTsIdOrd = tsIdOrd;
            long nextDocTimestamp;

            // Iterate over documents to find the first one matching the timestamp
            for (; nextDocID < maxDocs; nextDocID++) {
                nextDocTimestamp = docValues.docTimestamp(nextDocID);
                if (firstDocID < nextDocID) {
                    // After the first doc, we need to check again if _tsid matches
                    nextDocTsIdOrd = docValues.docTsIdOrdinal(nextDocID);
                }
                if (nextDocTsIdOrd == tsIdOrd && nextDocTimestamp == timestamp) {
                    if (Assertions.ENABLED) {
                        // _ts_routing_hash read from doc values
                        var routingHashDVBytes = docValues.docRoutingHash(nextDocID);
                        int routingHashDV = TimeSeriesRoutingHashFieldMapper.decode(
                            Uid.decodeId(routingHashDVBytes.bytes, routingHashDVBytes.offset, routingHashDVBytes.length)
                        );
                        // _ts_routing_hash from the synthetic id
                        int routingHashSyntheticId = TsidExtractingIdFieldMapper.extractRoutingHashFromSyntheticId(id);
                        assert Objects.equals(routingHashDV, routingHashSyntheticId) : routingHashDV + "!=" + routingHashSyntheticId;
                    }
                    // Document is found
                    docID = nextDocID;
                    docTsIdOrd = nextDocTsIdOrd;
                    docTimestamp = nextDocTimestamp;
                    return SeekStatus.FOUND;
                }
                // Remaining docs don't match, position the enum on the last doc seeked
                if (tsIdOrd < nextDocTsIdOrd || nextDocTimestamp < timestamp) {
                    docID = nextDocID;
                    docTsIdOrd = nextDocTsIdOrd;
                    docTimestamp = nextDocTimestamp;
                    return SeekStatus.NOT_FOUND;
                }
            }
            resetDocID(DocIdSetIterator.NO_MORE_DOCS);
            return SeekStatus.END;
        }

        @Override
        public BytesRef term() throws IOException {
            ensurePositioned();
            if (docTsIdOrd == null) {
                docTsIdOrd = docValues.docTsIdOrdinal(docID);
            }
            if (docTimestamp == null) {
                docTimestamp = docValues.docTimestamp(docID);
            }
            return docValues.docSyntheticId(docID, docTsIdOrd, docTimestamp);
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            ensurePositioned();
            if (docTsIdOrd == null) {
                docTsIdOrd = docValues.docTsIdOrdinal(docID);
            }
            if (docTimestamp == null) {
                docTimestamp = docValues.docTimestamp(docID);
            }
            return new SyntheticIdPostingsEnum(docID, docTsIdOrd, docTimestamp);
        }

        /**
         * This is an optional method as per the {@link TermsEnum#ord()} documentation that is not supported by the current implementation.
         * This method always throws an {@link UnsupportedOperationException}.
         */
        @Override
        public long ord() {
            throw unsupportedException();
        }

        /**
         * This is an optional method as per the {@link TermsEnum#ord()} documentation that is not supported by the current implementation.
         * This method always throws an {@link UnsupportedOperationException}.
         */
        @Override
        public void seekExact(long ord) {
            throw unsupportedException();
        }

        @Override
        public int docFreq() {
            return 0;
        }

        @Override
        public long totalTermFreq() {
            return 0;
        }

        @Override
        public ImpactsEnum impacts(int flags) {
            return null;
        }
    }

    class SyntheticIdPostingsEnum extends PostingsEnum {

        private final TSDBSyntheticIdDocValuesHolder docValues;

        /**
         * Current synthetic term the postings is pinned on. Only documents matching those _tsid and timestamp values are returned.
         */
        private final int termTsIdOrd;
        private final long termTimestamp;

        /**
         * Document ID of the first document matching the synthetic term.
         */
        private final int startDocId;

        /**
         * Current document ID.
         */
        private int docID = -1;

        private SyntheticIdPostingsEnum(int docID, int termTsIdOrd, long termTimestamp) {
            assert docID < maxDocs : docID + " >= " + maxDocs;
            this.docValues = new TSDBSyntheticIdDocValuesHolder(fieldInfos, docValuesProducer);
            this.termTsIdOrd = termTsIdOrd;
            this.termTimestamp = termTimestamp;
            this.startDocId = docID;
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int nextDoc() throws IOException {
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                return docID;
            }
            if (docID == -1) {
                docID = startDocId;
                return docID;
            }
            int nextDocID = docID + 1;
            if (nextDocID < maxDocs) {
                int tsIdOrd = docValues.docTsIdOrdinal(nextDocID);
                if (tsIdOrd == termTsIdOrd) {
                    long timestamp = docValues.docTimestamp(nextDocID);
                    if (timestamp == termTimestamp) {
                        assert Objects.equals(docValues.docRoutingHash(nextDocID), docValues.docRoutingHash(docID));
                        docID = nextDocID;
                        return docID;
                    }
                }
            }
            docID = DocIdSetIterator.NO_MORE_DOCS;
            return docID;
        }

        @Override
        public int advance(int target) throws IOException {
            return slowAdvance(target);
        }

        @Override
        public long cost() {
            return 0L;
        }

        @Override
        public int freq() throws IOException {
            return 0; // not supported
        }

        @Override
        public int nextPosition() throws IOException {
            return -1; // not supported
        }

        @Override
        public int startOffset() throws IOException {
            return -1; // not supported
        }

        @Override
        public int endOffset() throws IOException {
            return -1; // not supported
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return null; // not supported
        }
    }

    private static boolean assertFieldInfosExist(FieldInfos fieldInfos, String... fieldNames) {
        assert fieldNames != null && fieldNames.length > 0 : "fieldNames should be > 0";
        for (var fieldName : fieldNames) {
            assert fieldInfos.fieldInfo(fieldName) != null : "field [" + fieldName + "] not found";
        }
        return true;
    }

    private static UnsupportedOperationException unsupportedException() {
        var error = "This method should not be called on this enum";
        return new UnsupportedOperationException(error);
    }
}
