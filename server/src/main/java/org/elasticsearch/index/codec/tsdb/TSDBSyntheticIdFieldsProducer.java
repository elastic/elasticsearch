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
import org.elasticsearch.core.IOUtils;
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
    public FieldsProducer getMergeInstance() {
        return new TSDBSyntheticIdFieldsProducer(fieldInfos, docValuesProducer, maxDocs);
    }

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
                return maxDocs - 1; // All docs have a synthetic id
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
     * Represents the synthetic term the {@link TermsEnum} or {@link PostingsEnum} is positioned on. It points to a given docID and its
     * corresponding _tsid, @timestamp and _ts_routing_hash values. The {@link #term()} method returns the synthetic _id of the document.
     */
    private record SyntheticTerm(int docID, int tsIdOrd, BytesRef tsId, long timestamp, BytesRef routingHash) {
        private BytesRef term() {
            assert docID >= 0 : docID;
            assert tsIdOrd >= 0 : tsIdOrd;
            return syntheticId(tsId, timestamp, routingHash);
        }
    }

    /**
     * When returned by next(), seekCeil(), nextDoc() and docID() it means there are no more synthetic terms in the {@link TermsEnum}
     * or {@link PostingsEnum}.
     */
    private static final SyntheticTerm NO_MORE_DOCS = new SyntheticTerm(DocIdSetIterator.NO_MORE_DOCS, -1, null, -1L, null);

    /**
     * {@link TermsEnum} to iterate over documents synthetic _ids.
     */
    class SyntheticIdTermsEnum extends BaseTermsEnum {

        /**
         * Holds all doc values that composed the synthetic _id
         */
        private final TSDBSyntheticIdDocValuesHolder docValues;

        /**
         * Current synthetic term the enum is positioned on. It points to 1 document.
         */
        private SyntheticTerm current;

        private SyntheticIdTermsEnum() {
            this.docValues = new TSDBSyntheticIdDocValuesHolder(fieldInfos, docValuesProducer);
            this.current = null;
        }

        private void ensurePositioned() {
            if (current == null || current == NO_MORE_DOCS) {
                assert false;
                throw new IllegalStateException("Method should not be called when unpositioned");
            }
        }

        @Override
        public BytesRef next() throws IOException {
            if (current == NO_MORE_DOCS) {
                return null;
            }

            int docID = (current != null) ? current.docID + 1 : 0;
            if (maxDocs <= docID) {
                current = NO_MORE_DOCS;
                return null;
            }
            int tsIdOrdinal = docValues.docTsIdOrdinal(docID);
            current = new SyntheticTerm(
                docID,
                tsIdOrdinal,
                docValues.lookupTsIdOrd(tsIdOrdinal),
                docValues.docTimestamp(docID),
                docValues.docRoutingHash(docID)
            );
            return current.term();
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
                    int docID = docValues.findFirstDocWithTsIdOrdinalEqualOrGreaterThan(tsIdOrd);
                    if (docID != DocIdSetIterator.NO_MORE_DOCS) {
                        current = new SyntheticTerm(
                            docID,
                            tsIdOrd,
                            docValues.lookupTsIdOrd(tsIdOrd),
                            docValues.docTimestamp(docID),
                            docValues.docRoutingHash(docID)
                        );
                        return SeekStatus.NOT_FOUND;
                    }
                }
                // no docs/terms to iterate on
                current = NO_MORE_DOCS;
                return SeekStatus.END;
            }

            // Find the first document ID matching the _tsid
            int startDocID = docValues.findFirstDocWithTsIdOrdinalEqualTo(tsIdOrd);
            assert startDocID >= 0 : startDocID;

            if (startDocID != DocIdSetIterator.NO_MORE_DOCS) {
                // _tsid found, extract the timestamp
                final long timestamp = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(id);

                startDocID = docValues.skipDocIDForTimestamp(timestamp, startDocID);
                if (startDocID != DocIdSetIterator.NO_MORE_DOCS) {
                    int docID = startDocID;
                    int docTsIdOrd = tsIdOrd;
                    long docTimestamp;

                    // Iterate over documents to find the first one matching the timestamp
                    for (; docID < maxDocs; docID++) {
                        docTimestamp = docValues.docTimestamp(docID);
                        if (startDocID < docID) {
                            // After the first doc, we need to check again if _tsid matches
                            docTsIdOrd = docValues.docTsIdOrdinal(docID);
                        }
                        if (docTsIdOrd == tsIdOrd && docTimestamp == timestamp) {
                            // It's a match!
                            current = new SyntheticTerm(docID, tsIdOrd, tsId, docTimestamp, docValues.docRoutingHash(docID));
                            return SeekStatus.FOUND;
                        }
                        // Remaining docs don't match, stop here
                        if (tsIdOrd < docTsIdOrd || docTimestamp < timestamp) {
                            break;
                        }
                    }
                }
            }
            current = NO_MORE_DOCS;
            return SeekStatus.END;
        }

        @Override
        public BytesRef term() {
            ensurePositioned();
            return current.term();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) {
            ensurePositioned();
            return new SyntheticIdPostingsEnum(current);
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
        public int docFreq() throws IOException {
            return 0;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return 0;
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            return null;
        }
    }

    class SyntheticIdPostingsEnum extends PostingsEnum {

        private final TSDBSyntheticIdDocValuesHolder docValues;

        /**
         * Current synthetic term the postings is pinned on.
         */
        private final SyntheticTerm term;
        private int docID = -1;

        private SyntheticIdPostingsEnum(SyntheticTerm term) {
            this.docValues = new TSDBSyntheticIdDocValuesHolder(fieldInfos, docValuesProducer);
            this.term = Objects.requireNonNull(term);
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
            int nextDocID = (docID == -1) ? term.docID() : docID + 1;
            if (nextDocID < maxDocs) {
                int tsIdOrd = docValues.docTsIdOrdinal(nextDocID);
                if (tsIdOrd == term.tsIdOrd()) {
                    long timestamp = docValues.docTimestamp(nextDocID);
                    if (timestamp == term.timestamp()) {
                        assert Objects.equals(docValues.docRoutingHash(nextDocID), term.routingHash());
                        assert Objects.equals(docValues.lookupTsIdOrd(tsIdOrd), term.tsId());
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

    private static BytesRef syntheticId(BytesRef tsId, long timestamp, BytesRef routingHashBytes) {
        assert tsId != null;
        assert timestamp > 0L;
        assert routingHashBytes != null;
        String routingHashString = Uid.decodeId(routingHashBytes.bytes, routingHashBytes.offset, routingHashBytes.length);
        int routingHash = TimeSeriesRoutingHashFieldMapper.decode(routingHashString);
        return TsidExtractingIdFieldMapper.createSyntheticIdBytesRef(tsId, timestamp, routingHash);
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
        assert false : error;
        return new UnsupportedOperationException(error);
    }
}
