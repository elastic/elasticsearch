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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
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
     * Holds all the doc values used in the {@link TermsEnum} and {@link PostingsEnum} to lookup and to build synthetic _ids, along with
     * some utility methods to access doc values.
     * <p>
     *     It holds the instance of {@link DocValuesProducer} used to create the sorted doc values for _tsid, @timestamp and
     *     _ts_routing_hash. Because doc values can only advance, they are re-created from the {@link DocValuesProducer} when we need to
     *     seek backward.
     * </p>
     */
    private static class DocValuesHolder {

        private final FieldInfo tsIdFieldInfo;
        private final FieldInfo timestampFieldInfo;
        private final FieldInfo routingHashFieldInfo;
        private final DocValuesProducer docValuesProducer;

        private SortedNumericDocValues timestampDocValues; // sorted desc. order
        private SortedDocValues routingHashDocValues; // sorted asc. order
        private SortedDocValues tsIdDocValues; // sorted asc. order
        // Keep around the latest tsId ordinal and value
        private int cachedTsIdOrd = -1;
        private BytesRef cachedTsId;

        private DocValuesHolder(FieldInfos fieldInfos, DocValuesProducer docValuesProducer) {
            this.tsIdFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TS_ID);
            this.timestampFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TIMESTAMP);
            this.routingHashFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TS_ROUTING_HASH);
            this.docValuesProducer = docValuesProducer;
        }

        private FieldInfo safeFieldInfo(FieldInfos fieldInfos, String fieldName) {
            var fi = fieldInfos.fieldInfo(fieldName);
            if (fi == null) {
                var message = "Field [" + fieldName + "] does not exist";
                assert false : message;
                throw new IllegalArgumentException(message);
            }
            return fi;
        }

        /**
         * Returns the _tsid ordinal value for a given docID. The document ID must exist and must have a value for the field.
         *
         * @param docID the docID
         * @return the _tsid ordinal value
         * @throws IOException if any I/O exception occurs
         */
        private int docTsIdOrdinal(int docID) throws IOException {
            if (tsIdDocValues == null || tsIdDocValues.docID() > docID) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
                cachedTsIdOrd = -1;
                cachedTsId = null;
            }
            boolean found = tsIdDocValues.advanceExact(docID);
            assert found : "No value found for field [" + tsIdFieldInfo.getName() + " and docID " + docID;
            return tsIdDocValues.ordValue();
        }

        /**
         * Returns the timestamp value for a given docID. The document ID must exist and must have a value for the field.
         *
         * @param docID the docID
         * @return the timestamp value
         * @throws IOException if any I/O exception occurs
         */
        private long docTimestamp(int docID) throws IOException {
            if (timestampDocValues == null || timestampDocValues.docID() > docID) {
                timestampDocValues = docValuesProducer.getSortedNumeric(timestampFieldInfo);
            }
            boolean found = timestampDocValues.advanceExact(docID);
            assert found : "No value found for field [" + timestampFieldInfo.getName() + " and docID " + docID;
            assert timestampDocValues.docValueCount() == 1;
            return timestampDocValues.nextValue();
        }

        /**
         * Returns the routing hash value for a given docID. The document ID must exist and must have a value for the field.
         *
         * @param docID the docID
         * @return the routing hash value
         * @throws IOException if any I/O exception occurs
         */
        private BytesRef docRoutingHash(int docID) throws IOException {
            if (routingHashDocValues == null || routingHashDocValues.docID() > docID) {
                routingHashDocValues = docValuesProducer.getSorted(routingHashFieldInfo);
            }
            boolean found = routingHashDocValues.advanceExact(docID);
            assert found : "No value found for field [" + routingHashFieldInfo.getName() + " and docID " + docID;
            return routingHashDocValues.lookupOrd(routingHashDocValues.ordValue());
        }

        /**
         * Lookup if a given _tsid exists, returning a positive ordinal if it exists otherwise it returns -insertionPoint-1.
         *
         * @param tsId the _tsid to look up
         * @return a positive ordinal if the _tsid exists, else returns -insertionPoint-1.
         * @throws IOException if any I/O exception occurs
         */
        private int lookupTsIdTerm(BytesRef tsId) throws IOException {
            int compare = Integer.MAX_VALUE;
            if (cachedTsId != null) {
                compare = cachedTsId.compareTo(tsId);
                if (compare == 0) {
                    return cachedTsIdOrd;
                }
            }
            if (tsIdDocValues == null || compare > 0) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
                cachedTsIdOrd = -1;
                cachedTsId = null;
            }
            int ordinal = tsIdDocValues.lookupTerm(tsId);
            if (0 <= ordinal) {
                cachedTsIdOrd = ordinal;
                cachedTsId = tsId;
            }
            return ordinal;
        }

        /**
         * Lookup the _tsid value for the given ordinal.
         *
         * @param tsIdOrdinal the _tsid  ordinal
         * @return the _tsid value
         * @throws IOException if any I/O exception occurs
         */
        private BytesRef lookupTsIdOrd(int tsIdOrdinal) throws IOException {
            if (cachedTsIdOrd != -1 && cachedTsIdOrd == tsIdOrdinal) {
                return cachedTsId;
            }
            if (tsIdDocValues == null || tsIdDocValues.ordValue() > tsIdOrdinal) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
                cachedTsIdOrd = -1;
                cachedTsId = null;
            }
            assert 0 <= tsIdOrdinal : tsIdOrdinal;
            assert tsIdOrdinal < tsIdDocValues.getValueCount() : tsIdOrdinal;
            var tsId = tsIdDocValues.lookupOrd(tsIdOrdinal);
            if (tsId != null) {
                cachedTsIdOrd = tsIdOrdinal;
                cachedTsId = tsId;
            }
            return tsId;
        }

        /**
         * Scan all documents to find the first document that has a _tsid equal or greater than the provided _tsid ordinal, returning its
         * document ID. If no document is found, the method returns {@link DocIdSetIterator#NO_MORE_DOCS}.
         *
         * Warning: This method is very slow because it potentially scans all documents in the segment.
         */
        private int slowScanToFirstDocWithTsIdOrdinalEqualOrGreaterThan(int tsIdOrd) throws IOException {
            // recreate even if doc values are already on the same ordinal, to ensure the method returns the first doc
            if (tsIdDocValues == null || (cachedTsIdOrd != -1 && cachedTsIdOrd >= tsIdOrd)) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
                cachedTsIdOrd = -1;
                cachedTsId = null;
            }
            assert 0 <= tsIdOrd : tsIdOrd;
            assert tsIdOrd < tsIdDocValues.getValueCount() : tsIdOrd;

            for (int docID = 0; docID != DocIdSetIterator.NO_MORE_DOCS; docID = tsIdDocValues.nextDoc()) {
                boolean found = tsIdDocValues.advanceExact(docID);
                assert found : "No value found for field [" + tsIdFieldInfo.getName() + " and docID " + docID;
                var ord = tsIdDocValues.ordValue();
                if (ord == tsIdOrd || tsIdOrd < ord) {
                    if (ord != cachedTsIdOrd) {
                        cachedTsId = tsIdDocValues.lookupOrd(ord);
                        cachedTsIdOrd = ord;
                    }
                    return docID;
                }
            }
            cachedTsIdOrd = -1;
            cachedTsId = null;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        /**
         * Scan all documents to find the first document that has a _tsid equal to the provided _tsid ordinal, returning its
         * document ID. If no document is found, the method returns {@link DocIdSetIterator#NO_MORE_DOCS}.
         *
         * Warning: This method is very slow because it potentially scans all documents in the segment.
         */
        private int slowScanToFirstDocWithTsIdOrdinalEqualTo(int tsIdOrd) throws IOException {
            // recreate even if doc values are already on the same ordinal, to ensure the method returns the first doc
            if (tsIdDocValues == null || (cachedTsIdOrd != -1 && cachedTsIdOrd >= tsIdOrd)) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
                cachedTsIdOrd = -1;
                cachedTsId = null;
            }
            assert 0 <= tsIdOrd : tsIdOrd;
            assert tsIdOrd < tsIdDocValues.getValueCount() : tsIdOrd;

            for (int docID = 0; docID != DocIdSetIterator.NO_MORE_DOCS; docID = tsIdDocValues.nextDoc()) {
                boolean found = tsIdDocValues.advanceExact(docID);
                assert found : "No value found for field [" + tsIdFieldInfo.getName() + " and docID " + docID;
                var ord = tsIdDocValues.ordValue();
                if (ord == tsIdOrd) {
                    if (ord != cachedTsIdOrd) {
                        cachedTsId = tsIdDocValues.lookupOrd(ord);
                        cachedTsIdOrd = ord;
                    }
                    return docID;
                } else if (tsIdOrd < ord) {
                    break;
                }
            }
            cachedTsIdOrd = -1;
            cachedTsId = null;
            assert false : "Method must be called with an existing _tsid ordinal: " + tsIdOrd;
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        private int getTsIdValueCount() throws IOException {
            if (tsIdDocValues == null) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            }
            return tsIdDocValues.getValueCount();
        }
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
    private class SyntheticIdTermsEnum extends BaseTermsEnum {

        /**
         * Holds all doc values that composed the synthetic _id
         */
        private final DocValuesHolder docValues;

        /**
         * Current synthetic term the enum is positioned on. It points to 1 document.
         */
        private SyntheticTerm current;

        private SyntheticIdTermsEnum() {
            this.docValues = new DocValuesHolder(fieldInfos, docValuesProducer);
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
                    int docID = docValues.slowScanToFirstDocWithTsIdOrdinalEqualOrGreaterThan(tsIdOrd);
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

            // _tsid found, extract the timestamp
            final long timestamp = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(id);

            // Slow scan to the first document matching the _tsid
            final int startDocID = docValues.slowScanToFirstDocWithTsIdOrdinalEqualTo(tsIdOrd);
            assert startDocID >= 0 : startDocID;

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

    private class SyntheticIdPostingsEnum extends PostingsEnum {

        private final DocValuesHolder docValues;

        /**
         * Current synthetic term the postings is pinned on.
         */
        private final SyntheticTerm term;
        private int docID = -1;

        private SyntheticIdPostingsEnum(SyntheticTerm term) {
            this.docValues = new DocValuesHolder(fieldInfos, docValuesProducer);
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
