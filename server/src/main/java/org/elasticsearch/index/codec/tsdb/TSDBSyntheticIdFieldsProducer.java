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
         * Use a doc values skipper to find a starting document ID for the provided _tsid ordinal. The returned document ID might have the
         * exact _tsid ordinal provided, or a lower one.
         *
         * @param tsIdOrd the _tsid ordinal
         * @return a docID to start scanning documents from in order to find the first document ID matching the provided _tsid
         * @throws IOException if any I/O exception occurs
         */
        private int findStartDocIDForTsIdOrd(int tsIdOrd) throws IOException {
            var skipper = docValuesProducer.getSkipper(tsIdFieldInfo);
            assert skipper != null;
            if (skipper.minValue() > tsIdOrd || tsIdOrd > skipper.maxValue()) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            skipper.advance(tsIdOrd, Long.MAX_VALUE);
            return skipper.minDocID(0);
        }

        /**
         * Find the first document that has a _tsid equal or greater than the provided _tsid ordinal, returning its document ID. If no
         * document is found, the method returns {@link DocIdSetIterator#NO_MORE_DOCS}.
         *
         * Warning: This method can be slow because it potentially scans many documents in the segment.
         */
        private int findFirstDocWithTsIdOrdinalEqualOrGreaterThan(int tsIdOrd) throws IOException {
            final int startDocId = findStartDocIDForTsIdOrd(tsIdOrd);
            if (startDocId == DocIdSetIterator.NO_MORE_DOCS) {
                return startDocId;
            }
            // recreate even if doc values are already on the same ordinal, to ensure the method returns the first doc
            if (tsIdDocValues == null || (cachedTsIdOrd != -1 && cachedTsIdOrd >= tsIdOrd) || tsIdDocValues.docID() > startDocId) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
                cachedTsIdOrd = -1;
                cachedTsId = null;
            }
            assert 0 <= tsIdOrd : tsIdOrd;
            assert tsIdOrd < tsIdDocValues.getValueCount() : tsIdOrd;

            for (int docID = startDocId; docID != DocIdSetIterator.NO_MORE_DOCS; docID = tsIdDocValues.nextDoc()) {
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
         * Find the first document that has a _tsid equal to the provided _tsid ordinal, returning its document ID. If no document is found,
         * the method returns {@link DocIdSetIterator#NO_MORE_DOCS}.
         *
         * Warning: This method can be slow because it potentially scans many documents in the segment.
         */
        private int findFirstDocWithTsIdOrdinalEqualTo(int tsIdOrd) throws IOException {
            final int startDocId = findStartDocIDForTsIdOrd(tsIdOrd);
            assert startDocId != DocIdSetIterator.NO_MORE_DOCS : startDocId;

            // recreate even if doc values are already on the same ordinal, to ensure the method returns the first doc
            if (tsIdDocValues == null || (cachedTsIdOrd != -1 && cachedTsIdOrd >= tsIdOrd) || tsIdDocValues.docID() > startDocId) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
                cachedTsIdOrd = -1;
                cachedTsId = null;
            }
            assert 0 <= tsIdOrd : tsIdOrd;
            assert tsIdOrd < tsIdDocValues.getValueCount() : tsIdOrd;

            for (int docID = startDocId; docID != DocIdSetIterator.NO_MORE_DOCS; docID = tsIdDocValues.nextDoc()) {
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

        /**
         * Skip as many documents as possible after a given document ID to find the first document ID matching the timestamp.
         *
         * @param timestamp the timestamp to match
         * @param minDocID the min. document ID
         * @return a docID to start scanning documents from in order to find the first document ID matching the provided timestamp
         * @throws IOException if any I/O exception occurs
         */
        private int skipDocIDForTimestamp(long timestamp, int minDocID) throws IOException {
            var skipper = docValuesProducer.getSkipper(timestampFieldInfo);
            assert skipper != null;
            if (skipper.minValue() > timestamp || timestamp > skipper.maxValue()) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            skipper.advance(minDocID);
            skipper.advance(timestamp, Long.MAX_VALUE);
            return Math.max(minDocID, skipper.minDocID(0));
        }

        private int getTsIdValueCount() throws IOException {
            if (tsIdDocValues == null) {
                tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            }
            return tsIdDocValues.getValueCount();
        }
    }

    /**
     * {@link TermsEnum} to iterate over documents synthetic _ids.
     */
    private class SyntheticIdTermsEnum extends BaseTermsEnum {

        /**
         * Holds all doc values that composed the synthetic _id
         */
        private final DocValuesHolder docValues;

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
            this.docValues = new DocValuesHolder(fieldInfos, docValuesProducer);
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
                    int firstDocID = docValues.findFirstDocWithTsIdOrdinalEqualOrGreaterThan(tsIdOrd);
                    if (firstDocID != DocIdSetIterator.NO_MORE_DOCS) {
                        docID = firstDocID;
                        docTsIdOrd = tsIdOrd;
                        docTimestamp = null;
                        return SeekStatus.NOT_FOUND;
                    }
                }
                // no docs/terms to iterate on
                resetDocID(DocIdSetIterator.NO_MORE_DOCS);
                return SeekStatus.END;
            }

            // Find the first document ID matching the _tsid
            int firstDocID = docValues.findFirstDocWithTsIdOrdinalEqualTo(tsIdOrd);
            assert firstDocID >= 0 : firstDocID;

            if (firstDocID != DocIdSetIterator.NO_MORE_DOCS) {
                // _tsid found, extract the timestamp
                final long timestamp = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(id);

                firstDocID = docValues.skipDocIDForTimestamp(timestamp, firstDocID);
                if (firstDocID != DocIdSetIterator.NO_MORE_DOCS) {
                    int nextDocID = firstDocID;
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
                            // Document is found
                            docID = nextDocID;
                            docTsIdOrd = nextDocTsIdOrd;
                            docTimestamp = nextDocTimestamp;
                            return SeekStatus.FOUND;
                        }
                        // Remaining docs don't match, stop here
                        if (tsIdOrd < nextDocTsIdOrd || nextDocTimestamp < timestamp) {
                            break;
                        }
                    }
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
            return syntheticId(docValues.lookupTsIdOrd(docTsIdOrd), docTimestamp, docValues.docRoutingHash(docID));
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

    private class SyntheticIdPostingsEnum extends PostingsEnum {

        private final DocValuesHolder docValues;

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
            this.docValues = new DocValuesHolder(fieldInfos, docValuesProducer);
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
            int nextDocID = (docID == -1) ? startDocId : docID + 1;
            if (nextDocID < maxDocs) {
                int tsIdOrd = docValues.docTsIdOrdinal(nextDocID);
                if (tsIdOrd == termTsIdOrd) {
                    long timestamp = docValues.docTimestamp(nextDocID);
                    if (timestamp == termTimestamp) {
                        assert Objects.equals(
                            docValues.docRoutingHash(nextDocID),
                            docValues.docRoutingHash((docID == -1) ? startDocId : docID)
                        );
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
