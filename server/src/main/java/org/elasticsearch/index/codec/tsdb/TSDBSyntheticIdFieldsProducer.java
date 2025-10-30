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
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.SYNTHETIC_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;

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
                return new FakeTermsEnum();
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
     * This is a fake TermsEnum that scans all documents for find docs matching a specific _id. This implementation is only here to show
     * that the synthetic _id terms is used when applying doc values updates during soft-updates. It is buggy and should not be used besides
     * some carefully crafted integration tests, because it relies on the current _id format for TSDB indices that has limitations:
     * - it is composed of a routing hash, a @timestamp and a tsid that cannot be un-hashed so all docs must be scanned to find matchings
     * - it is not sorted on _id in the Lucene segments so doc values updates stop too early when applying DV updates
     *
     * This fake terms enumeration will be changed to support a different _id format in a short future.
     */
    private class FakeTermsEnum extends BaseTermsEnum {

        private BytesRef term = null;
        private int docID = -1;

        private BytesRef latestTsId = null;
        private long latestTimestamp = -1L;

        private FakeTermsEnum() {}

        @Override
        public BytesRef next() throws IOException {
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                assert term == null;
                return null;
            }
            docID += 1;
            if (maxDocs <= docID) {
                docID = DocIdSetIterator.NO_MORE_DOCS;
                latestTimestamp = -1L;
                latestTsId = null;
                term = null;
                return null;
            }

            // Retrieve _tsid
            SortedDocValues tsIdDocValues = docValuesProducer.getSorted(fieldInfos.fieldInfo(TS_ID));
            boolean found = tsIdDocValues.advanceExact(docID);
            assert found;
            int tsIdOrd = tsIdDocValues.ordValue();
            BytesRef tsId = tsIdDocValues.lookupOrd(tsIdOrd);
            assert tsId != null;

            // Retrieve timestamp
            SortedNumericDocValues timestampDocValues = docValuesProducer.getSortedNumeric(fieldInfos.fieldInfo(TIMESTAMP));
            found = timestampDocValues.advanceExact(docID);
            assert found;
            assert timestampDocValues.docValueCount() == 1;
            long timestamp = timestampDocValues.nextValue();

            // Retrieve routing hash
            var tsRoutingHash = fieldInfos.fieldInfo(TimeSeriesRoutingHashFieldMapper.NAME);
            assert tsRoutingHash != null;
            SortedDocValues routingHashDocValues = docValuesProducer.getSorted(tsRoutingHash);
            found = routingHashDocValues.advanceExact(docID);
            assert found;
            BytesRef routingHashBytes = routingHashDocValues.lookupOrd(routingHashDocValues.ordValue());

            int routingHash = TimeSeriesRoutingHashFieldMapper.decode(
                Uid.decodeId(routingHashBytes.bytes, routingHashBytes.offset, routingHashBytes.length)
            );
            term = Uid.encodeId(TsidExtractingIdFieldMapper.createId(routingHash, tsId, timestamp));
            latestTimestamp = timestamp;
            latestTsId = tsId;
            return term;
        }

        @Override
        public SeekStatus seekCeil(BytesRef id) {
            assert id != null;
            if (term != null && term.equals(id)) {
                return SeekStatus.FOUND;
            }
            try {
                while (next() != null) {
                    if (term.equals(id)) {
                        return SeekStatus.FOUND;
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return SeekStatus.END;
        }

        @Override
        public BytesRef term() {
            return term;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) {
            return new FakePostingsEnum(docID, latestTsId, latestTimestamp, maxDocs);
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

    /**
     * Do not use in production. See {@link FakeTermsEnum}.
     */
    private class FakePostingsEnum extends PostingsEnum {

        private final int startDocID;
        private final BytesRef latestTsId;
        private final long latestTimestamp;
        private final int maxDocs;
        private int docID;

        private FakePostingsEnum(int docID, BytesRef latestTsId, long latestTimestamp, int maxDocs) {
            this.startDocID = docID;
            this.latestTsId = latestTsId;
            this.latestTimestamp = latestTimestamp;
            this.maxDocs = maxDocs;
            this.docID = -1;
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int nextDoc() throws IOException {
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                return docID;
            } else if (docID == -1) {
                docID = startDocID;
            } else {
                docID = docID + 1;
                if (maxDocs <= docID) {
                    docID = DocIdSetIterator.NO_MORE_DOCS;
                    return docID;
                }
            }

            // Retrieve _tsid
            SortedDocValues tsIdDocValues = docValuesProducer.getSorted(fieldInfos.fieldInfo(TS_ID));
            boolean found = tsIdDocValues.advanceExact(docID);
            assert found;
            int tsIdOrd = tsIdDocValues.ordValue();
            BytesRef tsId = tsIdDocValues.lookupOrd(tsIdOrd);
            assert tsId != null;

            if (latestTsId != null && latestTsId.equals(tsId) == false) {
                // Different _tsid, stop here
                docID = DocIdSetIterator.NO_MORE_DOCS;
                return docID;
            }

            // Retrieve timestamp
            SortedNumericDocValues timestampDocValues = docValuesProducer.getSortedNumeric(fieldInfos.fieldInfo(TIMESTAMP));
            found = timestampDocValues.advanceExact(docID);
            assert found;
            assert timestampDocValues.docValueCount() == 1;
            long timestamp = timestampDocValues.nextValue();

            if (latestTimestamp != -1L && latestTimestamp != timestamp) {
                // Different @timestamp, stop here
                docID = DocIdSetIterator.NO_MORE_DOCS;
                return docID;
            }

            // Retrieve routing hash
            var tsRoutingHash = fieldInfos.fieldInfo(TimeSeriesRoutingHashFieldMapper.NAME);
            assert tsRoutingHash != null;
            SortedDocValues routingHashDocValues = docValuesProducer.getSorted(tsRoutingHash);
            found = routingHashDocValues.advanceExact(docID);
            assert found;
            BytesRef routingHashBytes = routingHashDocValues.lookupOrd(routingHashDocValues.ordValue());
            assert routingHashBytes != null;
            return docID;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc;
            while ((doc = nextDoc()) < target) {
                // Continue
            }
            return doc;
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
        assert false : error;
        return new UnsupportedOperationException(error);
    }
}
