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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;

/**
 * Holds all the doc values used in the {@link TSDBSyntheticIdFieldsProducer.SyntheticIdTermsEnum},
 * {@link TSDBSyntheticIdFieldsProducer.SyntheticIdPostingsEnum} and {@link TSDBSyntheticIdStoredFieldsReader} to lookup and to build
 * synthetic _ids, along with some utility methods to access doc values.
 * <p>
 * It holds the instance of {@link DocValuesProducer} used to create the sorted doc values for _tsid, @timestamp and
 * _ts_routing_hash. Because doc values can only advance, they are re-created from the {@link DocValuesProducer} when we need to
 * seek backward.
 * </p>
 */
class TSDBSyntheticIdDocValuesHolder {

    private final FieldInfo tsIdFieldInfo;
    private final FieldInfo timestampFieldInfo;
    private final FieldInfo routingHashFieldInfo;
    private final @Nullable FieldInfo tombstoneFieldInfo;
    private final @Nullable FieldInfo softDeletesFieldInfo;
    private final DocValuesProducer docValuesProducer;
    private final int maxDocs;
    private final boolean hasTsIdSkipper;
    private final boolean hasTimestampSkipper;
    /**
     * Whether _tsid is the primary index sort field, so that _tsid ordinals ascend with document ids. Otherwise (the grouped time
     * series sort) the documents of a _tsid are still contiguous but the runs are not in ordinal order, and their position comes
     * from the {@link OrdinalRunsDocValues.OrdinalRuns} of the segment.
     */
    private final boolean tsIdPrimarySorted;
    /**
     * The index sort field ahead of every other one, or {@code null} if _tsid is the primary sort field or the segment has no
     * documents with a value for it. See {@link #liveRunsEnd()}.
     */
    private final @Nullable FieldInfo primarySortFieldInfo;

    private SortedNumericDocValues timestampDocValues; // sorted desc. order
    private SortedDocValues routingHashDocValues; // sorted asc. order
    private SortedDocValues tsIdDocValues; // sorted asc. order
    private NumericDocValues tombstoneDocValues;
    private NumericDocValues softDeletesDocValues;
    // Resolved once; null when the layout only supports iteration.
    private RandomAccessNumericValues randomAccessTimestamps;
    private boolean randomAccessTimestampsResolved;
    private RandomAccessNumericValues randomAccessTsIdOrds;
    private boolean randomAccessTsIdOrdsResolved;
    // Resolved once; null when _tsid is the primary sort field or the segment has no _tsid values.
    private OrdinalRunsDocValues.OrdinalRuns tsIdRuns;
    private boolean tsIdRunsResolved;
    // Resolved once, see #liveRunsEnd()
    private int liveRunsEnd = -1;
    // tsids in the same segment have the same length
    private int tsidFixedLength = -1;
    // Keep around the latest tsId ordinal and value
    private int cachedTsIdOrd = -1;
    private BytesRef cachedTsId;

    TSDBSyntheticIdDocValuesHolder(SegmentInfo segmentInfo, FieldInfos fieldInfos, DocValuesProducer docValuesProducer) {
        this.maxDocs = segmentInfo.maxDoc();
        this.tsIdPrimarySorted = tsIdIsPrimarySortField(segmentInfo);
        this.primarySortFieldInfo = tsIdPrimarySorted ? null : fieldInfos.fieldInfo(segmentInfo.getIndexSort().getSort()[0].getField());
        this.tsIdFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TS_ID);
        this.timestampFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TIMESTAMP);
        this.routingHashFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TS_ROUTING_HASH);
        this.tombstoneFieldInfo = fieldInfos.fieldInfo(SeqNoFieldMapper.TOMBSTONE_NAME);
        this.softDeletesFieldInfo = fieldInfos.fieldInfo(Lucene.SOFT_DELETES_FIELD);
        this.docValuesProducer = docValuesProducer;
        this.hasTsIdSkipper = tsIdFieldInfo.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE;
        this.hasTimestampSkipper = timestampFieldInfo.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE;
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
     * Whether the segment is sorted on _tsid first (the time series sort), as opposed to the grouped time series sort that puts the
     * metric name hash and the dimension group ahead of it. A segment without an index sort is treated as tsid-first, which is what
     * tests that index synthetic id documents by hand rely on.
     */
    static boolean tsIdIsPrimarySortField(SegmentInfo segmentInfo) {
        var indexSort = segmentInfo.getIndexSort();
        return indexSort == null
            || indexSort.getSort().length == 0
            || TSDBSyntheticIdPostingsFormat.TS_ID.equals(indexSort.getSort()[0].getField());
    }

    /**
     * Returns the runs of documents of each _tsid ordinal, or {@code null} when _tsid is the primary sort field (the ordinals then
     * ascend with the document ids and the runs are not needed) or the segment has no _tsid values. Throws if the segment is not
     * sorted on _tsid first and holds no runs: its documents cannot be located by _tsid then, and synthetic ids can't work.
     */
    @Nullable
    OrdinalRunsDocValues.OrdinalRuns tsIdRuns() throws IOException {
        if (tsIdPrimarySorted) {
            return null;
        }
        if (tsIdRunsResolved == false) {
            tsIdRunsResolved = true;
            var sortedDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            if (sortedDocValues.getValueCount() == 0) {
                return null;
            }
            if (sortedDocValues instanceof OrdinalRunsDocValues runsDocValues) {
                tsIdRuns = runsDocValues.ordinalRuns();
            }
            if (tsIdRuns == null) {
                throw new IllegalStateException(
                    "Segment is not sorted on ["
                        + TSDBSyntheticIdPostingsFormat.TS_ID
                        + "] first and holds no ordinal runs for it, so synthetic _ids cannot be looked up"
                );
            }
        }
        return tsIdRuns;
    }

    /**
     * Returns the first document of the region at the end of the segment whose documents have no value for the primary index sort
     * field, or {@code maxDocs} if every document has one. Under the grouped sort those are the delete tombstones: they carry the
     * _tsid of the deleted document but none of the sort keys ahead of it, so they trail the segment, sorted by _tsid and timestamp
     * among themselves. When the tombstones of the last live _tsid directly follow its documents, the run recorded for that
     * ordinal (see {@link OrdinalRunsWriter}) spans both and is not sorted by timestamp as a whole, so it is clipped here, see
     * {@link #tsIdRunEnd}. Runs that start in the region (ordinals with tombstones only) are left intact.
     */
    private int liveRunsEnd() throws IOException {
        if (liveRunsEnd == -1) {
            if (primarySortFieldInfo == null) {
                liveRunsEnd = 0;
            } else {
                // Bisect for the first document from which the field has no more values; every probe needs a fresh iterator, as
                // doc values only advance.
                int lo = 0;
                int hi = maxDocs;
                while (lo < hi) {
                    int mid = (lo + hi) >>> 1;
                    if (primarySortFieldValues().advance(mid) == DocIdSetIterator.NO_MORE_DOCS) {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }
                liveRunsEnd = lo;
            }
        }
        return liveRunsEnd;
    }

    private DocIdSetIterator primarySortFieldValues() throws IOException {
        return switch (primarySortFieldInfo.getDocValuesType()) {
            case SORTED -> docValuesProducer.getSorted(primarySortFieldInfo);
            case SORTED_SET -> docValuesProducer.getSortedSet(primarySortFieldInfo);
            case NUMERIC -> docValuesProducer.getNumeric(primarySortFieldInfo);
            case SORTED_NUMERIC -> docValuesProducer.getSortedNumeric(primarySortFieldInfo);
            case BINARY -> docValuesProducer.getBinary(primarySortFieldInfo);
            case NONE -> throw new IllegalStateException("Index sort field [" + primarySortFieldInfo.getName() + "] has no doc values");
        };
    }

    /** The end of the run of {@code tsIdOrd}, without the tombstones that may directly follow its documents, see {@link #liveRunsEnd}. */
    private int clippedRunEnd(OrdinalRunsDocValues.OrdinalRuns runs, int tsIdOrd) throws IOException {
        final int end = runs.endDoc(tsIdOrd);
        final int liveEnd = liveRunsEnd();
        return runs.firstDoc(tsIdOrd) < liveEnd && liveEnd < end ? liveEnd : end;
    }

    /**
     * Returns a document id past all the documents of {@code tsIdOrd} that precedes all the documents of greater _tsid ordinals: the
     * end of the run of the ordinal under the grouped sort (see {@link #clippedRunEnd}), and, when _tsid is the primary sort field,
     * the document the skipper reports the next ordinal from (every document before it has an ordinal at most {@code tsIdOrd}).
     * Returns {@code maxDocs} if no document has a greater ordinal.
     */
    int tsIdRunEnd(int tsIdOrd) throws IOException {
        var runs = tsIdRuns();
        if (runs != null) {
            return clippedRunEnd(runs, tsIdOrd);
        }
        int nextStartDocID = findStartDocIDForTsIdOrd(tsIdOrd + 1);
        return nextStartDocID == DocIdSetIterator.NO_MORE_DOCS ? maxDocs : nextStartDocID;
    }

    /**
     * Returns the document holding the smallest synthetic _id of the segment, or {@link DocIdSetIterator#NO_MORE_DOCS} if no
     * document has a _tsid.
     */
    int firstDocInTermOrder() throws IOException {
        var runs = tsIdRuns();
        if (runs != null) {
            return runs.firstDoc(0);
        }
        for (int doc = 0; doc < maxDocs; doc++) {
            if (hasTsIdDocValue(doc)) {
                return doc;
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    /**
     * Returns the document holding the greatest synthetic _id of the segment, or {@link DocIdSetIterator#NO_MORE_DOCS} if no
     * document has a _tsid. Documents of a _tsid are sorted by descending timestamp, so that is the last document of the greatest
     * _tsid, skipping over the NOOP tombstones that may follow it.
     */
    int lastDocInTermOrder() throws IOException {
        var runs = tsIdRuns();
        int from = runs != null ? clippedRunEnd(runs, getTsIdValueCount() - 1) : maxDocs;
        for (int doc = from - 1; doc >= 0; doc--) {
            if (hasTsIdDocValue(doc)) {
                return doc;
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    /**
     * Returns the _tsid ordinal value for a given docID. The document ID must exist and must have a value for the field.
     *
     * @param docID the docID
     * @return the _tsid ordinal value
     * @throws IOException if any I/O exception occurs
     */
    int docTsIdOrdinal(int docID) throws IOException {
        if (tsIdDocValues == null || tsIdDocValues.docID() > docID) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        boolean found = tsIdDocValues.advanceExact(docID);
        if (found == false) {
            throw new IllegalStateException("No _tsid value for docID " + docID);
        }
        return tsIdDocValues.ordValue();
    }

    /**
     * Returns the timestamp value for a given docID. The document ID must exist and must have a value for the field.
     *
     * @param docID the docID
     * @return the timestamp value
     * @throws IOException if any I/O exception occurs
     */
    long docTimestamp(int docID) throws IOException {
        if (timestampDocValues == null || timestampDocValues.docID() > docID) {
            timestampDocValues = docValuesProducer.getSortedNumeric(timestampFieldInfo);
        }
        boolean found = timestampDocValues.advanceExact(docID);
        assert found : "No value found for field [" + timestampFieldInfo.getName() + "] and docID " + docID;
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
    BytesRef docRoutingHash(int docID) throws IOException {
        if (routingHashDocValues == null || routingHashDocValues.docID() > docID) {
            routingHashDocValues = docValuesProducer.getSorted(routingHashFieldInfo);
        }
        boolean found = routingHashDocValues.advanceExact(docID);
        assert found : "No value found for field [" + routingHashFieldInfo.getName() + "] and docID " + docID;
        return routingHashDocValues.lookupOrd(routingHashDocValues.ordValue());
    }

    /**
     * Lookup if a given _tsid exists, returning a positive ordinal if it exists otherwise it returns -insertionPoint-1.
     *
     * @param tsId the _tsid to look up
     * @return a positive ordinal if the _tsid exists, else returns -insertionPoint-1.
     * @throws IOException if any I/O exception occurs
     */
    int lookupTsIdTerm(BytesRef tsId) throws IOException {
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
            cachedTsId = BytesRef.deepCopyOf(tsId);
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
    BytesRef lookupTsIdOrd(int tsIdOrdinal) throws IOException {
        if (cachedTsIdOrd != -1 && cachedTsIdOrd == tsIdOrdinal) {
            return cachedTsId;
        }
        // The ordinal of the current document is only defined once the iterator is positioned, which the ordinal runs path (see
        // #tsIdRuns) does not need before building a term.
        if (tsIdDocValues == null || (isPositioned(tsIdDocValues) && tsIdDocValues.ordValue() > tsIdOrdinal)) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        assert 0 <= tsIdOrdinal : tsIdOrdinal;
        assert tsIdOrdinal < tsIdDocValues.getValueCount() : tsIdOrdinal;
        var tsId = tsIdDocValues.lookupOrd(tsIdOrdinal);
        if (tsId != null) {
            cachedTsIdOrd = tsIdOrdinal;
            cachedTsId = BytesRef.deepCopyOf(tsId);
        }
        return tsId;
    }

    private static boolean isPositioned(SortedDocValues docValues) {
        return docValues.docID() >= 0 && docValues.docID() != DocIdSetIterator.NO_MORE_DOCS;
    }

    /**
     * Use a doc values skipper to find a starting document ID for the provided _tsid ordinal. The returned document ID might have the
     * exact _tsid ordinal provided, or a lower one; every document before it is guaranteed to have a lower _tsid ordinal. Returns
     * {@link DocIdSetIterator#NO_MORE_DOCS} if the provided ordinal is greater than every ordinal in the segment.
     *
     * @param tsIdOrd the _tsid ordinal
     * @return a docID to start scanning documents from in order to find the first document ID matching the provided _tsid
     * @throws IOException if any I/O exception occurs
     */
    int findStartDocIDForTsIdOrd(int tsIdOrd) throws IOException {
        assert tsIdOrd >= 0 : tsIdOrd;
        if (hasTsIdSkipper == false) {
            return 0;
        }
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
     * <p>
     * Warning: This method can be slow because it potentially scans many documents in the segment.
     * </p>
     */
    int findFirstDocWithTsIdOrdinalEqualOrGreaterThan(int tsIdOrd) throws IOException {
        var runs = tsIdRuns();
        if (runs != null) {
            // Every ordinal has documents, so the first document with an ordinal at least tsIdOrd is the start of its own run
            return runs.firstDoc(tsIdOrd);
        }
        int startDocId = findStartDocIDForTsIdOrd(tsIdOrd);
        assert startDocId != DocIdSetIterator.NO_MORE_DOCS : startDocId;

        // recreate even if doc values are already on the same ordinal, to ensure the method returns the first doc
        if (tsIdDocValues == null || (cachedTsIdOrd != -1 && cachedTsIdOrd >= tsIdOrd) || tsIdDocValues.docID() > startDocId) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        assert 0 <= tsIdOrd : tsIdOrd;
        assert tsIdOrd < tsIdDocValues.getValueCount() : tsIdOrd;

        final int bisected = bisectFirstDocWithTsIdOrdinalAtLeast(startDocId, tsIdOrd);
        if (bisected == DocIdSetIterator.NO_MORE_DOCS) {
            cachedTsIdOrd = -1;
            cachedTsId = null;
            return DocIdSetIterator.NO_MORE_DOCS;
        }
        if (bisected >= 0) {
            startDocId = bisected;
        }

        for (int docID = startDocId; docID != DocIdSetIterator.NO_MORE_DOCS; docID = tsIdDocValues.nextDoc()) {
            // Skip documents without _tsid (NOOP tombstones)
            if (tsIdDocValues.advanceExact(docID) == false) {
                continue;
            }
            var ord = tsIdDocValues.ordValue();
            if (ord == tsIdOrd || tsIdOrd < ord) {
                if (ord != cachedTsIdOrd) {
                    cachedTsId = BytesRef.deepCopyOf(tsIdDocValues.lookupOrd(ord));
                    cachedTsIdOrd = ord;
                }
                return docID;
            }
        }
        cachedTsIdOrd = -1;
        cachedTsId = null;
        assert false : "Method must be called with an existing _tsid ordinal: " + tsIdOrd;
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    /**
     * Find the first document that has a _tsid equal to the provided _tsid ordinal, returning its document ID. If no document is found,
     * the method returns {@link DocIdSetIterator#NO_MORE_DOCS}.
     * <p>
     * Warning: This method can be slow because it potentially scans many documents in the segment.
     * </p>
     */
    int findFirstDocWithTsIdOrdinalEqualTo(int tsIdOrd) throws IOException {
        var runs = tsIdRuns();
        if (runs != null) {
            return runs.firstDoc(tsIdOrd);
        }
        int startDocId = findStartDocIDForTsIdOrd(tsIdOrd);
        assert startDocId != DocIdSetIterator.NO_MORE_DOCS : startDocId;

        // recreate even if doc values are already on the same ordinal, to ensure the method returns the first doc
        if (tsIdDocValues == null || (cachedTsIdOrd != -1 && cachedTsIdOrd >= tsIdOrd) || tsIdDocValues.docID() > startDocId) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        assert 0 <= tsIdOrd : tsIdOrd;
        assert tsIdOrd < tsIdDocValues.getValueCount() : tsIdOrd;

        final int bisected = bisectFirstDocWithTsIdOrdinalAtLeast(startDocId, tsIdOrd);
        if (bisected == DocIdSetIterator.NO_MORE_DOCS) {
            cachedTsIdOrd = -1;
            cachedTsId = null;
            return DocIdSetIterator.NO_MORE_DOCS;
        }
        if (bisected >= 0) {
            startDocId = bisected;
        }

        for (int docID = startDocId; docID != DocIdSetIterator.NO_MORE_DOCS; docID = tsIdDocValues.nextDoc()) {
            // Skip documents without _tsid (NOOP tombstones)
            if (tsIdDocValues.advanceExact(docID) == false) {
                continue;
            }
            var ord = tsIdDocValues.ordValue();
            if (ord == tsIdOrd) {
                if (ord != cachedTsIdOrd) {
                    cachedTsId = BytesRef.deepCopyOf(tsIdDocValues.lookupOrd(ord));
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

    @Nullable
    DocValuesSkipper docValuesSkipperForTimestamp() throws IOException {
        if (hasTimestampSkipper == false) {
            return null;
        }
        var skipper = docValuesProducer.getSkipper(timestampFieldInfo);
        assert skipper != null;
        return skipper;
    }

    /** Returns a random access reader over the timestamp column, or {@code null} if it only supports iteration. */
    @Nullable
    RandomAccessNumericValues randomAccessTimestamps() throws IOException {
        if (randomAccessTimestampsResolved == false) {
            randomAccessTimestampsResolved = true;
            // Single valued, so the sorted numeric wraps a plain numeric column.
            var unwrapped = DocValues.unwrapSingleton(docValuesProducer.getSortedNumeric(timestampFieldInfo));
            if (unwrapped instanceof RandomAccessNumericValues.Provider provider) {
                randomAccessTimestamps = provider.tryRandomAccess();
            }
        }
        return randomAccessTimestamps;
    }

    /** Returns a random access reader over the _tsid ordinals, or {@code null} if the column only supports iteration. */
    @Nullable
    private RandomAccessNumericValues randomAccessTsIdOrdinals() throws IOException {
        if (randomAccessTsIdOrdsResolved == false) {
            randomAccessTsIdOrdsResolved = true;
            if (docValuesProducer.getSorted(tsIdFieldInfo) instanceof RandomAccessNumericValues.Provider provider) {
                randomAccessTsIdOrds = provider.tryRandomAccess();
            }
        }
        return randomAccessTsIdOrds;
    }

    /**
     * Returns the first document at or after {@code startDocID} whose _tsid ordinal is at least {@code tsIdOrd}, found by
     * bisection since ordinals ascend with document id, or {@code -1} when the column only supports iteration.
     */
    private int bisectFirstDocWithTsIdOrdinalAtLeast(int startDocID, int tsIdOrd) throws IOException {
        final var ords = randomAccessTsIdOrdinals();
        if (ords == null) {
            return -1;
        }
        int lo = startDocID;
        int hi = maxDocs;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (ords.valueAt(mid) < tsIdOrd) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo == maxDocs ? DocIdSetIterator.NO_MORE_DOCS : lo;
    }

    int getTsIdValueCount() throws IOException {
        if (tsIdDocValues == null) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
        }
        return tsIdDocValues.getValueCount();
    }

    int getTsidFixedLength() throws IOException {
        if (tsidFixedLength >= 0) {
            return tsidFixedLength;
        }
        if (tsIdDocValues == null) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
        }
        if (tsIdDocValues.getValueCount() == 0) {
            tsidFixedLength = 0;
        } else {
            tsidFixedLength = tsIdDocValues.lookupOrd(0).length;
        }
        return tsidFixedLength;
    }

    /**
     * Returns true if the document has a _tsid doc value.
     * NOOP tombstones don't have _tsid doc values and return false.
     */
    boolean hasTsIdDocValue(int docID) throws IOException {
        if (tsIdDocValues == null || tsIdDocValues.docID() > docID) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        boolean hasTsId = tsIdDocValues.advanceExact(docID);
        assert hasTsId || assertNoOpTombstone(docID);
        return hasTsId;
    }

    private boolean assertNoOpTombstone(int docID) throws IOException {
        assert isTombstone(docID) : "Document " + docID + " has no _tsid but is not a tombstone";
        assert isSoftDeleted(docID) : "Document " + docID + " has no _tsid but is not soft-deleted";
        return true;
    }

    private boolean isTombstone(int docID) throws IOException {
        if (tombstoneFieldInfo == null) {
            return false;
        }
        if (tombstoneDocValues == null || tombstoneDocValues.docID() > docID) {
            tombstoneDocValues = docValuesProducer.getNumeric(tombstoneFieldInfo);
        }
        return tombstoneDocValues.advanceExact(docID) && tombstoneDocValues.longValue() > 0;
    }

    private boolean isSoftDeleted(int docID) throws IOException {
        if (softDeletesFieldInfo == null) {
            return false;
        }
        if (softDeletesDocValues == null || softDeletesDocValues.docID() > docID) {
            softDeletesDocValues = docValuesProducer.getNumeric(softDeletesFieldInfo);
        }
        return softDeletesDocValues.advanceExact(docID) && softDeletesDocValues.longValue() == 1;
    }

    /**
     * Returns the synthetic _id for a given document ID. Document must exist.
     *
     * @param   docID the document ID
     * @return  the synthetic _id
     * @throws IOException if any I/O exception occurs
     */
    BytesRef docSyntheticId(int docID) throws IOException {
        return docSyntheticId(docID, docTsIdOrdinal(docID), docTimestamp(docID), null);
    }

    BytesRef docSyntheticId(int docID, int docTsIdOrd, long docTimestamp, @Nullable BytesRefBuilder scratch) throws IOException {
        final var tsId = lookupTsIdOrd(docTsIdOrd);
        final var routingHashBytes = docRoutingHash(docID);

        // The synthetic _id starts with the tsId bytes. If the first byte is >= BASE64_ESCAPE (0xfd),
        // it could be misinterpreted as the UTF8 (0xff) or NUMERIC (0xfe) encoding prefixes used
        // elsewhere in ID handling. In that case, we prepend an escape byte to avoid ambiguity.
        // See Uid#encodeBase64Id which applies the same escaping logic when encoding IDs.
        final boolean needsEscape = Byte.toUnsignedInt(tsId.bytes[tsId.offset]) >= Uid.BASE64_ESCAPE;
        final int offset = needsEscape ? 1 : 0;
        final int length = TsidExtractingIdFieldMapper.syntheticIdLength(tsId) + offset;

        if (scratch != null) {
            scratch.grow(length);
            if (needsEscape) {
                scratch.setByteAt(0, (byte) Uid.BASE64_ESCAPE);
            }
            TsidExtractingIdFieldMapper.writeSyntheticId(tsId, docTimestamp, routingHashBytes, scratch.bytes(), offset);
            scratch.setLength(length);
            return scratch.get();
        }

        // Fallback without scratch (allocates)
        byte[] bytes = new byte[length];
        if (needsEscape) {
            bytes[0] = (byte) Uid.BASE64_ESCAPE;
        }
        TsidExtractingIdFieldMapper.writeSyntheticId(tsId, docTimestamp, routingHashBytes, bytes, offset);
        return new BytesRef(bytes);
    }
}
