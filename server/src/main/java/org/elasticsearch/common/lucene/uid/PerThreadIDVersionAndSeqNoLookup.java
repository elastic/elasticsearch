/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/** Utility class to do efficient primary-key (only 1 doc contains the
 *  given term) lookups by segment, re-using the enums.  This class is
 *  not thread safe, so it is the caller's job to create and use one
 *  instance of this per thread.  Do not use this if a term may appear
 *  in more than one document!  It will only return the first one it
 *  finds.
 *  This class uses live docs, so it should be cached based on the
 *  {@link org.apache.lucene.index.IndexReader#getReaderCacheHelper() reader cache helper}
 *  rather than the {@link LeafReader#getCoreCacheHelper() core cache helper}.
 */
final class PerThreadIDVersionAndSeqNoLookup {
    // TODO: do we really need to store all this stuff? some if it might not speed up anything.
    // we keep it around for now, to reduce the amount of e.g. hash lookups by field and stuff

    private final TermsEnum termsEnum;

    /** Reused for iteration (when the term exists) */
    private PostingsEnum docsEnum;

    /** used for assertions to make sure class usage meets assumptions */
    private final Object readerKey;

    final boolean loadedTimestampRange;
    final long minTimestamp;
    final long maxTimestamp;

    /**
     * Initialize lookup for the provided segment
     */
    PerThreadIDVersionAndSeqNoLookup(LeafReader reader, boolean trackReaderKey, boolean loadTimestampRange) throws IOException {
        final Terms terms = reader.terms(IdFieldMapper.NAME);
        if (terms == null) {
            // If a segment contains only no-ops, it does not have _uid but has both _soft_deletes and _tombstone fields.
            final NumericDocValues softDeletesDV = reader.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
            final NumericDocValues tombstoneDV = reader.getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
            // this is a special case when we pruned away all IDs in a segment since all docs are deleted.
            final boolean allDocsDeleted = (softDeletesDV != null && reader.numDocs() == 0);
            if ((softDeletesDV == null || tombstoneDV == null) && allDocsDeleted == false) {
                throw new IllegalArgumentException(
                    "reader does not have _uid terms but not a no-op segment; "
                        + "_soft_deletes ["
                        + softDeletesDV
                        + "], _tombstone ["
                        + tombstoneDV
                        + "]"
                );
            }
            termsEnum = null;
        } else {
            termsEnum = terms.iterator();
        }
        if (reader.getNumericDocValues(VersionFieldMapper.NAME) == null) {
            throw new IllegalArgumentException("reader misses the [" + VersionFieldMapper.NAME + "] field; _uid terms [" + terms + "]");
        }
        Object readerKey = null;
        assert trackReaderKey ? (readerKey = reader.getCoreCacheHelper().getKey()) != null : readerKey == null;
        this.readerKey = readerKey;

        this.loadedTimestampRange = loadTimestampRange;
        // Also check for the existence of the timestamp field, because sometimes a segment can only contain tombstone documents,
        // which don't have any mapped fields (also not the timestamp field) and just some meta fields like _id, _seq_no etc.
        long minTimestamp = 0;
        long maxTimestamp = Long.MAX_VALUE;
        if (loadTimestampRange) {
            FieldInfo info = reader.getFieldInfos().fieldInfo(DataStream.TIMESTAMP_FIELD_NAME);
            if (info != null) {
                if (info.docValuesSkipIndexType() == DocValuesSkipIndexType.RANGE) {
                    DocValuesSkipper skipper = reader.getDocValuesSkipper(DataStream.TIMESTAMP_FIELD_NAME);
                    assert skipper != null : "no skipper for reader:" + reader + " and parent:" + reader.getContext().parent.reader();
                    minTimestamp = skipper.minValue();
                    maxTimestamp = skipper.maxValue();
                } else {
                    PointValues tsPointValues = reader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
                    assert tsPointValues != null
                        : "no timestamp field for reader:" + reader + " and parent:" + reader.getContext().parent.reader();
                    minTimestamp = LongPoint.decodeDimension(tsPointValues.getMinPackedValue(), 0);
                    maxTimestamp = LongPoint.decodeDimension(tsPointValues.getMaxPackedValue(), 0);
                }
            }
        }
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    PerThreadIDVersionAndSeqNoLookup(LeafReader reader, boolean loadTimestampRange) throws IOException {
        this(reader, true, loadTimestampRange);
    }

    /** Return null if id is not found.
     * We pass the {@link LeafReaderContext} as an argument so that things
     * still work with reader wrappers that hide some documents while still
     * using the same cache key. Otherwise we'd have to disable caching
     * entirely for these readers.
     */
    public DocIdAndVersion lookupVersion(BytesRef id, boolean loadSeqNo, LeafReaderContext context) throws IOException {
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";
        int docID = getDocID(id, context);

        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final long seqNo;
            final long term;
            if (loadSeqNo) {
                seqNo = readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID);
                term = readNumericDocValues(context.reader(), SeqNoFieldMapper.PRIMARY_TERM_NAME, docID);
            } else {
                seqNo = UNASSIGNED_SEQ_NO;
                term = UNASSIGNED_PRIMARY_TERM;
            }
            final long version = readNumericDocValues(context.reader(), VersionFieldMapper.NAME, docID);
            return new DocIdAndVersion(docID, version, seqNo, term, context.reader(), context.docBase);
        } else {
            return null;
        }
    }

    /**
     * returns the internal lucene doc id for the given id bytes.
     * {@link DocIdSetIterator#NO_MORE_DOCS} is returned if not found
     * */
    private int getDocID(BytesRef id, LeafReaderContext context) throws IOException {
        // termsEnum can possibly be null here if this leaf contains only no-ops.
        if (termsEnum != null && termsEnum.seekExact(id)) {
            return scanLiveDoc(context.reader().getLiveDocs());
        } else {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    /**
     * Scans postings for the term the {@link #termsEnum} is currently positioned on and returns the
     * highest live doc ID found, or {@link DocIdSetIterator#NO_MORE_DOCS} if no live doc exists.
     * There may be more than one matching doc ID in the case of nested docs, so we want the last one.
     */
    private int scanLiveDoc(Bits liveDocs) throws IOException {
        int docID = DocIdSetIterator.NO_MORE_DOCS;
        docsEnum = termsEnum.postings(docsEnum, 0);
        for (int d = docsEnum.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docsEnum.nextDoc()) {
            if (liveDocs != null && liveDocs.get(d) == false) {
                continue;
            }
            docID = d;
        }
        return docID;
    }

    /**
     * Resolves version info for multiple UIDs in a single forward pass through this segment's terms dictionary.
     * <p>
     * {@code sortedUids} must be provided in ascending order. For each uid at sorted position {@code i},
     * the result is written into {@code results[i]} if found; a null entry means not found in this segment.
     * <p>
     * {@code results} is an in/out parameter: entries that are already non-null are treated as resolved by
     * a newer segment and are skipped without touching the TermsEnum. This lets the caller accumulate
     * results across segments in a single shared array without a separate merge pass, and ensures that
     * the newest segment's version wins naturally.
     *
     * @return the number of newly resolved UIDs
     */
    int batchLookupVersion(LeafReaderContext context, BytesRef[] sortedUids, boolean[] loadSeqNo, DocIdAndVersion[] results)
        throws IOException {
        if (termsEnum == null) {
            return 0;
        }
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";

        final Bits liveDocs = context.reader().getLiveDocs();
        int resolved = 0;
        // currentTerm tracks where the TermsEnum is positioned after a NOT_FOUND seek, allowing
        // subsequent UIDs that fall before it to be skipped without issuing another seek.
        BytesRef currentTerm = null;

        for (int i = 0; i < sortedUids.length; i++) {
            if (results[i] != null) {
                // Already resolved by a newer segment — skip without touching the TermsEnum.
                continue;
            }

            final BytesRef uid = sortedUids[i];

            if (currentTerm != null) {
                final int cmp = uid.compareTo(currentTerm);
                if (cmp < 0) {
                    // uid falls before the term we already seeked past — not in this segment.
                    continue;
                } else if (cmp == 0) {
                    // The previous NOT_FOUND seek landed exactly on this uid, so the TermsEnum
                    // is already positioned at it. Fall through to scan postings.
                } else {
                    currentTerm = null; // uid is ahead of currentTerm — need a fresh seek.
                }
            }

            if (currentTerm == null) {
                final TermsEnum.SeekStatus status = termsEnum.seekCeil(uid);
                if (status == TermsEnum.SeekStatus.END) {
                    break; // exhausted this segment's terms
                }
                if (status == TermsEnum.SeekStatus.NOT_FOUND) {
                    // TermsEnum is now positioned at the first term > uid.
                    // Save it so subsequent UIDs between uid and this term can be skipped cheaply.
                    currentTerm = BytesRef.deepCopyOf(termsEnum.term());
                    continue;
                }
                // FOUND: TermsEnum is positioned at uid; clear currentTerm and scan postings below.
                currentTerm = null;
            }

            final int docID = scanLiveDoc(liveDocs);
            if (docID != DocIdSetIterator.NO_MORE_DOCS) {
                final boolean ls = loadSeqNo[i];
                final long seqNo = ls ? readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID) : UNASSIGNED_SEQ_NO;
                final long term = ls
                    ? readNumericDocValues(context.reader(), SeqNoFieldMapper.PRIMARY_TERM_NAME, docID)
                    : UNASSIGNED_PRIMARY_TERM;
                final long version = readNumericDocValues(context.reader(), VersionFieldMapper.NAME, docID);
                results[i] = new DocIdAndVersion(docID, version, seqNo, term, context.reader(), context.docBase);
                resolved++;
            }
        }

        return resolved;
    }

    /**
     * Resolves version info for multiple time-series UIDs in a single forward pass through this
     * segment's terms dictionary, using the segment's timestamp range to skip UIDs that cannot be
     * in this segment.
     * <p>
     * {@code sortedUids} must be provided in ascending lexicographic order; {@code sortedTimestamps}
     * must be aligned with {@code sortedUids}. For each uid at sorted position {@code i}, if
     * {@code results[i]} is null and the uid's timestamp falls within this segment's
     * {@link #minTimestamp}/{@link #maxTimestamp} range, the uid is looked up and the result is written
     * into {@code results[i]} if found.
     * <p>
     * UIDs whose timestamps exceed {@code maxTimestamp} are permanently resolved by storing
     * {@link VersionsAndSeqNoResolver#PERMANENTLY_NOT_FOUND} in {@code results[i]}, without a terms
     * lookup, because later segments in
     * {@link org.elasticsearch.cluster.metadata.DataStream#TIMESERIES_LEAF_READERS_SORTER} forward
     * order have even lower maxTimestamps, and therefore also cannot contain them. UIDs with
     * timestamps below {@code minTimestamp} are skipped for this segment but remain null so that
     * later segments may resolve them.
     *
     * @return the number of newly resolved UIDs (found in this segment + permanently not found)
     */
    int timeSeriesBatchLookupVersion(
        LeafReaderContext context,
        BytesRef[] sortedUids,
        long[] sortedTimestamps,
        boolean[] loadSeqNo,
        DocIdAndVersion[] results
    ) throws IOException {
        assert loadedTimestampRange : "timeSeriesBatchLookupVersion requires loadedTimestampRange=true";
        if (termsEnum == null) {
            return 0;
        }
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";

        final Bits liveDocs = context.reader().getLiveDocs();
        int resolved = 0;
        boolean termsExhausted = false;
        // currentTerm tracks where the TermsEnum is positioned after a NOT_FOUND seek, allowing
        // subsequent in-range UIDs that fall before it to be skipped without issuing another seek.
        BytesRef currentTerm = null;

        for (int i = 0; i < sortedUids.length; i++) {
            if (results[i] != null) {
                continue;
            }

            final long ts = sortedTimestamps[i];
            if (ts > maxTimestamp) {
                // Timestamp is newer than any doc in this or subsequent segments (maxTimestamp
                // decreases monotonically in TIME_SERIES forward iteration order).
                results[i] = VersionsAndSeqNoResolver.PERMANENTLY_NOT_FOUND;
                resolved++;
                continue;
            }
            if (ts < minTimestamp) {
                // Timestamp predates this segment's range; may be in a later (older) segment.
                continue;
            }

            // Timestamp is in range for this segment — look up the UID in the terms dictionary.
            if (termsExhausted) {
                // No more terms in this segment; continue to handle ts > maxTimestamp cases.
                continue;
            }

            final BytesRef uid = sortedUids[i];

            if (currentTerm != null) {
                final int cmp = uid.compareTo(currentTerm);
                if (cmp < 0) {
                    // uid falls before the term we already seeked past — not in this segment.
                    continue;
                } else if (cmp == 0) {
                    // The previous NOT_FOUND seek landed exactly on this uid, so the TermsEnum
                    // is already positioned at it. Fall through to scan postings.
                } else {
                    currentTerm = null; // uid is ahead of currentTerm — need a fresh seek.
                }
            }

            if (currentTerm == null) {
                final TermsEnum.SeekStatus status = termsEnum.seekCeil(uid);
                if (status == TermsEnum.SeekStatus.END) {
                    // Cannot break here (unlike batchLookupVersion): remaining UIDs may still
                    // have ts > maxTimestamp and need to be marked permanently not found.
                    termsExhausted = true;
                    continue;
                }
                if (status == TermsEnum.SeekStatus.NOT_FOUND) {
                    // TermsEnum is now positioned at the first term > uid.
                    // Save it so subsequent in-range UIDs before it can be skipped cheaply.
                    currentTerm = BytesRef.deepCopyOf(termsEnum.term());
                    continue;
                }
                // FOUND: TermsEnum is positioned at uid; clear currentTerm and scan postings below.
                currentTerm = null;
            }

            final int docID = scanLiveDoc(liveDocs);
            if (docID != DocIdSetIterator.NO_MORE_DOCS) {
                final boolean ls = loadSeqNo[i];
                final long seqNo = ls ? readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID) : UNASSIGNED_SEQ_NO;
                final long term = ls
                    ? readNumericDocValues(context.reader(), SeqNoFieldMapper.PRIMARY_TERM_NAME, docID)
                    : UNASSIGNED_PRIMARY_TERM;
                final long version = readNumericDocValues(context.reader(), VersionFieldMapper.NAME, docID);
                results[i] = new DocIdAndVersion(docID, version, seqNo, term, context.reader(), context.docBase);
                resolved++;
            }
        }

        return resolved;
    }

    private static long readNumericDocValues(LeafReader reader, String field, int docId) throws IOException {
        final NumericDocValues dv = reader.getNumericDocValues(field);
        if (dv == null || dv.advanceExact(docId) == false) {
            assert false : "document [" + docId + "] does not have docValues for [" + field + "]";
            throw new IllegalStateException("document [" + docId + "] does not have docValues for [" + field + "]");
        }
        return dv.longValue();
    }

    /** Return null if id is not found. */
    DocIdAndSeqNo lookupDocIdAndSeqNo(BytesRef id, LeafReaderContext context, boolean loadSeqNo) throws IOException {
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";
        final int docID = getDocID(id, context);
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final long seqNo = loadSeqNo ? readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID) : UNASSIGNED_SEQ_NO;
            return new DocIdAndSeqNo(docID, seqNo, context);
        } else {
            return null;
        }
    }
}
