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
import org.apache.lucene.index.SortedDocValues;
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
import org.elasticsearch.index.mapper.Uid;
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
    private SortedDocValues idDocValues;
    private final LeafReader reader;

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
        this.reader = reader;
        final Terms terms = reader.terms(IdFieldMapper.NAME);
        if (terms == null) {
            idDocValues = reader.getSortedDocValues(IdFieldMapper.NAME);
            termsEnum = null;
            if (idDocValues == null) {
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
            }
        } else {
            idDocValues = null;
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
            final Bits liveDocs = context.reader().getLiveDocs();
            int docID = DocIdSetIterator.NO_MORE_DOCS;
            // there may be more than one matching docID, in the case of nested docs, so we want the last one:
            docsEnum = termsEnum.postings(docsEnum, 0);
            for (int d = docsEnum.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docsEnum.nextDoc()) {
                if (liveDocs != null && liveDocs.get(d) == false) {
                    continue;
                }
                docID = d;
            }
            return docID;
        } else if (idDocValues != null) {
            idDocValues = reader.getSortedDocValues(IdFieldMapper.NAME);
//            var needle = id.utf8ToString();
            final Bits liveDocs = context.reader().getLiveDocs();
            int docID = DocIdSetIterator.NO_MORE_DOCS;
            // there may be more than one matching docID, in the case of nested docs, so we want the last one:
            for (int doc = idDocValues.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = idDocValues.nextDoc()) {
                if (liveDocs != null && liveDocs.get(doc) == false) {
                    continue;
                }

                int ord = idDocValues.ordValue();
                var possibleMatch = idDocValues.lookupOrd(ord);
                var uidForm = Uid.encodeId(possibleMatch.utf8ToString());
                if (id.bytesEquals(uidForm)) {
                    docID = doc;
                }
            }
            return docID;
        } else {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
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
    DocIdAndSeqNo lookupSeqNo(BytesRef id, LeafReaderContext context) throws IOException {
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";
        final int docID = getDocID(id, context);
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final long seqNo = readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID);
            return new DocIdAndSeqNo(docID, seqNo, context);
        } else {
            return null;
        }
    }
}
