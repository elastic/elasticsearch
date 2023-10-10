/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Translog.Snapshot} from changes in a Lucene index
 */
final class LuceneChangesSnapshot implements Translog.Snapshot {
    static final int DEFAULT_BATCH_SIZE = 1024;

    private final int searchBatchSize;
    private final long fromSeqNo, toSeqNo;
    private long lastSeenSeqNo;
    private int skippedOperations;
    private final boolean requiredFullRange;
    private final boolean singleConsumer;

    private final IndexSearcher indexSearcher;
    private int docIndex = 0;
    private final boolean accessStats;
    private final int totalHits;
    private ScoreDoc[] scoreDocs;
    private final ParallelArray parallelArray;
    private final Closeable onClose;

    private final IndexVersion indexVersionCreated;

    private int storedFieldsReaderOrd = -1;
    private StoredFieldsReader storedFieldsReader = null;

    private final Thread creationThread; // for assertion

    /**
     * Creates a new "translog" snapshot from Lucene for reading operations whose seq# in the specified range.
     *
     * @param engineSearcher    the internal engine searcher which will be taken over if the snapshot is opened successfully
     * @param searchBatchSize   the number of documents should be returned by each search
     * @param fromSeqNo         the min requesting seq# - inclusive
     * @param toSeqNo           the maximum requesting seq# - inclusive
     * @param requiredFullRange if true, the snapshot will strictly check for the existence of operations between fromSeqNo and toSeqNo
     * @param singleConsumer    true if the snapshot is accessed by a single thread that creates the snapshot
     * @param accessStats       true if the stats of the snapshot can be accessed via {@link #totalOperations()}
     * @param indexVersionCreated the version on which this index was created
     */
    LuceneChangesSnapshot(
        Engine.Searcher engineSearcher,
        int searchBatchSize,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        IndexVersion indexVersionCreated
    ) throws IOException {
        if (fromSeqNo < 0 || toSeqNo < 0 || fromSeqNo > toSeqNo) {
            throw new IllegalArgumentException("Invalid range; from_seqno [" + fromSeqNo + "], to_seqno [" + toSeqNo + "]");
        }
        if (searchBatchSize <= 0) {
            throw new IllegalArgumentException("Search_batch_size must be positive [" + searchBatchSize + "]");
        }
        final AtomicBoolean closed = new AtomicBoolean();
        this.onClose = () -> {
            if (closed.compareAndSet(false, true)) {
                IOUtils.close(engineSearcher);
            }
        };
        final long requestingSize = (toSeqNo - fromSeqNo) == Long.MAX_VALUE ? Long.MAX_VALUE : (toSeqNo - fromSeqNo + 1L);
        this.creationThread = Thread.currentThread();
        this.searchBatchSize = requestingSize < searchBatchSize ? Math.toIntExact(requestingSize) : searchBatchSize;
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.lastSeenSeqNo = fromSeqNo - 1;
        this.requiredFullRange = requiredFullRange;
        this.singleConsumer = singleConsumer;
        this.indexSearcher = newIndexSearcher(engineSearcher);
        this.indexSearcher.setQueryCache(null);
        this.accessStats = accessStats;
        this.parallelArray = new ParallelArray(this.searchBatchSize);
        this.indexVersionCreated = indexVersionCreated;
        final TopDocs topDocs = searchOperations(null, accessStats);
        this.totalHits = Math.toIntExact(topDocs.totalHits.value);
        this.scoreDocs = topDocs.scoreDocs;
        fillParallelArray(scoreDocs, parallelArray);
    }

    @Override
    public void close() throws IOException {
        assert assertAccessingThread();
        onClose.close();
    }

    @Override
    public int totalOperations() {
        assert assertAccessingThread();
        if (accessStats == false) {
            throw new IllegalStateException("Access stats of a snapshot created with [access_stats] is false");
        }
        return totalHits;
    }

    @Override
    public int skippedOperations() {
        assert assertAccessingThread();
        return skippedOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        assert assertAccessingThread();
        Translog.Operation op = null;
        for (int idx = nextDocIndex(); idx != -1; idx = nextDocIndex()) {
            op = readDocAsOp(idx);
            if (op != null) {
                break;
            }
        }
        if (requiredFullRange) {
            rangeCheck(op);
        }
        if (op != null) {
            lastSeenSeqNo = op.seqNo();
        }
        return op;
    }

    private boolean assertAccessingThread() {
        assert singleConsumer == false || creationThread == Thread.currentThread()
            : "created by [" + creationThread + "] != current thread [" + Thread.currentThread() + "]";
        assert Transports.assertNotTransportThread("reading changes snapshot may involve slow IO");
        return true;
    }

    private void rangeCheck(Translog.Operation op) {
        if (op == null) {
            if (lastSeenSeqNo < toSeqNo) {
                throw new MissingHistoryOperationsException(
                    "Not all operations between from_seqno ["
                        + fromSeqNo
                        + "] "
                        + "and to_seqno ["
                        + toSeqNo
                        + "] found; prematurely terminated last_seen_seqno ["
                        + lastSeenSeqNo
                        + "]"
                );
            }
        } else {
            final long expectedSeqNo = lastSeenSeqNo + 1;
            if (op.seqNo() != expectedSeqNo) {
                throw new MissingHistoryOperationsException(
                    "Not all operations between from_seqno ["
                        + fromSeqNo
                        + "] "
                        + "and to_seqno ["
                        + toSeqNo
                        + "] found; expected seqno ["
                        + expectedSeqNo
                        + "]; found ["
                        + op
                        + "]"
                );
            }
        }
    }

    private int nextDocIndex() throws IOException {
        // we have processed all docs in the current search - fetch the next batch
        if (docIndex == scoreDocs.length && docIndex > 0) {
            final ScoreDoc prev = scoreDocs[scoreDocs.length - 1];
            scoreDocs = searchOperations((FieldDoc) prev, false).scoreDocs;
            fillParallelArray(scoreDocs, parallelArray);
            docIndex = 0;
        }
        if (docIndex < scoreDocs.length) {
            int idx = docIndex;
            docIndex++;
            return idx;
        }
        return -1;
    }

    private void fillParallelArray(ScoreDoc[] scoreDocs, ParallelArray parallelArray) throws IOException {
        if (scoreDocs.length > 0) {
            for (int i = 0; i < scoreDocs.length; i++) {
                scoreDocs[i].shardIndex = i;
            }
            parallelArray.useSequentialStoredFieldsReader = singleConsumer && scoreDocs.length >= 10 && hasSequentialAccess(scoreDocs);
            if (parallelArray.useSequentialStoredFieldsReader == false) {
                storedFieldsReaderOrd = -1;
                storedFieldsReader = null;
            }
            // for better loading performance we sort the array by docID and
            // then visit all leaves in order.
            if (parallelArray.useSequentialStoredFieldsReader == false) {
                ArrayUtil.introSort(scoreDocs, Comparator.comparingInt(i -> i.doc));
            }
            int docBase = -1;
            int maxDoc = 0;
            List<LeafReaderContext> leaves = indexSearcher.getIndexReader().leaves();
            int readerIndex = 0;
            CombinedDocValues combinedDocValues = null;
            LeafReaderContext leaf = null;
            for (ScoreDoc scoreDoc : scoreDocs) {
                if (scoreDoc.doc >= docBase + maxDoc) {
                    do {
                        leaf = leaves.get(readerIndex++);
                        docBase = leaf.docBase;
                        maxDoc = leaf.reader().maxDoc();
                    } while (scoreDoc.doc >= docBase + maxDoc);
                    combinedDocValues = new CombinedDocValues(leaf.reader());
                }
                final int segmentDocID = scoreDoc.doc - docBase;
                final int index = scoreDoc.shardIndex;
                parallelArray.leafReaderContexts[index] = leaf;
                parallelArray.seqNo[index] = combinedDocValues.docSeqNo(segmentDocID);
                parallelArray.primaryTerm[index] = combinedDocValues.docPrimaryTerm(segmentDocID);
                parallelArray.version[index] = combinedDocValues.docVersion(segmentDocID);
                parallelArray.isTombStone[index] = combinedDocValues.isTombstone(segmentDocID);
                parallelArray.hasRecoverySource[index] = combinedDocValues.hasRecoverySource(segmentDocID);
            }
            // now sort back based on the shardIndex. we use this to store the previous index
            if (parallelArray.useSequentialStoredFieldsReader == false) {
                ArrayUtil.introSort(scoreDocs, Comparator.comparingInt(i -> i.shardIndex));
            }
        }
    }

    private static boolean hasSequentialAccess(ScoreDoc[] scoreDocs) {
        for (int i = 0; i < scoreDocs.length - 1; i++) {
            if (scoreDocs[i].doc + 1 != scoreDocs[i + 1].doc) {
                return false;
            }
        }
        return true;
    }

    private static IndexSearcher newIndexSearcher(Engine.Searcher engineSearcher) throws IOException {
        return new IndexSearcher(Lucene.wrapAllDocsLive(engineSearcher.getDirectoryReader()));
    }

    private static Query rangeQuery(long fromSeqNo, long toSeqNo, IndexVersion indexVersionCreated) {
        return new BooleanQuery.Builder().add(LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, fromSeqNo, toSeqNo), BooleanClause.Occur.MUST)
            .add(Queries.newNonNestedFilter(indexVersionCreated), BooleanClause.Occur.MUST) // exclude non-root nested documents
            .build();
    }

    static int countOperations(Engine.Searcher engineSearcher, long fromSeqNo, long toSeqNo, IndexVersion indexVersionCreated)
        throws IOException {
        if (fromSeqNo < 0 || toSeqNo < 0 || fromSeqNo > toSeqNo) {
            throw new IllegalArgumentException("Invalid range; from_seqno [" + fromSeqNo + "], to_seqno [" + toSeqNo + "]");
        }
        return newIndexSearcher(engineSearcher).count(rangeQuery(fromSeqNo, toSeqNo, indexVersionCreated));
    }

    private TopDocs searchOperations(FieldDoc after, boolean accurateTotalHits) throws IOException {
        final Query rangeQuery = rangeQuery(Math.max(fromSeqNo, lastSeenSeqNo), toSeqNo, indexVersionCreated);
        assert accurateTotalHits == false || after == null : "accurate total hits is required by the first batch only";
        final SortField sortBySeqNo = new SortField(SeqNoFieldMapper.NAME, SortField.Type.LONG);
        final TopFieldCollector collector = TopFieldCollector.create(
            new Sort(sortBySeqNo),
            searchBatchSize,
            after,
            accurateTotalHits ? Integer.MAX_VALUE : 0
        );
        indexSearcher.search(rangeQuery, collector);
        return collector.topDocs();
    }

    private Translog.Operation readDocAsOp(int docIndex) throws IOException {
        final LeafReaderContext leaf = parallelArray.leafReaderContexts[docIndex];
        final int segmentDocID = scoreDocs[docIndex].doc - leaf.docBase;
        final long primaryTerm = parallelArray.primaryTerm[docIndex];
        assert primaryTerm > 0 : "nested child document must be excluded";
        final long seqNo = parallelArray.seqNo[docIndex];
        // Only pick the first seen seq#
        if (seqNo == lastSeenSeqNo) {
            skippedOperations++;
            return null;
        }
        final long version = parallelArray.version[docIndex];
        final String sourceField = parallelArray.hasRecoverySource[docIndex]
            ? SourceFieldMapper.RECOVERY_SOURCE_NAME
            : SourceFieldMapper.NAME;
        final FieldsVisitor fields = new FieldsVisitor(true, sourceField);

        if (parallelArray.useSequentialStoredFieldsReader) {
            if (storedFieldsReaderOrd != leaf.ord) {
                if (leaf.reader() instanceof SequentialStoredFieldsLeafReader) {
                    storedFieldsReader = ((SequentialStoredFieldsLeafReader) leaf.reader()).getSequentialStoredFieldsReader();
                    storedFieldsReaderOrd = leaf.ord;
                } else {
                    storedFieldsReader = null;
                    storedFieldsReaderOrd = -1;
                }
            }
        }
        if (storedFieldsReader != null) {
            assert singleConsumer : "Sequential access optimization must not be enabled for multiple consumers";
            assert parallelArray.useSequentialStoredFieldsReader;
            assert storedFieldsReaderOrd == leaf.ord : storedFieldsReaderOrd + " != " + leaf.ord;
            storedFieldsReader.document(segmentDocID, fields);
        } else {
            leaf.reader().document(segmentDocID, fields);
        }

        final Translog.Operation op;
        final boolean isTombstone = parallelArray.isTombStone[docIndex];
        if (isTombstone && fields.id() == null) {
            op = new Translog.NoOp(seqNo, primaryTerm, fields.source().utf8ToString());
            assert version == 1L : "Noop tombstone should have version 1L; actual version [" + version + "]";
            assert assertDocSoftDeleted(leaf.reader(), segmentDocID) : "Noop but soft_deletes field is not set [" + op + "]";
        } else {
            final String id = fields.id();
            if (isTombstone) {
                op = new Translog.Delete(id, seqNo, primaryTerm, version);
                assert assertDocSoftDeleted(leaf.reader(), segmentDocID) : "Delete op but soft_deletes field is not set [" + op + "]";
            } else {
                final BytesReference source = fields.source();
                if (source == null) {
                    // TODO: Callers should ask for the range that source should be retained. Thus we should always
                    // check for the existence source once we make peer-recovery to send ops after the local checkpoint.
                    if (requiredFullRange) {
                        throw new MissingHistoryOperationsException(
                            "source not found for seqno=" + seqNo + " from_seqno=" + fromSeqNo + " to_seqno=" + toSeqNo
                        );
                    } else {
                        skippedOperations++;
                        return null;
                    }
                }
                // TODO: pass the latest timestamp from engine.
                final long autoGeneratedIdTimestamp = -1;
                op = new Translog.Index(id, seqNo, primaryTerm, version, source, fields.routing(), autoGeneratedIdTimestamp);
            }
        }
        assert fromSeqNo <= op.seqNo() && op.seqNo() <= toSeqNo && lastSeenSeqNo < op.seqNo()
            : "Unexpected operation; "
                + "last_seen_seqno ["
                + lastSeenSeqNo
                + "], from_seqno ["
                + fromSeqNo
                + "], to_seqno ["
                + toSeqNo
                + "], op ["
                + op
                + "]";
        return op;
    }

    private static boolean assertDocSoftDeleted(LeafReader leafReader, int segmentDocId) throws IOException {
        final NumericDocValues ndv = leafReader.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
        if (ndv == null || ndv.advanceExact(segmentDocId) == false) {
            throw new IllegalStateException("DocValues for field [" + Lucene.SOFT_DELETES_FIELD + "] is not found");
        }
        return ndv.longValue() == 1;
    }

    private static final class ParallelArray {
        final LeafReaderContext[] leafReaderContexts;
        final long[] version;
        final long[] seqNo;
        final long[] primaryTerm;
        final boolean[] isTombStone;
        final boolean[] hasRecoverySource;
        boolean useSequentialStoredFieldsReader = false;

        ParallelArray(int size) {
            version = new long[size];
            seqNo = new long[size];
            primaryTerm = new long[size];
            isTombStone = new boolean[size];
            hasRecoverySource = new boolean[size];
            leafReaderContexts = new LeafReaderContext[size];
        }
    }

    // for testing
    boolean useSequentialStoredFieldsReader() {
        return storedFieldsReader != null;
    }
}
