/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

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
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.engine.LuceneChangesSnapshot.getLeafDocIDs;
import static org.elasticsearch.index.engine.LuceneChangesSnapshot.loadFromStoredFields;
import static org.elasticsearch.index.engine.LuceneChangesSnapshot.loadFromSourceLoader;

/**
 * A {@link Translog.Snapshot} implementation that retrieves changes from a Lucene index in batches.
 * All operations within a batch are held in memory, with the batch size configurable to control
 * memory usage.
 */
public final class LuceneBatchChangesSnapshot implements Translog.Snapshot {
    private final int batchSize;
    private final long fromSeqNo, toSeqNo;
    private long lastSeenSeqNo;
    private int skippedOperations;
    private final boolean requiredFullRange;

    private final IndexSearcher indexSearcher;
    private int docIndex = 0;
    private final boolean accessStats;
    private final int totalHits;
    private ScoreDoc[] scoreDocs;
    private final Translog.Operation[] ops;
    private final Closeable onClose;

    private final IndexVersion indexVersionCreated;

    private final StoredFieldLoader storedFieldLoader;
    private final SourceLoader sourceLoader;

    /**
     * Creates a new "translog" snapshot from Lucene for reading operations whose seq# in the specified range.
     *
     * @param engineSearcher    the internal engine searcher which will be taken over if the snapshot is opened successfully
     * @param batchSize   the number of documents to load per batch
     * @param fromSeqNo         the min requesting seq# - inclusive
     * @param toSeqNo           the maximum requesting seq# - inclusive
     * @param requiredFullRange if true, the snapshot will strictly check for the existence of operations between fromSeqNo and toSeqNo
     * @param accessStats       true if the stats of the snapshot can be accessed via {@link #totalOperations()}
     * @param indexVersionCreated the version on which this index was created
     */
    public LuceneBatchChangesSnapshot(
        MappingLookup mappingLookup,
        Engine.Searcher engineSearcher,
        int batchSize,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accessStats,
        IndexVersion indexVersionCreated
    ) throws IOException {
        if (fromSeqNo < 0 || toSeqNo < 0 || fromSeqNo > toSeqNo) {
            throw new IllegalArgumentException("Invalid range; from_seqno [" + fromSeqNo + "], to_seqno [" + toSeqNo + "]");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batch_size must be positive [" + batchSize + "]");
        }
        final AtomicBoolean closed = new AtomicBoolean();
        this.onClose = () -> {
            if (closed.compareAndSet(false, true)) {
                IOUtils.close(engineSearcher);
            }
        };
        final long requestingSize = (toSeqNo - fromSeqNo) == Long.MAX_VALUE ? Long.MAX_VALUE : (toSeqNo - fromSeqNo + 1L);
        this.batchSize = requestingSize < batchSize ? Math.toIntExact(requestingSize) : batchSize;
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.lastSeenSeqNo = fromSeqNo - 1;
        this.requiredFullRange = requiredFullRange;
        this.indexSearcher = newIndexSearcher(engineSearcher);
        this.indexSearcher.setQueryCache(null);
        this.accessStats = accessStats;
        this.ops = new Translog.Operation[this.batchSize];
        this.indexVersionCreated = indexVersionCreated;
        final TopDocs topDocs = searchOperations(null, accessStats);
        this.totalHits = Math.toIntExact(topDocs.totalHits.value);
        this.scoreDocs = topDocs.scoreDocs;
        if (mappingLookup != null) {
            this.sourceLoader = mappingLookup.newSourceLoader(SourceFieldMetrics.NOOP);
            Set<String> storedFields = sourceLoader.requiredStoredFields();
            this.storedFieldLoader = StoredFieldLoader.create(mappingLookup.isSourceSynthetic(), storedFields);
        } else {
            this.storedFieldLoader = StoredFieldLoader.create(true, Set.of());
            this.sourceLoader = null;
        }
        fillOps(scoreDocs, ops);
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }

    @Override
    public int totalOperations() {
        if (accessStats == false) {
            throw new IllegalStateException("Access stats of a snapshot created with [access_stats] is false");
        }
        return totalHits;
    }

    @Override
    public int skippedOperations() {
        return skippedOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        Translog.Operation op = null;
        for (int idx = nextDocIndex(); idx != -1; idx = nextDocIndex()) {
            op = ops[idx];
            if (op != null) {
                // Only pick the first seen seq#
                if (op.seqNo() == lastSeenSeqNo) {
                    skippedOperations++;
                    continue;
                }
                break;
            }
        }
        if (requiredFullRange) {
            rangeCheck(op);
        }
        if (op != null) {
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
            lastSeenSeqNo = op.seqNo();
        }
        return op;
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
            fillOps(scoreDocs, ops);
            docIndex = 0;
        }
        if (docIndex < scoreDocs.length) {
            int idx = docIndex;
            docIndex++;
            return idx;
        }
        return -1;
    }

    private void fillOps(ScoreDoc[] scoreDocs, Translog.Operation[] ops) throws IOException {
        for (int i = 0; i < scoreDocs.length; i++) {
            scoreDocs[i].shardIndex = i;
        }
        // for better loading performance we sort the array by docID and
        // then visit all leaves in order.
        ArrayUtil.introSort(scoreDocs, Comparator.comparingInt(i -> i.doc));
        int docBase = -1;
        int maxDoc = 0;
        List<LeafReaderContext> leaves = indexSearcher.getIndexReader().leaves();
        int readerIndex = 0;
        LeafReaderContext leafReader = null;
        CombinedDocValues combinedDocValues = null;
        LeafStoredFieldLoader leafFields = null;
        SourceLoader.Leaf leafSource = null;
        for (int i = 0; i < scoreDocs.length; i++) {
            var scoreDoc = scoreDocs[i];
            if (scoreDoc.doc >= docBase + maxDoc) {
                do {
                    leafReader = leaves.get(readerIndex++);
                    docBase = leafReader.docBase;
                    maxDoc = leafReader.reader().maxDoc();
                } while (scoreDoc.doc >= docBase + maxDoc);

                combinedDocValues = new CombinedDocValues(leafReader.reader());
                var docIds = getLeafDocIDs(scoreDocs, i, docBase, maxDoc);
                leafFields = storedFieldLoader.getLoader(leafReader, docIds);
                leafSource = sourceLoader != null ? sourceLoader.leaf(leafReader.reader(), docIds) : null;
            }
            final int segmentDocID = scoreDoc.doc - docBase;
            final int index = scoreDoc.shardIndex;

            final long primaryTerm = combinedDocValues.docPrimaryTerm(segmentDocID);
            assert primaryTerm > 0 : "nested child document must be excluded";
            final long seqNo = combinedDocValues.docSeqNo(segmentDocID);
            final long version = combinedDocValues.docVersion(segmentDocID);
            final boolean hasRecoverySource = combinedDocValues.hasRecoverySource(segmentDocID);
            final var doc = (hasRecoverySource || leafSource == null)
                ? loadFromStoredFields(segmentDocID, leafFields.reader(), hasRecoverySource)
                : loadFromSourceLoader(segmentDocID, leafFields, leafSource);

            final Translog.Operation op;
            final boolean isTombstone = combinedDocValues.isTombstone(segmentDocID);
            if (isTombstone && doc.id() == null) {
                op = new Translog.NoOp(seqNo, primaryTerm, doc.source().utf8ToString());
                assert version == 1L : "Noop tombstone should have version 1L; actual version [" + version + "]";
                assert assertDocSoftDeleted(leafReader.reader(), segmentDocID) : "Noop but soft_deletes field is not set [" + op + "]";
            } else {
                if (isTombstone) {
                    op = new Translog.Delete(doc.id(), seqNo, primaryTerm, version);
                    assert assertDocSoftDeleted(leafReader.reader(), segmentDocID)
                        : "Delete op but soft_deletes field is not set [" + op + "]";
                } else {
                    if (doc.source() == null) {
                        // TODO: Callers should ask for the range that source should be retained. Thus we should always
                        // check for the existence source once we make peer-recovery to send ops after the local checkpoint.
                        if (requiredFullRange) {
                            throw new MissingHistoryOperationsException(
                                "source not found for seqno=" + seqNo + " from_seqno=" + fromSeqNo + " to_seqno=" + toSeqNo
                            );
                        } else {
                            skippedOperations++;
                            ops[index] = null;
                            continue;
                        }
                    }
                    // TODO: pass the latest timestamp from engine.
                    final long autoGeneratedIdTimestamp = -1;
                    op = new Translog.Index(doc.id(), seqNo, primaryTerm, version, doc.source(), doc.routing(), autoGeneratedIdTimestamp);
                }
            }
            ops[index] = op;
        }
        ArrayUtil.introSort(scoreDocs, Comparator.comparingInt(i -> i.shardIndex));
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
        TopFieldCollectorManager topFieldCollectorManager = new TopFieldCollectorManager(
            new Sort(sortBySeqNo),
            batchSize,
            after,
            accurateTotalHits ? Integer.MAX_VALUE : 0
        );
        return indexSearcher.search(rangeQuery, topFieldCollectorManager);
    }

    private static boolean assertDocSoftDeleted(LeafReader leafReader, int segmentDocId) throws IOException {
        final NumericDocValues ndv = leafReader.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
        if (ndv == null || ndv.advanceExact(segmentDocId) == false) {
            throw new IllegalStateException("DocValues for field [" + Lucene.SOFT_DELETES_FIELD + "] is not found");
        }
        return ndv.longValue() == 1;
    }
}
