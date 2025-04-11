/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.IntArrayList;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A {@link SearchBasedChangesSnapshot} that utilizes a synthetic field loader to rebuild the recovery source.
 * This snapshot is activated when {@link IndexSettings#RECOVERY_USE_SYNTHETIC_SOURCE_SETTING}
 * is enabled on the underlying index.
 *
 * The {@code maxMemorySizeInBytes} parameter limits the total size of uncompressed _sources
 * loaded into memory during batch retrieval.
 */
public class LuceneSyntheticSourceChangesSnapshot extends SearchBasedChangesSnapshot {
    private final long maxMemorySizeInBytes;
    private final StoredFieldLoader storedFieldLoader;
    private final SourceLoader sourceLoader;

    private int skippedOperations;
    private long lastSeenSeqNo;

    private record SearchRecord(FieldDoc doc, boolean isTombstone, long seqNo, long primaryTerm, long version, long size) {
        int index() {
            return doc.shardIndex;
        }

        int docID() {
            return doc.doc;
        }

        boolean hasRecoverySourceSize() {
            return size != -1;
        }
    }

    private final Deque<SearchRecord> pendingDocs = new LinkedList<>();
    private final Deque<Translog.Operation> operationQueue = new LinkedList<>();

    public LuceneSyntheticSourceChangesSnapshot(
        MapperService mapperService,
        Engine.Searcher engineSearcher,
        int searchBatchSize,
        long maxMemorySizeInBytes,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accessStats,
        IndexVersion indexVersionCreated
    ) throws IOException {
        super(mapperService, engineSearcher, searchBatchSize, fromSeqNo, toSeqNo, requiredFullRange, accessStats, indexVersionCreated);
        // a MapperService#updateMapping(...) of empty index may not have been invoked and then mappingLookup is empty
        assert engineSearcher.getDirectoryReader().maxDoc() == 0 || mapperService.mappingLookup().isSourceSynthetic()
            : "either an empty index or synthetic source must be enabled for proper functionality.";
        // ensure we can buffer at least one document
        this.maxMemorySizeInBytes = maxMemorySizeInBytes > 0 ? maxMemorySizeInBytes : 1;
        this.sourceLoader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP);
        Set<String> storedFields = sourceLoader.requiredStoredFields();
        this.storedFieldLoader = StoredFieldLoader.create(false, storedFields);
        this.lastSeenSeqNo = fromSeqNo - 1;
    }

    @Override
    public int skippedOperations() {
        return skippedOperations;
    }

    @Override
    protected Translog.Operation nextOperation() throws IOException {
        while (true) {
            if (operationQueue.isEmpty()) {
                loadNextBatch();
            }
            if (operationQueue.isEmpty()) {
                return null;
            }
            var op = operationQueue.pollFirst();
            if (op.seqNo() == lastSeenSeqNo) {
                skippedOperations++;
                continue;
            }
            lastSeenSeqNo = op.seqNo();
            return op;
        }
    }

    private void loadNextBatch() throws IOException {
        List<SearchRecord> documentsToLoad = new ArrayList<>();
        long accumulatedSize = 0;
        while (accumulatedSize < maxMemorySizeInBytes) {
            if (pendingDocs.isEmpty()) {
                ScoreDoc[] topDocs = nextTopDocs().scoreDocs;
                if (topDocs.length == 0) {
                    break;
                }
                pendingDocs.addAll(Arrays.asList(transformScoreDocsToRecords(topDocs)));
            }
            SearchRecord document = pendingDocs.pollFirst();
            document.doc().shardIndex = documentsToLoad.size();
            documentsToLoad.add(document);
            accumulatedSize += document.size();
        }

        for (var op : loadDocuments(documentsToLoad)) {
            if (op == null) {
                skippedOperations++;
                continue;
            }
            operationQueue.add(op);
        }
    }

    private SearchRecord[] transformScoreDocsToRecords(ScoreDoc[] scoreDocs) throws IOException {
        ArrayUtil.introSort(scoreDocs, Comparator.comparingInt(doc -> doc.doc));
        SearchRecord[] documentRecords = new SearchRecord[scoreDocs.length];
        CombinedDocValues combinedDocValues = null;
        int docBase = -1;
        int maxDoc = 0;
        int readerIndex = 0;
        LeafReaderContext leafReaderContext;

        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            if (scoreDoc.doc >= docBase + maxDoc) {
                do {
                    leafReaderContext = leaves().get(readerIndex++);
                    docBase = leafReaderContext.docBase;
                    maxDoc = leafReaderContext.reader().maxDoc();
                } while (scoreDoc.doc >= docBase + maxDoc);
                combinedDocValues = new CombinedDocValues(leafReaderContext.reader());
            }
            int segmentDocID = scoreDoc.doc - docBase;
            int index = scoreDoc.shardIndex;
            var primaryTerm = combinedDocValues.docPrimaryTerm(segmentDocID);
            assert primaryTerm > 0 : "nested child document must be excluded";
            documentRecords[index] = new SearchRecord(
                (FieldDoc) scoreDoc,
                combinedDocValues.isTombstone(segmentDocID),
                combinedDocValues.docSeqNo(segmentDocID),
                primaryTerm,
                combinedDocValues.docVersion(segmentDocID),
                combinedDocValues.recoverySourceSize(segmentDocID)
            );
        }
        return documentRecords;
    }

    private Translog.Operation[] loadDocuments(List<SearchRecord> documentRecords) throws IOException {
        documentRecords.sort(Comparator.comparingInt(doc -> doc.docID()));
        Translog.Operation[] operations = new Translog.Operation[documentRecords.size()];

        int docBase = -1;
        int maxDoc = 0;
        int readerIndex = 0;
        LeafReaderContext leafReaderContext = null;
        LeafStoredFieldLoader leafFieldLoader = null;
        SourceLoader.Leaf leafSourceLoader = null;
        for (int i = 0; i < documentRecords.size(); i++) {
            SearchRecord docRecord = documentRecords.get(i);
            if (docRecord.docID() >= docBase + maxDoc) {
                do {
                    leafReaderContext = leaves().get(readerIndex++);
                    docBase = leafReaderContext.docBase;
                    maxDoc = leafReaderContext.reader().maxDoc();
                } while (docRecord.docID() >= docBase + maxDoc);

                // TODO: instead of building an array, consider just checking whether doc ids are dense.
                // Note, field loaders then would lose the ability to optionally eagerly loading values.
                IntArrayList nextDocIds = new IntArrayList();
                for (int j = i; j < documentRecords.size(); j++) {
                    var record = documentRecords.get(j);
                    if (record.isTombstone()) {
                        continue;
                    }
                    int docID = record.docID();
                    if (docID >= docBase + maxDoc) {
                        break;
                    }
                    int segmentDocID = docID - docBase;
                    nextDocIds.add(segmentDocID);
                }

                // This computed doc ids arrays us used by stored field loader as a heuristic to determine whether to use a sequential
                // stored field reader (which bulk loads stored fields and avoids decompressing the same blocks multiple times). For
                // source loader, it is also used as a heuristic for bulk reading doc values (E.g. SingletonDocValuesLoader).
                int[] nextDocIdArray = nextDocIds.toArray();
                leafFieldLoader = storedFieldLoader.getLoader(leafReaderContext, nextDocIdArray);
                leafSourceLoader = sourceLoader.leaf(leafReaderContext.reader(), nextDocIdArray);
                setNextSourceMetadataReader(leafReaderContext);
            }
            int segmentDocID = docRecord.docID() - docBase;
            leafFieldLoader.advanceTo(segmentDocID);
            operations[docRecord.index()] = createOperation(docRecord, leafFieldLoader, leafSourceLoader, segmentDocID, leafReaderContext);
        }
        return operations;
    }

    private Translog.Operation createOperation(
        SearchRecord docRecord,
        LeafStoredFieldLoader fieldLoader,
        SourceLoader.Leaf sourceLoader,
        int segmentDocID,
        LeafReaderContext context
    ) throws IOException {
        if (docRecord.isTombstone() && fieldLoader.id() == null) {
            assert docRecord.version() == 1L : "Noop tombstone should have version 1L; actual version [" + docRecord.version() + "]";
            assert assertDocSoftDeleted(context.reader(), segmentDocID) : "Noop but soft_deletes field is not set [" + docRecord + "]";
            return new Translog.NoOp(docRecord.seqNo(), docRecord.primaryTerm(), "null");
        } else if (docRecord.isTombstone()) {
            assert assertDocSoftDeleted(context.reader(), segmentDocID) : "Delete op but soft_deletes field is not set [" + docRecord + "]";
            return new Translog.Delete(fieldLoader.id(), docRecord.seqNo(), docRecord.primaryTerm(), docRecord.version());
        } else {
            if (docRecord.hasRecoverySourceSize() == false) {
                // TODO: Callers should ask for the range that source should be retained. Thus we should always
                // check for the existence source once we make peer-recovery to send ops after the local checkpoint.
                if (requiredFullRange) {
                    throw new MissingHistoryOperationsException(
                        "source not found for seqno=" + docRecord.seqNo() + " from_seqno=" + fromSeqNo + " to_seqno=" + toSeqNo
                    );
                } else {
                    skippedOperations++;
                    return null;
                }
            }
            var sourceBytes = addSourceMetadata(sourceLoader.source(fieldLoader, segmentDocID).internalSourceRef(), segmentDocID);
            return new Translog.Index(
                fieldLoader.id(),
                docRecord.seqNo(),
                docRecord.primaryTerm(),
                docRecord.version(),
                sourceBytes,
                fieldLoader.routing(),
                -1 // autogenerated timestamp
            );
        }
    }
}
