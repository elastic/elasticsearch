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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.Uid;
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
public final class LuceneSyntheticSourceChangesSnapshot extends SearchBasedChangesSnapshot {
    private final long maxMemorySizeInBytes;
    private final StoredFieldLoader storedFieldLoader;
    private final SourceLoader sourceLoader;

    private final boolean routingDocValues;
    private final boolean sliceEnabled;
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
        boolean accessStats
    ) throws IOException {
        super(mapperService, engineSearcher, searchBatchSize, fromSeqNo, toSeqNo, requiredFullRange, accessStats);
        // a MapperService#updateMapping(...) of empty index may not have been invoked and then mappingLookup is empty
        assert engineSearcher.getDirectoryReader().maxDoc() == 0
            || mapperService.mappingLookup().isSourceSynthetic()
            || mapperService.mappingLookup().isSourceColumnarStored()
            : "either an empty index or synthetic/columnar_stored source must be enabled for proper functionality.";
        // ensure we can buffer at least one document
        this.maxMemorySizeInBytes = maxMemorySizeInBytes > 0 ? maxMemorySizeInBytes : 1;
        this.sourceLoader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP);
        Set<String> storedFields = sourceLoader.requiredStoredFields();
        String defaultCodec = EngineConfig.INDEX_CODEC_SETTING.get(mapperService.getIndexSettings().getSettings());
        // zstd best compression stores upto 2048 docs in a block, so it is likely that in this case docs are co-located in same block:
        boolean forceSequentialReader = CodecService.BEST_COMPRESSION_CODEC.equals(defaultCodec);
        this.storedFieldLoader = StoredFieldLoader.create(false, storedFields, forceSequentialReader);
        RoutingFieldMapper routingMapper = (RoutingFieldMapper) mapperService.mappingLookup().getMapper(RoutingFieldMapper.NAME);
        this.routingDocValues = routingMapper != null && routingMapper.docValues();
        this.sliceEnabled = mapperService.getIndexSettings().isSliceEnabled();
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
        SortedDocValues leafRoutingDocValues = null;
        BinaryDocValues leafIdDocValues = null;
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
                    if (record.hasRecoverySourceSize() == false) {
                        assert requiredFullRange == false : "source not found for seqno=" + record.seqNo();
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
                if (routingDocValues) {
                    leafRoutingDocValues = leafReaderContext.reader().getSortedDocValues(RoutingFieldMapper.NAME);
                }
                if (columnarId) {
                    leafIdDocValues = DocValues.getBinary(leafReaderContext.reader(), IdFieldMapper.NAME);
                }
                setNextSyntheticFieldsReader(leafReaderContext);
            }
            int segmentDocID = docRecord.docID() - docBase;
            leafFieldLoader.advanceTo(segmentDocID);
            operations[docRecord.index()] = createOperation(
                docRecord,
                leafFieldLoader,
                leafSourceLoader,
                leafRoutingDocValues,
                leafIdDocValues,
                segmentDocID,
                leafReaderContext
            );
        }
        return operations;
    }

    private Translog.Operation createOperation(
        SearchRecord docRecord,
        LeafStoredFieldLoader fieldLoader,
        SourceLoader.Leaf sourceLoader,
        SortedDocValues routingDocValues,
        BinaryDocValues leafIdDocValues,
        int segmentDocID,
        LeafReaderContext context
    ) throws IOException {
        // The _id lives in binary doc values in columnar mode and in stored fields in document mode; these are mutually
        // exclusive, so exactly one source is populated.
        assert columnarId == false || fieldLoader.id() == null : "_id shouldn't be in stored fields when columnar";
        assert columnarId || leafIdDocValues == null : "_id shouldn't be in doc values when not columnar";
        // A tombstone and a live doc need different _id representations, so resolve each independently. Slice only
        // affects the tombstone (whose identity is the compound term); a live doc always carries the plain user id.
        return docRecord.isTombstone()
            ? createTombstoneOperation(docRecord, fieldLoader, leafIdDocValues, segmentDocID, context)
            : createIndexOperation(docRecord, fieldLoader, sourceLoader, routingDocValues, leafIdDocValues, segmentDocID);
    }

    private Translog.Operation createIndexOperation(
        SearchRecord docRecord,
        LeafStoredFieldLoader fieldLoader,
        SourceLoader.Leaf sourceLoader,
        SortedDocValues routingDocValues,
        BinaryDocValues leafIdDocValues,
        int segmentDocID
    ) throws IOException {
        if (docRecord.hasRecoverySourceSize() == false) {
            // TODO: Callers should ask for the range that source should be retained. Thus we should always
            // check for the existence source once we make peer-recovery to send ops after the local checkpoint.
            if (requiredFullRange) {
                throw new MissingHistoryOperationsException(
                    "source not found for seqno=" + docRecord.seqNo() + " from_seqno=" + fromSeqNo + " to_seqno=" + toSeqNo
                );
            }
            skippedOperations++;
            return null;
        }
        // A live doc's _id is the plain user id in every mode (a slice index stores the plain id, not the compound term,
        // on live docs): from doc values in columnar mode, otherwise from the field loader.
        final String id = columnarId ? decodeColumnarId(leafIdDocValues, segmentDocID) : fieldLoader.id();
        var source = addSyntheticFields(sourceLoader.source(fieldLoader, segmentDocID), segmentDocID);
        String routing = fieldLoader.routing();
        if (routing == null && routingDocValues != null) {
            routing = readRoutingFromDocValues(routingDocValues, segmentDocID);
        }
        return new Translog.Index(
            id,
            docRecord.seqNo(),
            docRecord.primaryTerm(),
            docRecord.version(),
            source != null ? source.internalSourceRef() : null,
            routing,
            -1 // autogenerated timestamp
        );
    }

    private Translog.Operation createTombstoneOperation(
        SearchRecord docRecord,
        LeafStoredFieldLoader fieldLoader,
        BinaryDocValues leafIdDocValues,
        int segmentDocID,
        LeafReaderContext context
    ) throws IOException {
        if (sliceEnabled) {
            // The slice tombstone's identity IS the compound (slice, id) term, used verbatim (routing-free): from doc
            // values in columnar mode, otherwise read raw from stored fields (the field loader would mis-decode it).
            final BytesRef uid = columnarId ? readColumnarUid(leafIdDocValues, segmentDocID) : readRawId(context, segmentDocID);
            return uid == null ? noOp(docRecord, segmentDocID, context) : delete(uid, docRecord, segmentDocID, context);
        }
        // Otherwise the tombstone _id is the plain id, which Translog.Delete re-encodes into the identity term.
        final String id = columnarId ? decodeColumnarId(leafIdDocValues, segmentDocID) : fieldLoader.id();
        return id == null ? noOp(docRecord, segmentDocID, context) : delete(id, docRecord, segmentDocID, context);
    }

    private Translog.Operation noOp(SearchRecord docRecord, int segmentDocID, LeafReaderContext context) throws IOException {
        assert docRecord.version() == 1L : "Noop tombstone should have version 1L; actual version [" + docRecord.version() + "]";
        assert assertDocSoftDeleted(context.reader(), segmentDocID) : "Noop but soft_deletes field is not set [" + docRecord + "]";
        return new Translog.NoOp(docRecord.seqNo(), docRecord.primaryTerm(), "null");
    }

    private Translog.Operation delete(BytesRef uid, SearchRecord docRecord, int segmentDocID, LeafReaderContext context)
        throws IOException {
        assert assertDocSoftDeleted(context.reader(), segmentDocID) : "Delete op but soft_deletes field is not set [" + docRecord + "]";
        return new Translog.Delete(uid, docRecord.seqNo(), docRecord.primaryTerm(), docRecord.version());
    }

    private Translog.Operation delete(String id, SearchRecord docRecord, int segmentDocID, LeafReaderContext context) throws IOException {
        assert assertDocSoftDeleted(context.reader(), segmentDocID) : "Delete op but soft_deletes field is not set [" + docRecord + "]";
        return new Translog.Delete(id, docRecord.seqNo(), docRecord.primaryTerm(), docRecord.version());
    }

    /** Decode the plain id from the {@code _id} binary doc values (columnar mode), or {@code null} if absent. */
    private static String decodeColumnarId(BinaryDocValues leafIdDocValues, int segmentDocID) throws IOException {
        return leafIdDocValues.advanceExact(segmentDocID) ? Uid.decodeId(leafIdDocValues.binaryValue()) : null;
    }

    /** Copy the raw {@code _id} bytes from the binary doc values (a slice tombstone's compound term), or {@code null}. */
    private static BytesRef readColumnarUid(BinaryDocValues leafIdDocValues, int segmentDocID) throws IOException {
        return leafIdDocValues.advanceExact(segmentDocID) ? BytesRef.deepCopyOf(leafIdDocValues.binaryValue()) : null;
    }

    private static String readRoutingFromDocValues(SortedDocValues routingDocValues, int segmentDocID) throws IOException {
        if (routingDocValues != null && routingDocValues.advanceExact(segmentDocID)) {
            return routingDocValues.lookupOrd(routingDocValues.ordValue()).utf8ToString();
        }
        return null;
    }

    /** Read the raw stored {@code _id} bytes (the compound term on a slice tombstone), or {@code null} if absent. */
    private static BytesRef readRawId(LeafReaderContext context, int segmentDocID) throws IOException {
        RawIdVisitor visitor = new RawIdVisitor();
        context.reader().storedFields().document(segmentDocID, visitor);
        return visitor.idBytes;
    }

    private static final class RawIdVisitor extends StoredFieldVisitor {
        private BytesRef idBytes;

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            return IdFieldMapper.NAME.equals(fieldInfo.name) ? Status.YES : Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            idBytes = new BytesRef(value);
        }
    }
}
