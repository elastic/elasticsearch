/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceLoader.SyntheticVectorsLoader;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A {@link Translog.Snapshot} from changes in a Lucene index
 */
public final class LuceneChangesSnapshot extends SearchBasedChangesSnapshot {
    private long lastSeenSeqNo;
    private int skippedOperations;
    private final boolean singleConsumer;

    private int docIndex = 0;
    private int maxDocIndex;
    private final ParallelArray parallelArray;

    private int storedFieldsReaderOrd = -1;
    private StoredFieldsReader storedFieldsReader = null;
    private final SyntheticVectorsLoader syntheticVectorPatchLoader;
    private SyntheticVectorsLoader.Leaf syntheticVectorPatchLoaderLeaf;

    private final DocValuesOrdinalToRoutingLookup ordinalToRoutingLookup;
    private final boolean sliceEnabled;

    private final Thread creationThread; // for assertion

    /**
     * Creates a new "translog" snapshot from Lucene for reading operations whose seq# in the specified range.
     *
     * @param mapperService     the mapper service for this index
     * @param engineSearcher    the internal engine searcher which will be taken over if the snapshot is opened successfully
     * @param searchBatchSize   the number of documents should be returned by each search
     * @param fromSeqNo         the min requesting seq# - inclusive
     * @param toSeqNo           the maximum requesting seq# - inclusive
     * @param requiredFullRange if true, the snapshot will strictly check for the existence of operations between fromSeqNo and toSeqNo
     * @param singleConsumer    true if the snapshot is accessed by a single thread that creates the snapshot
     * @param accessStats       true if the stats of the snapshot can be accessed via {@link #totalOperations()}
     */
    public LuceneChangesSnapshot(
        MapperService mapperService,
        Engine.Searcher engineSearcher,
        int searchBatchSize,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats
    ) throws IOException {
        super(mapperService, engineSearcher, searchBatchSize, fromSeqNo, toSeqNo, requiredFullRange, accessStats);
        this.creationThread = Assertions.ENABLED ? Thread.currentThread() : null;
        this.singleConsumer = singleConsumer;
        this.parallelArray = new ParallelArray(this.searchBatchSize);
        this.lastSeenSeqNo = fromSeqNo - 1;
        final TopDocs topDocs = nextTopDocs();
        this.maxDocIndex = topDocs.scoreDocs.length;
        this.syntheticVectorPatchLoader = mapperService.mappingLookup().getMapping().syntheticVectorsLoader(null);
        RoutingFieldMapper routingMapper = (RoutingFieldMapper) mapperService.mappingLookup().getMapper(RoutingFieldMapper.NAME);
        boolean routingStoredAsDocValues = routingMapper != null && routingMapper.docValues();
        this.ordinalToRoutingLookup = routingStoredAsDocValues ? new DocValuesOrdinalToRoutingLookup() : null;
        this.sliceEnabled = mapperService.getIndexSettings().isSliceEnabled();
        fillParallelArray(topDocs.scoreDocs, parallelArray);
    }

    @Override
    public void close() throws IOException {
        assert assertAccessingThread();
        super.close();
    }

    @Override
    public int totalOperations() {
        assert assertAccessingThread();
        return super.totalOperations();
    }

    @Override
    public int skippedOperations() {
        assert assertAccessingThread();
        return skippedOperations;
    }

    @Override
    protected Translog.Operation nextOperation() throws IOException {
        assert assertAccessingThread();
        Translog.Operation op = null;
        for (int idx = nextDocIndex(); idx != -1; idx = nextDocIndex()) {
            op = readDocAsOp(idx);
            if (op != null) {
                break;
            }
        }
        return op;
    }

    private boolean assertAccessingThread() {
        assert singleConsumer == false || creationThread == Thread.currentThread()
            : "created by [" + creationThread + "] != current thread [" + Thread.currentThread() + "]";
        assert Transports.assertNotTransportThread("reading changes snapshot may involve slow IO");
        return true;
    }

    private int nextDocIndex() throws IOException {
        // we have processed all docs in the current search - fetch the next batch
        if (docIndex == maxDocIndex && docIndex > 0) {
            var scoreDocs = nextTopDocs().scoreDocs;
            fillParallelArray(scoreDocs, parallelArray);
            docIndex = 0;
            maxDocIndex = scoreDocs.length;
        }
        if (docIndex < maxDocIndex) {
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
            int readerIndex = 0;
            CombinedDocValues combinedDocValues = null;
            LeafReaderContext leaf = null;
            SortedDocValues routingDocValues = null;
            for (ScoreDoc scoreDoc : scoreDocs) {
                if (scoreDoc.doc >= docBase + maxDoc) {
                    do {
                        leaf = leaves().get(readerIndex++);
                        docBase = leaf.docBase;
                        maxDoc = leaf.reader().maxDoc();
                    } while (scoreDoc.doc >= docBase + maxDoc);
                    combinedDocValues = new CombinedDocValues(leaf.reader());
                    if (ordinalToRoutingLookup != null) {
                        routingDocValues = leaf.reader().getSortedDocValues(RoutingFieldMapper.NAME);
                    }
                }
                final int segmentDocID = scoreDoc.doc - docBase;
                final int index = scoreDoc.shardIndex;
                parallelArray.leafReaderContexts[index] = leaf;
                parallelArray.docID[index] = scoreDoc.doc;
                parallelArray.seqNo[index] = combinedDocValues.docSeqNo(segmentDocID);
                parallelArray.primaryTerm[index] = combinedDocValues.docPrimaryTerm(segmentDocID);
                parallelArray.version[index] = combinedDocValues.docVersion(segmentDocID);
                parallelArray.isTombStone[index] = combinedDocValues.isTombstone(segmentDocID);
                parallelArray.hasRecoverySource[index] = combinedDocValues.hasRecoverySource(segmentDocID);
                if (ordinalToRoutingLookup != null) {
                    // If _routing isn't configured to be required then isn't guaranteed that all documents have a routing value.
                    // This why this docId check is required here.
                    if (routingDocValues != null && routingDocValues.advanceExact(segmentDocID)) {
                        parallelArray.routingOrdinals[index] = routingDocValues.ordValue();
                    } else {
                        parallelArray.routingOrdinals[index] = -1;
                    }
                }
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

    static int countOperations(Engine.Searcher engineSearcher, IndexSettings indexSettings, long fromSeqNo, long toSeqNo)
        throws IOException {
        if (fromSeqNo < 0 || toSeqNo < 0 || fromSeqNo > toSeqNo) {
            throw new IllegalArgumentException("Invalid range; from_seqno [" + fromSeqNo + "], to_seqno [" + toSeqNo + "]");
        }
        return newIndexSearcher(engineSearcher).count(rangeQuery(indexSettings, fromSeqNo, toSeqNo));
    }

    private Translog.Operation readDocAsOp(int docIndex) throws IOException {
        final LeafReaderContext leaf = parallelArray.leafReaderContexts[docIndex];
        final int segmentDocID = parallelArray.docID[docIndex] - leaf.docBase;
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
        // In a slice index the tombstone _id is the compound term (not a valid plain encodeId), so capture the raw _id
        // bytes rather than eagerly decoding them; live docs still store the plain _id.
        final FieldsVisitor fields = sliceEnabled ? new SliceAwareFieldsVisitor(true, sourceField) : new FieldsVisitor(true, sourceField);

        if (parallelArray.useSequentialStoredFieldsReader) {
            if (storedFieldsReaderOrd != leaf.ord) {
                if (leaf.reader() instanceof SequentialStoredFieldsLeafReader) {
                    storedFieldsReader = ((SequentialStoredFieldsLeafReader) leaf.reader()).getSequentialStoredFieldsReader();
                    storedFieldsReaderOrd = leaf.ord;
                    setNextSyntheticFieldsReader(leaf);
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
            setNextSyntheticFieldsReader(leaf);
            leaf.reader().storedFields().document(segmentDocID, fields);
        }
        final BytesReference source = fields.source() != null && fields.source().length() > 0
            ? addSyntheticFields(Source.fromBytes(fields.source()), segmentDocID).internalSourceRef()
            : fields.source();

        String routing;
        if (ordinalToRoutingLookup != null) {
            assert fields.routing() == null : "routing shouldn't exist in stored fields if doc_values is enabled for routing field";
            int routingOrdinal = parallelArray.routingOrdinals[docIndex];
            routing = ordinalToRoutingLookup.lookupRoutingOrdinal(leaf, routingOrdinal);
        } else {
            routing = fields.routing();
        }

        // For a slice index the raw _id bytes are the compound term on tombstones and the plain encodeId(id) on live
        // docs. The plain user id (for the noop check and the Index op) decodes from the live doc's plain _id.
        final BytesRef idBytes = sliceEnabled ? ((SliceAwareFieldsVisitor) fields).idBytes() : null;
        final String id = sliceEnabled ? (idBytes == null ? null : Uid.decodeId(idBytes)) : fields.id();

        final Translog.Operation op;
        final boolean isTombstone = parallelArray.isTombStone[docIndex];
        if (isTombstone && id == null) {
            op = new Translog.NoOp(seqNo, primaryTerm, fields.source().utf8ToString());
            assert version == 1L : "Noop tombstone should have version 1L; actual version [" + version + "]";
            assert assertDocSoftDeleted(leaf.reader(), segmentDocID) : "Noop but soft_deletes field is not set [" + op + "]";
        } else {
            if (isTombstone) {
                // The Delete carries the engine identity term: for a slice index the tombstone _id IS the compound term,
                // used as-is (routing-free); for a non-slice index encodeId(plain id) reproduces it.
                op = sliceEnabled
                    ? new Translog.Delete(BytesRef.deepCopyOf(idBytes), seqNo, primaryTerm, version)
                    : new Translog.Delete(id, seqNo, primaryTerm, version);
                assert assertDocSoftDeleted(leaf.reader(), segmentDocID) : "Delete op but soft_deletes field is not set [" + op + "]";
            } else {
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
                // The Index op carries the PLAIN id + routing(slice); replay re-parses and rebuilds the two terms.
                op = new Translog.Index(id, seqNo, primaryTerm, version, source, routing, autoGeneratedIdTimestamp);
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
        lastSeenSeqNo = op.seqNo();
        return op;
    }

    @Override
    protected void setNextSyntheticFieldsReader(LeafReaderContext context) throws IOException {
        super.setNextSyntheticFieldsReader(context);
        if (syntheticVectorPatchLoader != null) {
            syntheticVectorPatchLoaderLeaf = syntheticVectorPatchLoader.leaf(context);
        }
    }

    @Override
    protected Source addSyntheticFields(Source source, int segmentDocID) throws IOException {
        if (syntheticVectorPatchLoaderLeaf == null) {
            return super.addSyntheticFields(source, segmentDocID);
        }
        List<SourceLoader.SyntheticVectorPatch> patches = new ArrayList<>();
        syntheticVectorPatchLoaderLeaf.load(segmentDocID, patches);
        if (patches.size() == 0) {
            return super.addSyntheticFields(source, segmentDocID);
        }
        var newSource = SourceLoader.applySyntheticVectors(source, patches);
        return super.addSyntheticFields(newSource, segmentDocID);
    }

    private static final class DocValuesOrdinalToRoutingLookup {

        private int routingDocValuesOrd = -1;
        private SortedDocValues routingDocValuesReader;

        String lookupRoutingOrdinal(LeafReaderContext leaf, int routingOrdinal) throws IOException {
            if (routingOrdinal == -1) {
                return null;
            }

            if (routingDocValuesOrd != leaf.ord) {
                routingDocValuesReader = leaf.reader().getSortedDocValues(RoutingFieldMapper.NAME);
                routingDocValuesOrd = leaf.ord;
            }
            if (routingDocValuesReader != null) {
                return routingDocValuesReader.lookupOrd(routingOrdinal).utf8ToString();
            }
            return null;
        }
    }

    private static final class ParallelArray {
        final LeafReaderContext[] leafReaderContexts;
        final int[] docID;
        final long[] version;
        final long[] seqNo;
        final long[] primaryTerm;
        final boolean[] isTombStone;
        final boolean[] hasRecoverySource;
        final int[] routingOrdinals;
        boolean useSequentialStoredFieldsReader = false;

        ParallelArray(int size) {
            docID = new int[size];
            version = new long[size];
            seqNo = new long[size];
            primaryTerm = new long[size];
            isTombStone = new boolean[size];
            hasRecoverySource = new boolean[size];
            routingOrdinals = new int[size];
            Arrays.fill(routingOrdinals, -1);
            leafReaderContexts = new LeafReaderContext[size];
        }
    }

    // for testing
    boolean useSequentialStoredFieldsReader() {
        return storedFieldsReader != null;
    }

    /**
     * Captures the raw stored {@code _id} bytes instead of decoding them, because in a slice index a tombstone's
     * {@code _id} is the compound term (not a plain {@link Uid#encodeId}). Live docs store the plain {@code _id}; the
     * plain user id is decoded from those raw bytes by the caller, while a tombstone's compound bytes are used as-is.
     */
    private static final class SliceAwareFieldsVisitor extends FieldsVisitor {
        private BytesRef idBytes;

        SliceAwareFieldsVisitor(boolean loadSource, String sourceField) {
            super(loadSource, sourceField);
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                idBytes = new BytesRef(value);
            } else {
                super.binaryField(fieldInfo, value);
            }
        }

        BytesRef idBytes() {
            return idBytes;
        }
    }
}
