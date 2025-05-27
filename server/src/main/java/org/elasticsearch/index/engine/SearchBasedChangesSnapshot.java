/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.search.lookup.Source;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract class that provides a snapshot mechanism to retrieve operations from a live Lucene index
 * within a specified range of sequence numbers. Subclasses are expected to define the
 * method to fetch the next batch of operations.
 */
public abstract class SearchBasedChangesSnapshot implements Translog.Snapshot, Closeable {
    public static final int DEFAULT_BATCH_SIZE = 1024;

    private final IndexVersion indexVersionCreated;
    private final IndexSearcher indexSearcher;
    private final ValueFetcher sourceMetadataFetcher;
    private final Closeable onClose;

    protected final long fromSeqNo, toSeqNo;
    protected final boolean requiredFullRange;
    protected final int searchBatchSize;

    private final boolean accessStats;
    private final int totalHits;
    private FieldDoc afterDoc;
    private long lastSeenSeqNo;

    /**
     * Constructs a new snapshot for fetching changes within a sequence number range.
     *
     * @param engineSearcher       Engine searcher instance.
     * @param searchBatchSize      Number of documents to retrieve per batch.
     * @param fromSeqNo            Starting sequence number.
     * @param toSeqNo              Ending sequence number.
     * @param requiredFullRange    Whether the full range is required.
     * @param accessStats          If true, enable access statistics for counting total operations.
     * @param indexVersionCreated  Version of the index when it was created.
     */
    protected SearchBasedChangesSnapshot(
        MapperService mapperService,
        Engine.Searcher engineSearcher,
        int searchBatchSize,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
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

        this.indexVersionCreated = indexVersionCreated;
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.lastSeenSeqNo = fromSeqNo - 1;
        this.requiredFullRange = requiredFullRange;
        this.indexSearcher = newIndexSearcher(engineSearcher);
        this.indexSearcher.setQueryCache(null);

        long requestingSize = (toSeqNo - fromSeqNo == Long.MAX_VALUE) ? Long.MAX_VALUE : (toSeqNo - fromSeqNo + 1L);
        this.searchBatchSize = (int) Math.min(requestingSize, searchBatchSize);

        this.accessStats = accessStats;
        this.totalHits = accessStats ? indexSearcher.count(rangeQuery(fromSeqNo, toSeqNo, indexVersionCreated)) : -1;
        this.sourceMetadataFetcher = createSourceMetadataValueFetcher(mapperService, indexSearcher);
    }

    private ValueFetcher createSourceMetadataValueFetcher(MapperService mapperService, IndexSearcher searcher) {
        if (mapperService.mappingLookup().inferenceFields().isEmpty()) {
            return null;
        }
        var mapper = (InferenceMetadataFieldsMapper) mapperService.mappingLookup()
            .getMapping()
            .getMetadataMapperByName(InferenceMetadataFieldsMapper.NAME);
        return mapper != null
            ? mapper.fieldType().valueFetcher(mapperService.mappingLookup(), mapperService.getBitSetProducer(), searcher)
            : null;
    }

    /**
     * Abstract method for retrieving the next operation. Should be implemented by subclasses.
     *
     * @return The next Translog.Operation in the snapshot.
     * @throws IOException If an I/O error occurs.
     */
    protected abstract Translog.Operation nextOperation() throws IOException;

    /**
     * Returns the list of index leaf reader contexts.
     *
     * @return List of LeafReaderContext.
     */
    public List<LeafReaderContext> leaves() {
        return indexSearcher.getIndexReader().leaves();
    }

    @Override
    public int totalOperations() {
        if (accessStats == false) {
            throw new IllegalStateException("Access stats of a snapshot created with [access_stats] is false");
        }
        return totalHits;
    }

    @Override
    public final Translog.Operation next() throws IOException {
        Translog.Operation op = nextOperation();
        if (requiredFullRange) {
            verifyRange(op);
        }
        if (op != null) {
            assert fromSeqNo <= op.seqNo() && op.seqNo() <= toSeqNo && lastSeenSeqNo < op.seqNo()
                : "Unexpected operation; last_seen_seqno ["
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

    @Override
    public void close() throws IOException {
        onClose.close();
    }

    /**
     * Retrieves the next batch of top documents based on the sequence range.
     *
     * @return TopDocs instance containing the documents in the current batch.
     */
    protected TopDocs nextTopDocs() throws IOException {
        Query rangeQuery = rangeQuery(Math.max(fromSeqNo, lastSeenSeqNo), toSeqNo, indexVersionCreated);
        SortField sortBySeqNo = new SortField(SeqNoFieldMapper.NAME, SortField.Type.LONG);

        TopFieldCollectorManager collectorManager = new TopFieldCollectorManager(new Sort(sortBySeqNo), searchBatchSize, afterDoc, 0);
        TopDocs results = indexSearcher.search(rangeQuery, collectorManager);

        if (results.scoreDocs.length > 0) {
            afterDoc = (FieldDoc) results.scoreDocs[results.scoreDocs.length - 1];
        }
        for (int i = 0; i < results.scoreDocs.length; i++) {
            results.scoreDocs[i].shardIndex = i;
        }
        return results;
    }

    /**
     * Sets the reader context to enable reading metadata that was removed from the {@code _source}.
     * This method sets up the {@code sourceMetadataFetcher} with the provided {@link LeafReaderContext},
     * ensuring it is ready to fetch metadata for subsequent operations.
     *
     * <p>Note: This method should be called before {@link #addSourceMetadata(BytesReference, int)} at the start of every leaf
     * to ensure the metadata fetcher is properly initialized.</p>
     */
    protected void setNextSourceMetadataReader(LeafReaderContext context) {
        if (sourceMetadataFetcher != null) {
            sourceMetadataFetcher.setNextReader(context);
        }
    }

    /**
     * Creates a new {@link Source} object by combining the provided {@code originalSource}
     * with additional metadata fields. If the {@code sourceMetadataFetcher} is null or no metadata
     * fields are fetched, the original source is returned unchanged.
     *
     * @param originalSourceBytes the original source bytes
     * @param segmentDocID the document ID used to fetch metadata fields
     * @return a new {@link Source} instance containing the original data and additional metadata,
     *         or the original source if no metadata is added
     * @throws IOException if an error occurs while fetching metadata values
     */
    protected BytesReference addSourceMetadata(BytesReference originalSourceBytes, int segmentDocID) throws IOException {
        if (sourceMetadataFetcher == null) {
            return originalSourceBytes;
        }
        var originalSource = Source.fromBytes(originalSourceBytes);
        List<Object> values = sourceMetadataFetcher.fetchValues(originalSource, segmentDocID, List.of());
        if (values.isEmpty()) {
            return originalSourceBytes;
        }
        var map = originalSource.source();
        map.put(InferenceMetadataFieldsMapper.NAME, values.get(0));
        return Source.fromMap(map, originalSource.sourceContentType()).internalSourceRef();
    }

    static IndexSearcher newIndexSearcher(Engine.Searcher engineSearcher) throws IOException {
        return new IndexSearcher(Lucene.wrapAllDocsLive(engineSearcher.getDirectoryReader()));
    }

    static Query rangeQuery(long fromSeqNo, long toSeqNo, IndexVersion indexVersionCreated) {
        return new BooleanQuery.Builder().add(SeqNoFieldMapper.rangeQueryForSeqNo(fromSeqNo, toSeqNo), BooleanClause.Occur.MUST)
            .add(Queries.newNonNestedFilter(indexVersionCreated), BooleanClause.Occur.MUST)
            .build();
    }

    private void verifyRange(Translog.Operation op) {
        if (op == null && lastSeenSeqNo < toSeqNo) {
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
        } else if (op != null && op.seqNo() != lastSeenSeqNo + 1) {
            throw new MissingHistoryOperationsException(
                "Not all operations between from_seqno ["
                    + fromSeqNo
                    + "] "
                    + "and to_seqno ["
                    + toSeqNo
                    + "] found; expected seqno ["
                    + lastSeenSeqNo
                    + 1
                    + "]; found ["
                    + op
                    + "]"
            );
        }
    }

    protected static boolean assertDocSoftDeleted(LeafReader leafReader, int segmentDocId) throws IOException {
        NumericDocValues docValues = leafReader.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
        if (docValues == null || docValues.advanceExact(segmentDocId) == false) {
            throw new IllegalStateException("DocValues for field [" + Lucene.SOFT_DELETES_FIELD + "] is not found");
        }
        return docValues.longValue() == 1;
    }
}
