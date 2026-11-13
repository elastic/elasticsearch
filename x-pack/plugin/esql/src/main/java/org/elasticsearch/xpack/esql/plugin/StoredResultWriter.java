/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.async.AsyncResultPagesIndex;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.ASYNC_ID_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.FIRST_ROW_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.PAGE_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.ROW_COUNT_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.TRANSPORT_VERSION_FIELD;

/**
 * Stores the result of a streaming query while it is streamed. Page {@code 0}, the {@link StoredResultMetadata}, is written
 * first and rewritten once the query is done, and the index is then refreshed so that the whole result is visible. Each page
 * of rows is serialized straight into a columnar batch as it is handed to the client, at the cluster's minimum transport
 * version so that any node can read it, and indexed in the background, one bulk at a time. Producers are held back while too
 * many serialized bytes wait for a bulk to be acknowledged.
 */
final class StoredResultWriter implements PageStreamPublisher.PageObserver {

    private static final Logger logger = LogManager.getLogger(StoredResultWriter.class);

    static final int PAGES_PER_BULK = 16;
    static final long MAX_UNACKED_BYTES = ByteSizeValue.ofMb(4).getBytes();

    /**
     * The pages of one bulk, serialized into a batch as they arrive.
     */
    private final class Chunk implements Releasable {
        private final EscfBatchBuilder builder = new EscfBatchBuilder(recycler);
        private final List<IndexRequest> requests = new ArrayList<>(PAGES_PER_BULK);
        private long bytes;

        @Override
        public void close() {
            builder.close();
        }
    }

    private final Client client;
    private final ClusterService clusterService;
    private final Recycler<BytesRef> recycler;
    private final String docId;
    private final byte[] docIdBytes;
    private final StoredResultMetadata running;
    private final long expirationTime;
    private final PageStreamPublisher publisher;

    // Guarded by this
    private final ArrayDeque<Chunk> chunks = new ArrayDeque<>();
    private long unackedBytes;
    private boolean bulkInFlight;
    private int nextSeq = 1;
    private long nextRow;
    private boolean finished;
    private Exception failure;

    StoredResultWriter(
        Client client,
        ClusterService clusterService,
        Recycler<BytesRef> recycler,
        String docId,
        StoredResultMetadata running,
        long expirationTime,
        PageStreamPublisher publisher
    ) {
        this.client = new OriginSettingClient(client, ASYNC_SEARCH_ORIGIN);
        this.clusterService = clusterService;
        this.recycler = recycler;
        this.docId = docId;
        this.docIdBytes = docId.getBytes(StandardCharsets.UTF_8);
        this.running = running;
        this.expirationTime = expirationTime;
        this.publisher = publisher;
    }

    /**
     * Writes page {@code 0} before any page of rows.
     */
    void start() {
        addMetadata(running);
        flush();
    }

    @Override
    public void onPage(Page page) {
        synchronized (this) {
            if (failure == null) {
                add(nextSeq, nextRow, page.getPositionCount(), page, DocWriteRequest.OpType.CREATE, false);
                nextSeq++;
                nextRow += page.getPositionCount();
            }
        }
        flush();
    }

    @Override
    public synchronized boolean hasCapacity() {
        return failure != null || unackedBytes < MAX_UNACKED_BYTES;
    }

    /**
     * Rewrites page {@code 0} with how the query ended, once every page before it is stored.
     */
    void finish(@Nullable PageStreamPublisher.StreamFooter footer, @Nullable Exception error) {
        StoredResultMetadata.Status status;
        if (error != null) {
            status = StoredResultMetadata.Status.FAILED;
        } else if (footer == null) {
            status = StoredResultMetadata.Status.CANCELLED;
        } else {
            status = StoredResultMetadata.Status.COMPLETED;
        }
        synchronized (this) {
            finished = true;
            addMetadata(
                new StoredResultMetadata(
                    running.headers(),
                    running.columns(),
                    running.zoneId(),
                    status,
                    nextSeq - 1,
                    nextRow,
                    footer == null ? 0 : footer.tookMillis(),
                    footer == null || footer.isPartial(),
                    footer == null ? List.of() : footer.warnings(),
                    error
                )
            );
        }
        flush();
    }

    /**
     * Page {@code 0} is written twice, and a batch cannot hold the same document twice, so it starts a batch of its own.
     */
    private synchronized void addMetadata(StoredResultMetadata metadata) {
        if (failure == null) {
            add(0, 0, 0, metadata, DocWriteRequest.OpType.INDEX, true);
        }
    }

    private void add(int seq, long firstRow, int rowCount, Writeable content, DocWriteRequest.OpType opType, boolean newChunk) {
        assert Thread.holdsLock(this);
        Chunk chunk = chunks.peekLast();
        if (chunk == null || newChunk || chunk.requests.size() >= PAGES_PER_BULK) {
            chunk = new Chunk();
            chunks.add(chunk);
        }
        TransportVersion version = clusterService.state().getMinTransportVersion();
        long[] written = new long[1];
        try {
            EscfRowBuffer row = chunk.builder.beginRow();
            row.stringField(ASYNC_ID_FIELD, docIdBytes, 0, docIdBytes.length);
            row.longField(PAGE_SEQ_FIELD, seq);
            row.longField(FIRST_ROW_FIELD, firstRow);
            row.longField(ROW_COUNT_FIELD, rowCount);
            row.longField(TRANSPORT_VERSION_FIELD, version.id());
            if (seq == 0) {
                row.longField(EXPIRATION_TIME_FIELD, expirationTime);
            }
            row.binaryField(PAGE_FIELD, out -> {
                out.setTransportVersion(version);
                long start = out.position();
                content.writeTo(out);
                written[0] = out.position() - start;
            });
            row.finishRow();
            chunk.builder.commit(0);
        } catch (Exception e) {
            fail(e);
            return;
        }
        String id = seq == 0 ? docId : AsyncResultPagesIndex.pageDocId(docId, seq);
        chunk.requests.add(new IndexRequest(AsyncResultPagesIndex.INDEX).id(id).opType(opType));
        chunk.bytes += written[0];
        unackedBytes += written[0];
    }

    private void flush() {
        Chunk chunk;
        boolean last;
        synchronized (this) {
            if (bulkInFlight || chunks.isEmpty() || failure != null) {
                return;
            }
            chunk = chunks.poll();
            last = finished && chunks.isEmpty();
            bulkInFlight = true;
        }
        final long sentBytes = chunk.bytes;
        BulkRequest bulk = new BulkRequest();
        EscfBatch batch = null;
        try (chunk) {
            batch = chunk.builder.buildPartition(0);
            for (int i = 0; i < chunk.requests.size(); i++) {
                IndexRequest request = chunk.requests.get(i);
                request.indexSource().setSourceRow(batch, i, XContentType.JSON);
                bulk.add(request);
            }
            // the bulk owns the batch from here on
            bulk.setPreBuiltBatches(Map.of(AsyncResultPagesIndex.INDEX, batch));
            batch = null;
        } catch (Exception e) {
            Releasables.close(batch);
            onBulkDone(sentBytes, last, e);
            return;
        }
        client.bulk(
            bulk,
            ActionListener.wrap(response -> onBulkDone(sentBytes, last, firstFailure(response)), e -> onBulkDone(sentBytes, last, e))
        );
    }

    private void onBulkDone(long sentBytes, boolean last, @Nullable Exception e) {
        synchronized (this) {
            bulkInFlight = false;
            unackedBytes -= sentBytes;
            if (e != null) {
                fail(e);
            }
        }
        publisher.observerCapacityAvailable();
        if (e == null && last) {
            client.admin()
                .indices()
                .prepareRefresh(AsyncResultPagesIndex.INDEX)
                .execute(ActionListener.wrap(r -> {}, ex -> logger.warn(() -> "failed to refresh stored result [" + docId + "]", ex)));
        } else {
            flush();
        }
    }

    private void fail(Exception e) {
        assert Thread.holdsLock(this);
        if (failure == null) {
            failure = e;
            logger.warn(() -> "failed to store result [" + docId + "]", e);
        }
        Releasables.close(chunks);
        chunks.clear();
        unackedBytes = 0;
    }

    @Nullable
    private static Exception firstFailure(BulkResponse response) {
        if (response.hasFailures() == false) {
            return null;
        }
        for (BulkItemResponse item : response.getItems()) {
            if (item.isFailed()) {
                return item.getFailure().getCause();
            }
        }
        return null;
    }
}
