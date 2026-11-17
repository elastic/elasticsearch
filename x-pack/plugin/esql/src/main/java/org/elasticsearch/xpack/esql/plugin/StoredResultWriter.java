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
import org.elasticsearch.action.support.WriteRequest;
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
import org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;
import static org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex.FIRST_ROW_FIELD;
import static org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex.PAGE_FIELD;
import static org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex.ROW_COUNT_FIELD;
import static org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex.STREAM_ID_FIELD;
import static org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex.TRANSPORT_VERSION_FIELD;

/**
 * Stores the result of a streaming query while it is streamed. Each page is serialized into a columnar batch as it is handed to
 * the client, at the cluster's minimum transport version so that any node can read it, and indexed in the background, one bulk
 * at a time. Producers are held back while too many bytes wait for a bulk to be acknowledged.
 * <p>
 * Page {@code 0}, the {@link StoredResultMetadata}, is written first and rewritten with how the query ended, in a last bulk that
 * refreshes the result's shard. If storing fails, the pages not sent yet are dropped and page {@code 0} says so.
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
    private final String streamId;
    private final byte[] streamIdBytes;
    private final StoredResultMetadata running;
    private final long expirationTime;
    private final Runnable onCapacity;

    // Guarded by this; unackedBytes and failure are also read without it, by hasCapacity
    private final ArrayDeque<Chunk> chunks = new ArrayDeque<>();
    private volatile long unackedBytes;
    private boolean bulkInFlight;
    private int nextSeq = 1;
    private long nextRow;
    // no page is added once finished
    private boolean finished;
    private volatile Exception failure;

    /**
     * @param expirationTime when the result expires, in epoch millis, or {@code -1} for none
     * @param onCapacity called when {@link #hasCapacity()} may have turned true
     */
    StoredResultWriter(
        Client client,
        ClusterService clusterService,
        Recycler<BytesRef> recycler,
        String streamId,
        StoredResultMetadata running,
        long expirationTime,
        Runnable onCapacity
    ) {
        this.client = new OriginSettingClient(client, ESQL_ORIGIN);
        this.clusterService = clusterService;
        this.recycler = recycler;
        this.streamId = streamId;
        this.streamIdBytes = streamId.getBytes(StandardCharsets.UTF_8);
        this.running = running;
        this.expirationTime = expirationTime;
        this.onCapacity = onCapacity;
    }

    /**
     * Writes page {@code 0} before any page of rows.
     */
    void start() {
        synchronized (this) {
            addMetadata(running);
        }
        flush();
    }

    @Override
    public void onPage(Page page) {
        synchronized (this) {
            // a page can still be delivered while the stream is cancelled
            if (finished == false) {
                add(nextSeq, nextRow, page.getPositionCount(), page);
                nextSeq++;
                nextRow += page.getPositionCount();
            }
        }
        flush();
    }

    @Override
    public boolean hasCapacity() {
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
            if (failure == null) {
                finished = true;
                addMetadata(ended(status, nextSeq - 1, nextRow, footer, error));
            }
        }
        flush();
    }

    private StoredResultMetadata ended(
        StoredResultMetadata.Status status,
        int pageCount,
        long rowCount,
        @Nullable PageStreamPublisher.StreamFooter footer,
        @Nullable Exception error
    ) {
        return new StoredResultMetadata(
            running.headers(),
            running.task(),
            running.columns(),
            running.zoneId(),
            status,
            pageCount,
            rowCount,
            footer == null ? 0 : footer.tookMillis(),
            footer == null || footer.isPartial(),
            footer == null ? List.of() : footer.warnings(),
            error
        );
    }

    /**
     * Page {@code 0} is written twice, and a batch cannot hold the same document twice, so it starts a batch of its own.
     */
    private void addMetadata(StoredResultMetadata metadata) {
        assert Thread.holdsLock(this);
        add(0, 0, 0, metadata);
    }

    private void add(int seq, long offset, int size, Writeable content) {
        assert Thread.holdsLock(this);
        boolean metadata = seq == 0;
        Chunk chunk = chunks.peekLast();
        if (chunk == null || metadata || chunk.requests.size() >= PAGES_PER_BULK) {
            chunk = new Chunk();
            chunks.add(chunk);
        }
        TransportVersion version = clusterService.state().getMinTransportVersion();
        long[] written = new long[1];
        try {
            EscfRowBuffer row = chunk.builder.beginRow();
            row.stringField(STREAM_ID_FIELD, streamIdBytes, 0, streamIdBytes.length);
            row.longField(PAGE_SEQ_FIELD, seq);
            row.longField(FIRST_ROW_FIELD, offset);
            row.longField(ROW_COUNT_FIELD, size);
            row.longField(TRANSPORT_VERSION_FIELD, version.id());
            if (metadata && expirationTime >= 0) {
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
            // the chunk holds a partial row
            chunks.removeLastOccurrence(chunk);
            unackedBytes -= chunk.bytes;
            chunk.close();
            fail(e);
            return;
        }
        chunk.requests.add(
            new IndexRequest(EsqlStreamResultsIndex.INDEX) //
                .id(metadata ? streamId : EsqlStreamResultsIndex.pageDocId(streamId, seq))
                .routing(streamId)
                .opType(metadata ? DocWriteRequest.OpType.INDEX : DocWriteRequest.OpType.CREATE)
        );
        chunk.bytes += written[0];
        unackedBytes += written[0];
    }

    private void flush() {
        Chunk chunk;
        boolean last;
        synchronized (this) {
            if (bulkInFlight || chunks.isEmpty()) {
                return;
            }
            chunk = chunks.poll();
            last = finished && chunks.isEmpty();
            bulkInFlight = true;
        }
        final long sentBytes = chunk.bytes;
        // the last bulk makes the whole result visible, refreshing its shard only
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(
            last ? WriteRequest.RefreshPolicy.IMMEDIATE : WriteRequest.RefreshPolicy.NONE
        );
        EscfBatch batch = null;
        try (chunk) {
            batch = chunk.builder.buildPartition(0);
            for (int i = 0; i < chunk.requests.size(); i++) {
                IndexRequest request = chunk.requests.get(i);
                request.indexSource().setSourceRow(batch, i, XContentType.JSON);
                bulk.add(request);
            }
            // the bulk owns the batch from here on
            bulk.setPreBuiltBatches(Map.of(EsqlStreamResultsIndex.INDEX, batch));
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
        onCapacity.run();
        flush();
    }

    /**
     * Drops the pages not sent yet and queues page {@code 0} saying that storing failed, sent once no bulk is in flight. Only the
     * first failure does so: a later one, of that page too, is only logged.
     */
    private void fail(Exception e) {
        assert Thread.holdsLock(this);
        if (failure != null) {
            logger.debug(() -> "failed to store result [" + streamId + "] after it failed", e);
            return;
        }
        failure = e;
        finished = true;
        logger.warn(() -> "failed to store result [" + streamId + "]", e);
        for (Chunk chunk : chunks) {
            unackedBytes -= chunk.bytes;
        }
        Releasables.close(chunks);
        chunks.clear();
        // none of the pages can be trusted to be stored
        addMetadata(ended(StoredResultMetadata.Status.FAILED, 0, 0, null, e));
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
