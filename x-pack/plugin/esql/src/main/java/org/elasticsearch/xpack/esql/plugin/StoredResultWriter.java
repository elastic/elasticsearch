/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.async.AsyncResultPagesIndex;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.ASYNC_ID_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.FIRST_ROW_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.PAGE_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.ROW_COUNT_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.TRANSPORT_VERSION_FIELD;

/**
 * Stores the result of a streaming query while it is streamed. Page {@code 0}, the {@link StoredResultMetadata},
 * is written first; each page of rows is then serialized as it is handed to the client and indexed in the
 * background, one bulk at a time. Only the serialized bytes wait for the bulk to be acknowledged, and producers
 * are held back while too many bytes are waiting. Page {@code 0} is rewritten last, once the query is done, and
 * the index is then refreshed so the whole result is visible. Pages are serialized with the minimum transport version
 * of the cluster so that any node can read them.
 */
final class StoredResultWriter implements PageStreamPublisher.PageObserver {

    private static final Logger logger = LogManager.getLogger(StoredResultWriter.class);

    static final int PAGES_PER_BULK = 16;
    static final long MAX_UNACKED_BYTES = ByteSizeValue.ofMb(4).getBytes();

    private record Pending(IndexRequest request, long bytes) {}

    private final Client client;
    private final ClusterService clusterService;
    private final String docId;
    private final StoredResultMetadata running;
    private final long expirationTime;
    private final PageStreamPublisher publisher;

    // Guarded by this
    private final ArrayDeque<Pending> queue = new ArrayDeque<>();
    private long unackedBytes;
    private boolean bulkInFlight;
    private int nextSeq = 1;
    private long nextRow;
    private boolean finished;
    private Exception failure;

    StoredResultWriter(
        Client client,
        ClusterService clusterService,
        String docId,
        StoredResultMetadata running,
        long expirationTime,
        PageStreamPublisher publisher
    ) {
        this.client = new OriginSettingClient(client, ASYNC_SEARCH_ORIGIN);
        this.clusterService = clusterService;
        this.docId = docId;
        this.running = running;
        this.expirationTime = expirationTime;
        this.publisher = publisher;
    }

    /**
     * Writes page {@code 0} before any page of rows.
     */
    void start() {
        enqueue(() -> metadataRequest(running));
        flush();
    }

    @Override
    public void onPage(Page page) {
        enqueue(() -> {
            IndexRequest request = pageRequest(nextSeq, nextRow, page);
            nextSeq++;
            nextRow += page.getPositionCount();
            return request;
        });
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
        enqueue(() -> {
            finished = true;
            StoredResultMetadata.Status status;
            if (error != null) {
                status = StoredResultMetadata.Status.FAILED;
            } else if (footer == null) {
                status = StoredResultMetadata.Status.CANCELLED;
            } else {
                status = StoredResultMetadata.Status.COMPLETED;
            }
            return metadataRequest(
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
        });
        flush();
    }

    private interface RequestSupplier {
        IndexRequest get() throws IOException;
    }

    private synchronized void enqueue(RequestSupplier supplier) {
        if (failure != null) {
            return;
        }
        try {
            IndexRequest request = supplier.get();
            long bytes = request.source().length();
            queue.add(new Pending(request, bytes));
            unackedBytes += bytes;
        } catch (IOException e) {
            fail(e);
        }
    }

    private void flush() {
        BulkRequest bulk;
        long bytes = 0;
        boolean last;
        synchronized (this) {
            if (bulkInFlight || queue.isEmpty() || failure != null) {
                return;
            }
            bulk = new BulkRequest();
            for (int i = 0; i < PAGES_PER_BULK && queue.isEmpty() == false; i++) {
                Pending pending = queue.poll();
                bulk.add(pending.request);
                bytes += pending.bytes;
            }
            last = finished && queue.isEmpty();
            bulkInFlight = true;
        }
        final long sentBytes = bytes;
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
        queue.clear();
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

    private IndexRequest metadataRequest(StoredResultMetadata metadata) throws IOException {
        return request(docId, 0, 0, 0, metadata).opType(DocWriteRequest.OpType.INDEX);
    }

    private IndexRequest pageRequest(int seq, long firstRow, Page page) throws IOException {
        return request(AsyncResultPagesIndex.pageDocId(docId, seq), seq, firstRow, page.getPositionCount(), page).opType(
            DocWriteRequest.OpType.CREATE
        );
    }

    private IndexRequest request(String id, int seq, long firstRow, int rowCount, Writeable content) throws IOException {
        TransportVersion version = clusterService.state().getMinTransportVersion();
        BytesReference bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            content.writeTo(out);
            bytes = out.bytes();
        }
        XContentBuilder source = XContentFactory.cborBuilder()
            .startObject()
            .field(ASYNC_ID_FIELD, docId)
            .field(PAGE_SEQ_FIELD, seq)
            .field(FIRST_ROW_FIELD, firstRow)
            .field(ROW_COUNT_FIELD, rowCount)
            .field(TRANSPORT_VERSION_FIELD, version.id());
        if (seq == 0) {
            source.field(EXPIRATION_TIME_FIELD, expirationTime);
        }
        source.field(PAGE_FIELD, BytesReference.toBytes(bytes)).endObject();
        return new IndexRequest(AsyncResultPagesIndex.INDEX).id(id).source(source);
    }
}
