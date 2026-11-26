/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.EscfBulkService;
import org.elasticsearch.action.bulk.EscfBulkWriter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.INDEX;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_OFFSET_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_SIZE_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.PAGE_TRANSPORT_VERSION_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_COLUMNS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_FAILURE;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_HEADERS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_ID_FIELD;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_PARTIAL;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_STATUS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TASK;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOOK;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOTAL_PAGES;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_TOTAL_ROWS;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.STREAM_WARNINGS;

/**
 * A {@link StoredStreamWriter} that keeps the stream in the system index defined by {@link EsqlStreamResultsIndexDescriptor}:
 * one metadata document and one document per page. Everything goes through one {@link EscfBulkWriter} with one bulk in flight,
 * so the rows are indexed in the order they are added. Each metadata row starts a new batch, so that a bulk never holds the same
 * document twice. A failed bulk or item fails the writer: it drops the rows it has not sent and passes the failure to the callback
 * once. Use {@link #onCapacity} to wait until more pages may be appended.
 */
public class SystemIndexStreamWriter implements StoredStreamWriter {

    private final TransportVersion version;
    private final EscfBulkWriter writer;
    private final EscfBulkWriter.Producer producer;
    private final Consumer<Exception> onFailure;
    private final AtomicBoolean failureReported = new AtomicBoolean();
    private final List<Runnable> waiting = new ArrayList<>();

    private volatile StreamMetadata started;
    private boolean terminated;

    /**
     * @param client the client that sends the bulks. It must be scoped to the project that owns the stream and set the ESQL origin.
     */
    @SuppressWarnings("this-escape")
    public SystemIndexStreamWriter(
        Client client,
        EscfBulkService escfBulkService,
        CircuitBreaker breaker,
        TransportVersion version,
        Consumer<Exception> onFailure
    ) {
        this.version = version;
        this.onFailure = onFailure;
        this.writer = escfBulkService.newWriter(
            client,
            breaker,
            INDEX,
            createPolicy(escfBulkService.defaultPolicy()),
            this::failure,
            this::wake
        );
        this.producer = writer.newProducer();
    }

    private static EscfBulkWriter.Policy createPolicy(EscfBulkWriter.Policy defaultPolicy) {
        return new EscfBulkWriter.Policy(1, defaultPolicy.targetBulkBytes(), defaultPolicy.maxNodeBytes());
    }

    @Override
    public String createStarted(
        TaskId taskId,
        Instant expiration,
        List<ColumnInfo> columns,
        Map<String, String> headers,
        List<String> warnings
    ) {
        var streamId = StreamId.newStreamId(taskId).encoded();
        synchronized (this) {
            started = StreamMetadata.started(streamId, taskId, expiration, columns, headers, warnings);
            producer.add(new MetadataRow(started), true);
        }
        return streamId;
    }

    @Override
    public void appendPage(int seq, long offset, Page page) {
        synchronized (this) {
            if (started == null || terminated) {
                return;
            }
            producer.add(new PageRow(started.streamId(), seq, offset, page, version), false);
        }
    }

    @Override
    public void markComplete(boolean partial, long tookMillis, long pages, long rows) {
        terminate(() -> started.complete(partial, tookMillis, pages, rows));
    }

    @Override
    public void markFailed(boolean partial, long tookMillis, Exception e) {
        terminate(() -> started.failed(partial, tookMillis, e));
    }

    @Override
    public void markCanceled() {
        terminate(() -> started.canceled());
    }

    public void onCapacity(Runnable runnable) {
        synchronized (this) {
            if (producer.hasCapacity() == false) {
                waiting.add(runnable);
                return;
            }
        }
        runnable.run();
    }

    private void wake() {
        reportFailure();
        runWaiting(false);
    }

    private void reportFailure() {
        Exception failure = writer.failure();
        if (failure != null && failureReported.compareAndSet(false, true)) {
            onFailure.accept(failure);
        }
    }

    private void runWaiting(boolean force) {
        List<Runnable> ready;
        synchronized (this) {
            if (waiting.isEmpty() || (force == false && producer.hasCapacity() == false)) {
                return;
            }
            ready = List.copyOf(waiting);
            waiting.clear();
        }
        for (Runnable runnable : ready) {
            runnable.run();
        }
    }

    private void terminate(Supplier<StreamMetadata> finalStatus) {
        synchronized (this) {
            if (started == null || terminated) {
                return;
            }
            terminated = true;
            producer.add(new MetadataRow(finalStatus.get()), true);
            producer.close();
        }
        writer.finish(ActionListener.runAfter(ActionListener.wrap(ignored -> {}, e -> reportFailure()), () -> runWaiting(true)));
    }

    private Exception failure(BulkResponse response) {
        if (response.hasFailures() && hasUnexpectedFailures(response, started.streamId())) {
            return new ElasticsearchException("failed to store stream results: [{}]", response.buildFailureMessage());
        }
        return null;
    }

    private static boolean hasUnexpectedFailures(BulkResponse response, String streamId) {
        List<BulkItemResponse> failed = Arrays.stream(response.getItems()).filter(BulkItemResponse::isFailed).toList();
        return switch (failed.size()) {
            case 0 -> false;
            case 1 -> failed.getFirst().getId().equals(streamId) == false
                || failed.getFirst().getFailure().getStatus() != RestStatus.CONFLICT;
            default -> true;
        };
    }

    private record MetadataRow(StreamMetadata metadata) implements EscfBulkWriter.Rows {

        @Override
        public int count() {
            return 1;
        }

        @Override
        public void append(EscfBatchBuilder builder) {
            String streamId = metadata.streamId();
            EscfRowBuffer row = builder.beginRow();
            stringField(row, STREAM_ID_FIELD, streamId);
            row.longField(PAGE_SEQ_FIELD, -1);
            stringField(row, STREAM_TASK, metadata.taskId().toString());
            row.longField(STREAM_EXPIRATION_TIME_FIELD, metadata.expiration().toEpochMilli());
            stringField(row, STREAM_STATUS, metadata.status().name());
            jsonField(row, STREAM_COLUMNS, json -> {
                json.startArray();
                for (ColumnInfo column : metadata.columns()) {
                    column.toXContent(json, ToXContent.EMPTY_PARAMS);
                }
                return json.endArray();
            });
            jsonField(row, STREAM_HEADERS, json -> json.map(metadata.headers()));
            jsonField(row, STREAM_WARNINGS, json -> json.value(metadata.warnings()));
            StreamMetadata.StreamCompletionInfo completion = metadata.completionInfo();
            if (completion != null) {
                row.booleanField(STREAM_PARTIAL, completion.partial());
                row.longField(STREAM_TOOK, completion.tookMillis());
                if (completion.pages() != null) {
                    row.longField(STREAM_TOTAL_PAGES, completion.pages());
                }
                if (completion.rows() != null) {
                    row.longField(STREAM_TOTAL_ROWS, completion.rows());
                }
                StreamMetadata.StreamFailure failure = completion.failure();
                if (failure != null) {
                    jsonField(
                        row,
                        STREAM_FAILURE,
                        json -> json.startObject()
                            .field("type", failure.type())
                            .field("reason", failure.reason())
                            .field("status", failure.status().getStatus())
                            .endObject()
                    );
                }
            }
            row.finishRow();
            builder.commit(0);
        }

        @Override
        public IndexRequest request(int row) {
            OpType opType = isFinalStatus() ? OpType.INDEX : OpType.CREATE;
            return new IndexRequest(INDEX).id(metadata.streamId()).routing(metadata.streamId()).opType(opType);
        }

        @Override
        public boolean refresh() {
            return isFinalStatus();
        }

        private boolean isFinalStatus() {
            return metadata.status() != StreamMetadata.Status.RUNNING;
        }

        @FunctionalInterface
        private interface JsonContent {
            XContentBuilder apply(XContentBuilder builder) throws IOException;
        }

        private static void jsonField(EscfRowBuffer row, String name, JsonContent content) {
            try {
                BytesRef bytes = BytesReference.bytes(content.apply(jsonBuilder(new BytesStreamOutput()))).toBytesRef();
                row.stringField(name, bytes.bytes, bytes.offset, bytes.length);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static void stringField(EscfRowBuffer row, String name, String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            row.stringField(name, bytes, 0, bytes.length);
        }
    }

    private record PageRow(String streamId, int seq, long offset, Page page, TransportVersion version) implements EscfBulkWriter.Rows {

        @Override
        public int count() {
            return 1;
        }

        @Override
        public void append(EscfBatchBuilder builder) {
            byte[] streamIdBytes = streamId.getBytes(StandardCharsets.UTF_8);
            EscfRowBuffer row = builder.beginRow();
            row.stringField(STREAM_ID_FIELD, streamIdBytes, 0, streamIdBytes.length);
            row.longField(PAGE_SEQ_FIELD, seq);
            row.longField(PAGE_OFFSET_FIELD, offset);
            row.longField(PAGE_SIZE_FIELD, page.getPositionCount());
            row.longField(PAGE_TRANSPORT_VERSION_FIELD, version.id());
            row.binaryField(PAGE_FIELD, out -> {
                out.setTransportVersion(version);
                page.writeTo(out);
            });
            row.finishRow();
            builder.commit(0);
        }

        @Override
        public IndexRequest request(int row) {
            return new IndexRequest(INDEX).id(streamId + "_" + seq).routing(streamId).opType(OpType.CREATE);
        }
    }
}
