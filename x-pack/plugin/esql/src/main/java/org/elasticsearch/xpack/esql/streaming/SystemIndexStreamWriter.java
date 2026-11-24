/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.EscfBulkService;
import org.elasticsearch.action.bulk.EscfBulkWriter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;
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
 * document twice. Failed items are logged and do not stop the stream. Use {@link #onCapacity} to wait until more pages may be
 * appended.
 */
public class SystemIndexStreamWriter implements StoredStreamWriter {

    private static final Logger logger = LogManager.getLogger(SystemIndexStreamWriter.class);

    private final TransportVersion version;
    private final EscfBulkWriter writer;
    private final EscfBulkWriter.Producer producer;
    private final List<Runnable> waiting = new ArrayList<>();

    private volatile StreamMetadata started;
    private boolean terminated;

    @SuppressWarnings("this-escape")
    public SystemIndexStreamWriter(Client client, EscfBulkService bulks, CircuitBreaker breaker, TransportVersion version) {
        this.version = version;
        EscfBulkWriter.Policy defaultPolicy = bulks.defaultPolicy();
        this.writer = bulks.newWriter(
            new OriginSettingClient(client, ESQL_ORIGIN),
            breaker,
            INDEX,
            new EscfBulkWriter.Policy(1, defaultPolicy.targetBulkBytes(), defaultPolicy.maxNodeBytes()),
            SystemIndexStreamWriter::logFailedItems,
            this::wake
        );
        this.producer = writer.newProducer();
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

    /**
     * Runs {@code runnable} once more pages may be appended, right away if they may now.
     */
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
        List<Runnable> ready;
        synchronized (this) {
            if (waiting.isEmpty() || producer.hasCapacity() == false) {
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
        writer.finish(ActionListener.wrap(ignored -> {}, e -> logger.warn("failed to store stream results", e)));
    }

    private static Exception logFailedItems(BulkResponse response) {
        if (response.hasFailures()) {
            logger.warn("failed to store stream results: {}", response.buildFailureMessage());
        }
        return null;
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
            if (metadata.expiration() != null) {
                row.longField(STREAM_EXPIRATION_TIME_FIELD, metadata.expiration().toEpochMilli());
            }
            stringField(row, STREAM_STATUS, metadata.status().name());
            stringField(row, STREAM_COLUMNS, withJsonBuilder(json -> {
                json.startArray();
                for (ColumnInfo column : metadata.columns()) {
                    column.toXContent(json, ToXContent.EMPTY_PARAMS);
                }
                return json.endArray();
            }));
            stringField(row, STREAM_HEADERS, withJsonBuilder(json -> json.map(metadata.headers())));
            stringField(row, STREAM_WARNINGS, withJsonBuilder(json -> json.value(metadata.warnings())));
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
                    stringField(
                        row,
                        STREAM_FAILURE,
                        withJsonBuilder(
                            json -> json.startObject()
                                .field("type", failure.type())
                                .field("reason", failure.reason())
                                .field("status", failure.status().getStatus())
                                .endObject()
                        )
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
        private interface JsonStringBuilder {
            XContentBuilder apply(XContentBuilder builder) throws IOException;
        }

        private static String withJsonBuilder(JsonStringBuilder builder) {
            try {
                return Strings.toString(builder.apply(jsonBuilder()));
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
