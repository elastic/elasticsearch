/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

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

public class SystemIndexStreamWriter implements StoredStreamWriter {

    private static final Logger logger = LogManager.getLogger(SystemIndexStreamWriter.class);

    static final int PAGES_PER_BULK = 16;
    static final long MAX_UNACKED_BYTES = ByteSizeValue.ofMb(4).getBytes();

    private final Client client;
    private final Recycler<BytesRef> recycler;
    private final TransportVersion transportVersion;

    private StreamMetadata metadata = null;
    private Bulk currentBulk = null;
    private int inFlightBulks = 0;
    private final List<Runnable> waiting = new ArrayList<>();

    public SystemIndexStreamWriter(Client client, Recycler<BytesRef> recycler, TransportVersion transportVersion) {
        this.client = new OriginSettingClient(client, ESQL_ORIGIN);
        this.recycler = recycler;
        this.transportVersion = transportVersion;
    }

    @Override
    public String createStarted(
        TaskId taskId,
        Instant expiration,
        List<ColumnInfo> columns,
        Map<String, String> headers,
        List<String> warnings
    ) {
        var streamId = UUID.randomUUID().toString();
        addToCurrentBulk(true, false, bulk -> {
            metadata = StreamMetadata.started(streamId, taskId, expiration, columns, headers, warnings);
            bulk.addMetadata(metadata);
        });
        return streamId;
    }

    @Override
    public void appendPage(int seq, long offset, Page page) {
        addToCurrentBulk(false, false, bulk -> bulk.addPage(seq, offset, page));
    }

    @Override
    public void markComplete(boolean partial, long tookMillis, long pages, long rows) {
        addToCurrentBulk(true, true, bulk -> {
            metadata = metadata.complete(partial, tookMillis, pages, rows);
            bulk.addMetadata(metadata);
        });
    }

    @Override
    public void markFailed(boolean partial, long tookMillis, Exception e) {
        addToCurrentBulk(true, true, bulk -> {
            metadata = metadata.failed(partial, tookMillis, e);
            bulk.addMetadata(metadata);
        });
    }

    @Override
    public void markCanceled() {
        addToCurrentBulk(true, true, bulk -> {
            metadata = metadata.canceled();
            bulk.addMetadata(metadata);
        });
    }

    public void onCapacity(Runnable runnable) {
        synchronized (this) {
            if (inFlightBulks > 0) {
                waiting.add(runnable);
                return;
            }
        }
        runnable.run();
    }

    private void onBulkDone() {
        List<Runnable> ready;
        synchronized (this) {
            inFlightBulks--;
            if (inFlightBulks > 0) {
                return;
            }
            ready = List.copyOf(waiting);
            waiting.clear();
        }
        for (Runnable runnable : ready) {
            runnable.run();
        }
    }

    private synchronized void addToCurrentBulk(boolean force, boolean refresh, Consumer<Bulk> consumer) {
        if (metadata != null && metadata.status() != StreamMetadata.Status.RUNNING) {
            return;
        }
        if (currentBulk == null) {
            currentBulk = new Bulk();
        }
        consumer.accept(currentBulk);
        if (force || !currentBulk.hasCapacity()) {
            flush(refresh);
        }
    }

    private synchronized void flush(boolean refresh) {
        Bulk bulk = currentBulk;
        if (bulk == null) {
            return;
        }
        currentBulk = null;
        BulkRequest request = bulk.toBulkRequest(refresh);
        inFlightBulks++;
        client.bulk(request, ActionListener.runAfter(ActionListener.wrap(response -> {
            if (response.hasFailures()) {
                logger.warn("failed to store stream results: {}", response.buildFailureMessage());
            }
        }, e -> logger.warn("failed to store stream results", e)), this::onBulkDone));
    }

    private final class Bulk implements Releasable {
        private final EscfBatchBuilder builder = new EscfBatchBuilder(recycler);
        private final List<IndexRequest> requests = new ArrayList<>(PAGES_PER_BULK);
        private long bytes;

        boolean hasCapacity() {
            return currentBulk.requests.size() < PAGES_PER_BULK && currentBulk.bytes < MAX_UNACKED_BYTES;
        }

        BulkRequest toBulkRequest(boolean refresh) {
            BulkRequest request = new BulkRequest().setRefreshPolicy(refresh ? RefreshPolicy.IMMEDIATE : RefreshPolicy.NONE);
            EscfBatch batch = null;
            try (builder) {
                batch = builder.buildPartition(0);
                for (int i = 0; i < requests.size(); i++) {
                    IndexRequest indexRequest = requests.get(i);
                    indexRequest.indexSource().setSourceRow(batch, i, XContentType.JSON);
                    request.add(indexRequest);
                }
                request.setPreBuiltBatches(Map.of(INDEX, batch));
                batch = null;
                return request;
            } catch (Exception e) {
                Releasables.close(batch);
                throw e;
            }
        }

        void addMetadata(StreamMetadata metadata) {
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
            requests.add(new IndexRequest(INDEX).id(streamId).routing(streamId).opType(DocWriteRequest.OpType.INDEX));
        }

        void addPage(int seq, long offset, Page page) {
            String streamId = metadata.streamId();
            byte[] streamIdBytes = streamId.getBytes(StandardCharsets.UTF_8);
            EscfRowBuffer row = builder.beginRow();
            row.stringField(STREAM_ID_FIELD, streamIdBytes, 0, streamIdBytes.length);
            row.longField(PAGE_SEQ_FIELD, seq);
            row.longField(PAGE_OFFSET_FIELD, offset);
            row.longField(PAGE_SIZE_FIELD, page.getPositionCount());
            row.longField(PAGE_TRANSPORT_VERSION_FIELD, transportVersion.id());
            row.binaryField(PAGE_FIELD, out -> {
                out.setTransportVersion(transportVersion);
                long start = out.position();
                page.writeTo(out);
                bytes += out.position() - start;
            });
            row.finishRow();
            builder.commit(0);
            requests.add(new IndexRequest(INDEX).id(streamId + "_" + seq).routing(streamId).opType(DocWriteRequest.OpType.CREATE));
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

        @Override
        public void close() {
            builder.close();
        }
    }
}
