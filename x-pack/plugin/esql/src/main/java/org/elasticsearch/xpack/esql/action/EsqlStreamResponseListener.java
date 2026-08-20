/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * REST listener for the streaming ES|QL query endpoint. Subscribes to a {@link PageStreamPublisher}
 * and streams results as NDJSON to the HTTP client, one JSON line per logical unit:
 *   - First line: {@code {"columns":[...]}}
 *   - One line per page: {@code {"values":[[...],...]}}
 *   - Last line: {@code {"took":N,"is_partial":false,"warnings":["..."]}}
 *   - On error: {@code {"error":{"type":"...","reason":"..."},"status":N}}
 */
public class EsqlStreamResponseListener implements ActionListener<ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(EsqlStreamResponseListener.class);
    private static final String NDJSON_CONTENT_TYPE = "application/x-ndjson";
    private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);

    private final RestChannel channel;
    private final AtomicBoolean terminalEmitted = new AtomicBoolean(false);
    private volatile boolean streamStarted = false;
    private final StreamingSubscriber subscriber = new StreamingSubscriber();

    private final Object continuationMonitor = new Object();
    private ActionListener<ChunkedRestResponseBodyPart> nextBodyPartListener;
    private ChunkedRestResponseBodyPart pendingTerminalPart;

    private volatile PageStreamPublisher publisher;
    private volatile List<ColumnInfoImpl> columns;
    private volatile boolean[] nullColumns;
    private final AtomicReference<Page> inFlightPage = new AtomicReference<>();

    public EsqlStreamResponseListener(RestChannel channel) {
        this.channel = channel;
    }

    public ActionListener<EsqlStreamQueryAction.StreamStart> streamStartListener() {
        return ActionListener.wrap(this::initializeStream, this::onFailure);
    }

    @Override
    public void onResponse(ActionResponse.Empty empty) {
        // Compute has finished; the footer was already delivered through publisher.completeWithFooter.
    }

    private void initializeStream(EsqlStreamQueryAction.StreamStart streamStart) throws IOException {
        this.publisher = streamStart.publisher();
        this.columns = streamStart.columns();
        this.nullColumns = streamStart.nullColumns();
        NdjsonColumnsBodyPart columnsBodyPart = new NdjsonColumnsBodyPart(streamStart.columns(), streamStart.nullColumns());
        streamStart.publisher().subscribe(subscriber);
        channel.sendResponse(RestResponse.chunked(RestStatus.OK, columnsBodyPart, this::release));
        streamStarted = true;
    }

    private void release() {
        Flow.Subscription subscription = subscriber.subscription;
        try {
            if (subscription != null) {
                subscription.cancel();
            }
        } finally {
            Page page = inFlightPage.getAndSet(null);
            if (page != null) {
                page.releaseBlocks();
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            if (streamStarted) {
                logger.debug("transport failure after stream started; delivering the error via the publisher", e);
                return;
            }
            if (terminalEmitted.compareAndSet(false, true) == false) {
                logger.debug("failure response already sent; discarding duplicate onFailure", e);
                return;
            }
            RestStatus status = ExceptionsHelper.status(e);
            channel.sendResponse(RestResponse.chunked(status, new NdjsonErrorBodyPart(e, status), this::release));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error("failed to send failure response", inner);
        } finally {
            PageStreamPublisher p = publisher;
            if (p != null) {
                p.failStream(e);
            }
        }
    }

    private void requestNextChunk(ActionListener<ChunkedRestResponseBodyPart> listener) {
        ChunkedRestResponseBodyPart terminal;
        synchronized (continuationMonitor) {
            terminal = pendingTerminalPart;
            if (terminal != null) {
                pendingTerminalPart = null;
            } else {
                nextBodyPartListener = listener;
            }
        }
        if (terminal != null) {
            listener.onResponse(terminal);
        } else {
            // IMPORTANT: subscription.request(1) must be called *after* releasing continuationMonitor.
            // PageStreamPublisher.deliverPages() calls subscriber.onNext() outside its own monitor, and
            // onNext() acquires continuationMonitor. If request(1) were called while holding
            // continuationMonitor the lock order would be continuationMonitor → publisher-monitor in this
            // direction but publisher-monitor → continuationMonitor in deliverPages(), creating a deadlock.
            Flow.Subscription subscription = subscriber.subscription;
            assert subscription != null : "requestNextChunk before onSubscribe; initializeStream must subscribe before sendResponse";
            subscription.request(1);
        }
    }

    private class StreamingSubscriber implements Flow.Subscriber<Page> {
        private volatile Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void onNext(Page page) {
            ActionListener<ChunkedRestResponseBodyPart> next;
            synchronized (continuationMonitor) {
                next = nextBodyPartListener;
                nextBodyPartListener = null;
            }
            if (next == null) {
                page.releaseBlocks();
                return;
            }
            Page previous = inFlightPage.getAndSet(page);
            assert previous == null : "a page is already in flight; demand must be one page at a time";
            next.onResponse(new NdjsonPageBodyPart(page, columns, nullColumns));
        }

        @Override
        public void onError(Throwable throwable) {
            if (terminalEmitted.compareAndSet(false, true)) {
                Exception e = throwable instanceof Exception ex ? ex : new RuntimeException(throwable);
                RestStatus status = ExceptionsHelper.status(e);
                ChunkedRestResponseBodyPart errorPart = new NdjsonErrorBodyPart(e, status);
                ActionListener<ChunkedRestResponseBodyPart> next;
                synchronized (continuationMonitor) {
                    next = nextBodyPartListener;
                    if (next != null) {
                        nextBodyPartListener = null;
                    } else {
                        pendingTerminalPart = errorPart;
                    }
                }
                if (next != null) {
                    next.onResponse(errorPart);
                }
            }
        }

        @Override
        public void onComplete() {
            if (terminalEmitted.compareAndSet(false, true)) {
                PageStreamPublisher.StreamFooter footer = publisher.footer();
                ChunkedRestResponseBodyPart footerPart = new NdjsonFooterBodyPart(footer);
                ActionListener<ChunkedRestResponseBodyPart> next;
                synchronized (continuationMonitor) {
                    next = nextBodyPartListener;
                    if (next != null) {
                        nextBodyPartListener = null;
                    } else {
                        pendingTerminalPart = footerPart;
                    }
                }
                if (next != null) {
                    next.onResponse(footerPart);
                }
            }
        }
    }

    private class NdjsonColumnsBodyPart implements ChunkedRestResponseBodyPart {
        private final List<ColumnInfoImpl> cols;
        private final boolean[] nullColumns;
        private boolean encoded = false;

        NdjsonColumnsBodyPart(List<ColumnInfoImpl> cols, boolean[] nullColumns) {
            this.cols = cols;
            this.nullColumns = nullColumns;
        }

        @Override
        public boolean isPartComplete() {
            return encoded;
        }

        @Override
        public boolean isLastPart() {
            return false;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            requestNextChunk(listener);
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
            try {
                writeJson(out, builder -> {
                    builder.startObject();
                    if (nullColumns != null) {
                        builder.startArray("all_columns");
                        for (ColumnInfoImpl col : cols) {
                            col.toXContent(builder, channel.request());
                        }
                        builder.endArray();
                        builder.startArray("columns");
                        for (int c = 0; c < cols.size(); c++) {
                            if (nullColumns[c] == false) {
                                cols.get(c).toXContent(builder, channel.request());
                            }
                        }
                        builder.endArray();
                    } else {
                        builder.startArray("columns");
                        for (ColumnInfoImpl col : cols) {
                            col.toXContent(builder, channel.request());
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                });
                out.write(NEWLINE);
                encoded = true;
                return out.moveToBytesReference();
            } catch (Exception e) {
                logger.error("failure encoding columns chunk", e);
                IOUtils.closeWhileHandlingException(out);
                throw e;
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }

    private class NdjsonPageBodyPart implements ChunkedRestResponseBodyPart {
        private final Page page;
        private final List<ColumnInfoImpl> cols;
        private final boolean[] nullColumns;
        private boolean encoded = false;

        NdjsonPageBodyPart(Page page, List<ColumnInfoImpl> cols, boolean[] nullColumns) {
            this.page = page;
            this.cols = cols;
            this.nullColumns = nullColumns;
        }

        @Override
        public boolean isPartComplete() {
            return encoded;
        }

        @Override
        public boolean isLastPart() {
            return false;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            requestNextChunk(listener);
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            if (inFlightPage.compareAndSet(page, null) == false) {
                throw new IllegalStateException("in-flight page was already released; the response is torn down");
            }
            final RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
            try {
                final int rowCount = page.getPositionCount();
                final int colCount = cols.size();
                final BytesRef scratch = new BytesRef();
                final PositionToXContent[] converters = new PositionToXContent[colCount];
                for (int c = 0; c < colCount; c++) {
                    if (nullColumns == null || nullColumns[c] == false) {
                        converters[c] = PositionToXContent.positionToXContent(cols.get(c), page.getBlock(c), ZoneOffset.UTC, scratch);
                    }
                }
                writeJson(out, builder -> {
                    builder.startObject();
                    builder.startArray("values");
                    for (int row = 0; row < rowCount; row++) {
                        builder.startArray();
                        for (int col = 0; col < colCount; col++) {
                            if (converters[col] != null) {
                                converters[col].positionToXContent(builder, channel.request(), row);
                            }
                        }
                        builder.endArray();
                    }
                    builder.endArray();
                    builder.endObject();
                });
                out.write(NEWLINE);
                encoded = true;
                return out.moveToBytesReference();
            } catch (Exception e) {
                logger.error("failure encoding page chunk", e);
                IOUtils.closeWhileHandlingException(out);
                throw e;
            } finally {
                page.releaseBlocks();
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }

    private static class NdjsonFooterBodyPart implements ChunkedRestResponseBodyPart {
        private final PageStreamPublisher.StreamFooter footer;
        private boolean encoded = false;

        NdjsonFooterBodyPart(PageStreamPublisher.StreamFooter footer) {
            this.footer = footer;
        }

        @Override
        public boolean isPartComplete() {
            return encoded;
        }

        @Override
        public boolean isLastPart() {
            return true;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert false : "no continuations";
            listener.onFailure(new IllegalStateException("no continuations available"));
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
            try {
                writeJson(out, builder -> {
                    builder.startObject();
                    if (footer != null) {
                        builder.field("took", footer.tookMillis());
                        builder.field(EsqlExecutionInfo.IS_PARTIAL_FIELD.getPreferredName(), footer.isPartial());
                        if (footer.warnings().isEmpty() == false) {
                            builder.array("warnings", footer.warnings().toArray(String[]::new));
                        }
                    }
                    builder.endObject();
                });
                out.write(NEWLINE);
                encoded = true;
                return out.moveToBytesReference();
            } catch (Exception e) {
                IOUtils.closeWhileHandlingException(out);
                throw e;
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }

    private static class NdjsonErrorBodyPart implements ChunkedRestResponseBodyPart {
        private final Throwable error;
        private final RestStatus status;
        private boolean encoded = false;

        NdjsonErrorBodyPart(Throwable error, RestStatus status) {
            this.error = error;
            this.status = status;
        }

        @Override
        public boolean isPartComplete() {
            return encoded;
        }

        @Override
        public boolean isLastPart() {
            return true;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert false : "no continuations";
            listener.onFailure(new IllegalStateException("no continuations available"));
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
            try {
                writeJson(out, builder -> {
                    builder.startObject();
                    builder.startObject("error");
                    Throwable cause = ExceptionsHelper.unwrapCause(error);
                    String type = ElasticsearchException.getExceptionName(cause);
                    String reason = error instanceof ElasticsearchException ese
                        ? ese.getDetailedMessage()
                        : (error.getMessage() != null ? error.getMessage() : type);
                    builder.field("type", type);
                    builder.field("reason", reason);
                    builder.endObject();
                    builder.field("status", status.getStatus());
                    builder.endObject();
                });
                out.write(NEWLINE);
                encoded = true;
                return out.moveToBytesReference();
            } catch (Exception e) {
                IOUtils.closeWhileHandlingException(out);
                throw e;
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }

    @FunctionalInterface
    private interface JsonWriter {
        void write(XContentBuilder builder) throws IOException;
    }

    private static void writeJson(RecyclerBytesStreamOutput out, JsonWriter writer) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }
        })) {
            writer.write(builder);
        }
    }
}
