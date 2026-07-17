/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
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

/**
 * REST listener for the streaming ES|QL query endpoint. Subscribes to a {@link PageStreamPublisher}
 * and streams results as NDJSON to the HTTP client, one JSON line per logical unit:
 * <ol>
 *   <li>First line: {@code {"columns":[...]}}</li>
 *   <li>One line per page: {@code {"values":[[...],...]}}</li>
 *   <li>Last line: {@code {"took":N,"warnings":["..."]}}</li>
 *   <li>On error: {@code {"error":{"type":"...","reason":"..."},"status":N}}</li>
 * </ol>
 */
public class EsqlStreamResponseListener implements ActionListener<EsqlStreamQueryAction.Response> {

    private static final Logger logger = LogManager.getLogger(EsqlStreamResponseListener.class);
    private static final String NDJSON_CONTENT_TYPE = "application/x-ndjson";
    private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);

    private final RestChannel channel;
    private final AtomicBoolean isLastPart = new AtomicBoolean(false);
    private final StreamingSubscriber subscriber = new StreamingSubscriber();

    /**
     * Listener for the next {@link ChunkedRestResponseBodyPart}. Set by each body part's
     * {@code getNextPart} call, consumed by the subscriber's {@code onNext}/{@code onComplete}/{@code onError}.
     */
    private volatile ActionListener<ChunkedRestResponseBodyPart> nextBodyPartListener;

    private volatile PageStreamPublisher publisher;
    private volatile List<ColumnInfoImpl> columns;

    public EsqlStreamResponseListener(RestChannel channel) {
        this.channel = channel;
    }

    @Override
    public void onResponse(EsqlStreamQueryAction.Response response) {
        try {
            initializeStream(response);
        } catch (Exception e) {
            onFailure(e);
        }
    }

    private void initializeStream(EsqlStreamQueryAction.Response response) throws IOException {
        this.publisher = response.publisher();
        this.columns = response.columns();
        NdjsonColumnsBodyPart columnsBodyPart = new NdjsonColumnsBodyPart(response.columns());
        channel.sendResponse(RestResponse.chunked(RestStatus.OK, columnsBodyPart, this::release));
        // Subscribe after sendResponse so HTTP headers are committed first.
        // onSubscribe stores the subscription but does NOT call request(1).
        // The first request(1) comes from columnsBodyPart.getNextPart().
        response.publisher().subscribe(subscriber);
    }

    private void release() {
        if (subscriber.subscription != null) {
            subscriber.subscription.cancel();
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            isLastPart.set(true);
            RestStatus status = ExceptionsHelper.status(e);
            channel.sendResponse(RestResponse.chunked(status, new NdjsonErrorBodyPart(e, status), this::release));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error("failed to send failure response", inner);
        }
    }

    private void requestNextChunk(ActionListener<ChunkedRestResponseBodyPart> listener) {
        nextBodyPartListener = listener;
        subscriber.subscription.request(1);
    }

    // -------------------------------------------------------------------------
    // Subscriber
    // -------------------------------------------------------------------------

    private class StreamingSubscriber implements Flow.Subscriber<Page> {
        private Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            // Do NOT call request(1) here — the first request comes from columnsBodyPart.getNextPart()
        }

        @Override
        public void onNext(Page page) {
            ActionListener<ChunkedRestResponseBodyPart> next = takeNextBodyPartListener();
            next.onResponse(new NdjsonPageBodyPart(page, columns));
        }

        @Override
        public void onError(Throwable throwable) {
            if (isLastPart.compareAndSet(false, true)) {
                Exception e = throwable instanceof Exception ex ? ex : new RuntimeException(throwable);
                RestStatus status = ExceptionsHelper.status(e);
                ActionListener<ChunkedRestResponseBodyPart> next = takeNextBodyPartListener();
                next.onResponse(new NdjsonErrorBodyPart(e, status));
            }
        }

        @Override
        public void onComplete() {
            if (isLastPart.compareAndSet(false, true)) {
                PageStreamPublisher.StreamFooter footer = publisher.getFooter();
                ActionListener<ChunkedRestResponseBodyPart> next = takeNextBodyPartListener();
                next.onResponse(new NdjsonFooterBodyPart(footer));
            }
        }

        private ActionListener<ChunkedRestResponseBodyPart> takeNextBodyPartListener() {
            ActionListener<ChunkedRestResponseBodyPart> l = nextBodyPartListener;
            nextBodyPartListener = null;
            return l;
        }
    }

    // -------------------------------------------------------------------------
    // Body parts
    // -------------------------------------------------------------------------

    /**
     * Writes the columns header line: {@code {"columns":[...]}} followed by a newline.
     * When the REST framework calls {@code getNextPart}, this triggers {@code request(1)} to
     * start pulling pages from the publisher.
     */
    private class NdjsonColumnsBodyPart implements ChunkedRestResponseBodyPart {
        private final List<ColumnInfoImpl> cols;
        private boolean encoded = false;

        NdjsonColumnsBodyPart(List<ColumnInfoImpl> cols) {
            this.cols = cols;
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
                    builder.startArray("columns");
                    for (ColumnInfoImpl col : cols) {
                        col.toXContent(builder, channel.request());
                    }
                    builder.endArray();
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

    /**
     * Writes one page as a values line: {@code {"values":[[...],...]}} followed by a newline.
     */
    private class NdjsonPageBodyPart implements ChunkedRestResponseBodyPart {
        private final Page page;
        private final List<ColumnInfoImpl> cols;
        private boolean encoded = false;

        NdjsonPageBodyPart(Page page, List<ColumnInfoImpl> cols) {
            this.page = page;
            this.cols = cols;
        }

        @Override
        public boolean isPartComplete() {
            return encoded;
        }

        @Override
        public boolean isLastPart() {
            return isLastPart.get();
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            requestNextChunk(listener);
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
            try {
                final int rowCount = page.getPositionCount();
                final int colCount = cols.size();
                final BytesRef scratch = new BytesRef();
                final PositionToXContent[] converters = new PositionToXContent[colCount];
                for (int c = 0; c < colCount; c++) {
                    converters[c] = PositionToXContent.positionToXContent(cols.get(c), page.getBlock(c), ZoneOffset.UTC, scratch);
                }
                writeJson(out, builder -> {
                    builder.startObject();
                    builder.startArray("values");
                    for (int row = 0; row < rowCount; row++) {
                        builder.startArray();
                        for (int col = 0; col < colCount; col++) {
                            converters[col].positionToXContent(builder, channel.request(), row);
                        }
                        builder.endArray();
                    }
                    builder.endArray();
                    builder.endObject();
                });
                out.write(NEWLINE);
                encoded = true;
                page.releaseBlocks();
                return out.moveToBytesReference();
            } catch (Exception e) {
                logger.error("failure encoding page chunk", e);
                IOUtils.closeWhileHandlingException(out);
                throw e;
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }

    /**
     * Writes the footer line: {@code {"took":N,"warnings":[...]}} followed by a newline.
     * This is the last body part.
     */
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

    /**
     * Writes an error line: {@code {"error":{"type":"...","reason":"..."},"status":N}} followed
     * by a newline. This is the last body part.
     */
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
                    String type = error.getClass().getSimpleName();
                    String reason = error.getMessage() != null ? error.getMessage() : type;
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

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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
