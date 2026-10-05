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
import org.elasticsearch.xpack.esql.formatter.NdjsonFormat;
import org.elasticsearch.xpack.esql.formatter.NdjsonLines;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * REST listener for the streaming ES|QL query endpoint. Subscribes to a {@link PageStreamPublisher}
 * and streams results as NDJSON to the HTTP client, one JSON line per logical unit:
 * <ul>
 *   <li>First line: {@code {"columns":[...]}}</li>
 *   <li>One line per page: {@code {"values":[[...],...]}}
 *   <li>Last line (success): {@code {"status":200,"took":N,"is_partial":false,"warnings":[...],"documents_found":N,...}}
 *       optionally followed by a {@code "profile"} object when {@code profile: true} was set.</li>
 *   <li>Last line (failure after header): {@code {"status":N,"took":N,"is_partial":false,"warnings":[...],
 *       "error":{"type":"...","reason":"..."}}}</li>
 *   <li>On pre-header error: same terminal-record shape with an error HTTP status code on the response line itself.</li>
 * </ul>
 *
 * <p>Each logical unit maps to one {@link ChunkedRestResponseBodyPart}. The columns and error parts each
 * emit a single small NDJSON line and ignore the {@code sizeHint} argument to {@code encodeChunk}. The
 * page and footer parts respect {@code sizeHint} and may span multiple {@code encodeChunk} calls: pages
 * because {@code batch_size} is a row count and per-row JSON width is unbounded; the footer because the
 * profile payload (when present) contains one entry per driver across all nodes. Pages are never coalesced
 * across parts: the next page is only available via an async {@code request(1)} call, and buffering to fill
 * a chunk would contradict the client's chosen {@code batch_size}.</p>
 *
 * <p>This class owns flow control only: the subscription, backpressure, page ownership and terminal handling.
 * Every NDJSON byte is written by {@link NdjsonLines}, the same writers that {@code NdjsonResponse} uses when the
 * query is not streamed.</p>
 */
public class EsqlStreamResponseListener implements ActionListener<ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(EsqlStreamResponseListener.class);

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
    private volatile ZoneId zoneId;
    private final AtomicReference<Page> inFlightPage = new AtomicReference<>();

    public EsqlStreamResponseListener(RestChannel channel) {
        this.channel = channel;
    }

    public ActionListener<EsqlStreamQueryAction.ResultStream> resultStreamListener() {
        return ActionListener.wrap(this::initializeStream, this::onFailure);
    }

    @Override
    public void onResponse(ActionResponse.Empty empty) {
        // Compute has finished; the footer was already delivered through publisher.completeWithFooter.
        assert streamStarted : "the transport action completed successfully without ever initializing the stream";
    }

    private void initializeStream(EsqlStreamQueryAction.ResultStream resultStream) throws IOException {
        this.publisher = resultStream.publisher();
        this.columns = resultStream.columns();
        this.nullColumns = resultStream.nullColumns();
        this.zoneId = resultStream.zoneId();
        assert zoneId != null : "ResultStream must carry the resolved query time zone";
        NdjsonColumnsBodyPart columnsBodyPart = new NdjsonColumnsBodyPart(resultStream.columns(), resultStream.nullColumns());
        resultStream.publisher().subscribe(subscriber);
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
            PageStreamPublisher.StreamFooter footer = new PageStreamPublisher.StreamFooter(
                status.getStatus(),
                0L,
                false,
                List.of(),
                null,
                e,
                null
            );
            channel.sendResponse(
                RestResponse.chunked(status, new NdjsonLines.NdjsonFooterBodyPart(footer, channel.request()), this::release)
            );
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
            next.onResponse(new NdjsonPageBodyPart(page, columns, nullColumns, zoneId));
        }

        @Override
        public void onError(Throwable throwable) {
            if (terminalEmitted.compareAndSet(false, true)) {
                Exception e = throwable instanceof Exception ex ? ex : new RuntimeException(throwable);
                PageStreamPublisher.StreamFooter footer = publisher.footer();
                if (footer == null) {
                    footer = new PageStreamPublisher.StreamFooter(
                        ExceptionsHelper.status(e).getStatus(),
                        0L,
                        false,
                        List.of(),
                        null,
                        e,
                        null
                    );
                }
                ChunkedRestResponseBodyPart footerPart = new NdjsonLines.NdjsonFooterBodyPart(footer, channel.request());
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

        @Override
        public void onComplete() {
            if (terminalEmitted.compareAndSet(false, true)) {
                PageStreamPublisher.StreamFooter footer = publisher.footer();
                ChunkedRestResponseBodyPart footerPart = new NdjsonLines.NdjsonFooterBodyPart(footer, channel.request());
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
                NdjsonLines.writeJson(out, builder -> NdjsonLines.writeColumns(builder, cols, nullColumns, channel.request()));
                out.write(NdjsonLines.NEWLINE);
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
            return NdjsonFormat.CONTENT_TYPE;
        }
    }

    private class NdjsonPageBodyPart implements ChunkedRestResponseBodyPart {
        private final Page page;
        private final List<ColumnInfoImpl> cols;
        private final boolean[] nullColumns;
        private final ZoneId zoneId;

        // Resume state across encodeChunk calls.
        // target is the current chunk's output stream; set at the top of each encodeChunk and nulled
        // before returning so that out never writes to a stream that has already been moved/closed.
        private RecyclerBytesStreamOutput target;
        private final OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                target.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                target.write(b, off, len);
            }
        };
        private XContentBuilder builder;           // created on the first encodeChunk; survives across calls
        private PositionToXContent[] converters;   // built once on the first encodeChunk
        private BytesRef scratch;                  // built once on the first encodeChunk
        private int nextRow = 0;                   // resume cursor into the page
        private boolean encoded = false;           // true only after the last row has been written

        NdjsonPageBodyPart(Page page, List<ColumnInfoImpl> cols, boolean[] nullColumns, ZoneId zoneId) {
            this.page = page;
            this.cols = cols;
            this.nullColumns = nullColumns;
            this.zoneId = zoneId;
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
            if (inFlightPage.get() != page) {
                throw new IllegalStateException("in-flight page was already released; the response is torn down");
            }
            final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
            target = chunkStream;
            try {
                final int rowCount = page.getPositionCount();
                if (builder == null) {
                    // First chunk: build converters and open the JSON structure.
                    scratch = new BytesRef();
                    converters = NdjsonLines.converters(cols, page, nullColumns, zoneId, scratch);
                    builder = XContentFactory.jsonBuilder(out);
                    builder.startObject();
                    builder.startArray("values");
                }
                // Emit rows until the chunk reaches sizeHint. builder.flush() pushes Jackson's
                // internal write buffer through out into chunkStream so that size() is accurate
                // to the row just written. Without it size() only advances in ~8 KB steps (Jackson's
                // internal buffer size), so the effective minimum chunk is ~8 KB regardless of
                // sizeHint. Measuring after the row (not before) guarantees at least one row per
                // call, so a part with a tiny sizeHint still makes forward progress.
                while (nextRow < rowCount) {
                    NdjsonLines.writeRow(builder, channel.request(), converters, nextRow);
                    nextRow++;
                    builder.flush();
                    if (chunkStream.size() >= sizeHint) {
                        break;
                    }
                }
                if (nextRow == rowCount) {
                    // Final chunk: close the JSON structure, then write the NDJSON newline.
                    // builder.close() flushes Jackson's internal buffer into chunkStream and must
                    // precede chunkStream.write(NEWLINE) so the newline follows the JSON, not precedes it.
                    builder.endArray();
                    builder.endObject();
                    builder.close();
                    builder = null;
                    chunkStream.write(NdjsonLines.NEWLINE);
                    encoded = true;
                }
                final var result = chunkStream.moveToBytesReference();
                target = null;
                return result;
            } catch (Exception e) {
                logger.error("failure encoding page chunk", e);
                encoded = true; // part is dead; never re-enter it
                // Close builder before chunkStream: builder.close() flushes through out into target
                // (chunkStream); closing chunkStream first would return its pages to the recycler,
                // making any subsequent flush a use-after-recycle.
                if (builder != null) {
                    IOUtils.closeWhileHandlingException(builder);
                    builder = null;
                }
                IOUtils.closeWhileHandlingException(chunkStream);
                target = null;
                releasePage();
                throw e;
            } finally {
                if (encoded) {
                    releasePage();
                }
            }
        }

        private void releasePage() {
            if (inFlightPage.compareAndSet(page, null)) {
                page.releaseBlocks();
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NdjsonFormat.CONTENT_TYPE;
        }
    }
}
