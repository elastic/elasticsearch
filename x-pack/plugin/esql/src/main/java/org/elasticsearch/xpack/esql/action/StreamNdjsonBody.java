/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultResponse;
import org.elasticsearch.xpack.esql.streaming.StreamMetadata;
import org.elasticsearch.xpack.esql.streaming.StreamPages;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.NdjsonRowWriter.NDJSON_CONTENT_TYPE;
import static org.elasticsearch.xpack.esql.action.NdjsonRowWriter.NEWLINE;

/**
 * Writes the result of a stream read as NDJSON, one line per logical unit:
 * <ul>
 *   <li>First line: {@code {"id":..., "columns":[...], "query_status":"running|completed|failed|cancelled"}}</li>
 *   <li>One line per row: a JSON array with the values in column order, {@code null} where there is no value</li>
 *   <li>Last line: {@code {"status":200,"is_partial":false,"warnings":[...],"complete":false,"next_offset":N}}, with an {@code error}
 *       object when the query failed</li>
 * </ul>
 * <p>
 * The pages were read from the storage, in one call, before the response started. The body is pull based: the HTTP layer asks for the
 * next part when it has written the previous one, so a slow client slows the writing and nothing is buffered ahead of the socket.
 * <p>
 * {@code max_rows} and {@code max_bytes} are counted while rows are written, and the first one reached ends the response at a row
 * boundary. When a limit is not set there is no limit. Bytes count the rows only, including their newline, not the first and last
 * lines. A row is always written when a limit is not yet reached, so a row larger than {@code max_bytes} is still written.
 * <p>
 * Pages are owned by this body until their part is fully written, and are released by the part, by {@link #close()} or when a limit
 * is reached.
 */
public final class StreamNdjsonBody implements Releasable {

    private static final Logger logger = LogManager.getLogger(StreamNdjsonBody.class);

    private final RestChannel channel;
    private final GetStreamResultRequest request;
    private final List<ColumnInfoImpl> columns;

    private final Object lock = new Object();
    /** Pages read but not yet handed to a part. Guarded by {@code lock}. */
    private final ArrayDeque<Page> pending = new ArrayDeque<>();
    /** The page of the part being written. Guarded by {@code lock}. */
    private Page inFlight;
    private boolean closed;

    private final StreamMetadata metadata;
    private volatile long rowsWritten;
    private volatile long bytesWritten;

    public StreamNdjsonBody(RestChannel channel, GetStreamResultResponse response) {
        this.channel = channel;
        this.request = response.request();
        this.metadata = response.metadata();
        this.columns = new ArrayList<>(metadata.columns().size());
        for (ColumnInfo column : metadata.columns()) {
            // The stored columns are written by the ES|QL plugin, so they are always ColumnInfoImpl, which the value converters need.
            if (column instanceof ColumnInfoImpl impl) {
                columns.add(impl);
            } else {
                response.close();
                throw new IllegalStateException("unexpected column implementation [" + column.getClass().getName() + "]");
            }
        }
        StreamPages read = response.takePages();
        synchronized (lock) {
            pending.addAll(read.takePages());
        }
    }

    public ChunkedRestResponseBodyPart firstPart() {
        return new ColumnsPart();
    }

    private boolean limitReached() {
        return (request.hasRowLimit() && rowsWritten >= request.maxRows())
            || (request.hasByteLimit() && bytesWritten >= request.maxBytes());
    }

    /** Counts the rows and their bytes as they are written, and stops the rows of a page when a limit is reached. */
    private final NdjsonRowWriter.RowLimit limit = new NdjsonRowWriter.RowLimit() {
        @Override
        public boolean reached() {
            return limitReached();
        }

        @Override
        public void rowWritten(int bytes) {
            rowsWritten++;
            bytesWritten += bytes;
        }
    };

    /** Produces the part after the one just written: the next page, or the last line when no page is left or a limit was reached. */
    private void next(ActionListener<ChunkedRestResponseBodyPart> listener) {
        try {
            if (request.statusOnly() || limitReached()) {
                releasePending();
                listener.onResponse(new FooterPart());
                return;
            }
            Page page;
            synchronized (lock) {
                if (closed) {
                    listener.onFailure(new IllegalStateException("the response was closed"));
                    return;
                }
                page = pending.poll();
                inFlight = page;
            }
            listener.onResponse(page != null ? new PagePart(page) : new FooterPart());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void releasePending() {
        synchronized (lock) {
            for (Page page : pending) {
                page.releaseBlocks();
            }
            pending.clear();
        }
    }

    private void releaseInFlight(Page page) {
        synchronized (lock) {
            if (inFlight == page) {
                inFlight = null;
                page.releaseBlocks();
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
            for (Page page : pending) {
                page.releaseBlocks();
            }
            pending.clear();
            if (inFlight != null) {
                inFlight.releaseBlocks();
                inFlight = null;
            }
        }
    }

    private static String queryStatus(StreamMetadata.Status status) {
        return switch (status) {
            case RUNNING -> "running";
            case COMPLETE -> "completed";
            case FAILED -> "failed";
            case CANCELED -> "cancelled";
        };
    }

    private abstract static class SingleLinePart implements ChunkedRestResponseBodyPart {
        private boolean encoded = false;

        abstract void writeLine(XContentBuilder builder) throws IOException;

        @Override
        public boolean isPartComplete() {
            return encoded;
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
            try {
                try (XContentBuilder builder = XContentFactory.jsonBuilder(Streams.noCloseStream(out))) {
                    writeLine(builder);
                }
                out.write(NEWLINE);
                encoded = true;
                return out.moveToBytesReference();
            } catch (Exception e) {
                encoded = true;
                IOUtils.closeWhileHandlingException(out);
                throw e;
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }

    private final class ColumnsPart extends SingleLinePart {
        @Override
        void writeLine(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("id", request.streamId());
            builder.startArray("columns");
            for (ColumnInfoImpl column : columns) {
                column.toXContent(builder, channel.request());
            }
            builder.endArray();
            builder.field("query_status", queryStatus(metadata.status()));
            builder.endObject();
        }

        @Override
        public boolean isLastPart() {
            return false;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            next(listener);
        }
    }

    private final class FooterPart extends SingleLinePart {

        @Override
        void writeLine(XContentBuilder builder) throws IOException {
            StreamMetadata meta = metadata;
            StreamMetadata.StreamCompletionInfo completion = meta.completionInfo();
            long nextOffset = request.offset() + rowsWritten;

            StreamMetadata.StreamFailure failure = null;
            if (meta.status() == StreamMetadata.Status.FAILED && completion != null) {
                failure = completion.failure();
            }
            // The row count can be missing from the stored metadata. Then the end of the result cannot be checked against it.
            Long totalRows = completion != null ? completion.rows() : null;
            // All the stored pages were written, no limit stopped the response, and still rows are missing: the storage lost pages.
            if (failure == null
                && request.statusOnly() == false
                && limitReached() == false
                && meta.status() == StreamMetadata.Status.COMPLETE
                && totalRows != null
                && nextOffset < totalRows) {
                failure = new StreamMetadata.StreamFailure(
                    ElasticsearchException.getExceptionName(new IllegalStateException()),
                    "stream ["
                        + request.streamId()
                        + "] is complete with ["
                        + totalRows
                        + "] rows but only has rows up to ["
                        + nextOffset
                        + "]",
                    RestStatus.INTERNAL_SERVER_ERROR
                );
            }
            // With a known row count the result is complete once every row was written. Without one, it is complete when the read of
            // a complete stream was written to its end, that is when no limit stopped the response.
            boolean complete = failure == null
                && meta.status() == StreamMetadata.Status.COMPLETE
                && (totalRows != null ? nextOffset >= totalRows : request.statusOnly() == false && limitReached() == false);
            boolean partial = failure != null
                || meta.status() == StreamMetadata.Status.FAILED
                || (meta.status() == StreamMetadata.Status.COMPLETE && completion != null && completion.partial());
            int status = failure != null ? failure.status().getStatus() : RestStatus.OK.getStatus();

            builder.startObject();
            builder.field("status", status);
            builder.field("is_partial", partial);
            builder.array("warnings", meta.warnings().toArray(String[]::new));
            builder.field("complete", complete);
            builder.field("next_offset", nextOffset);
            if (failure != null) {
                builder.startObject("error");
                builder.field("type", failure.type());
                builder.field("reason", failure.reason());
                builder.endObject();
            }
            builder.endObject();
        }

        @Override
        public boolean isLastPart() {
            return true;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert false : "the last line has no continuation";
            listener.onFailure(new IllegalStateException("no continuations available"));
        }
    }

    private final class PagePart implements ChunkedRestResponseBodyPart {
        private final Page page;
        private final NdjsonRowWriter rows;
        private boolean encoded = false;

        PagePart(Page page) {
            this.page = page;
            // The time zone of the query is not stored with the stream, so dates are written in UTC.
            this.rows = new NdjsonRowWriter(page, columns, null, ZoneOffset.UTC);
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
            next(listener);
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
            try {
                // Rows are written until the chunk reaches sizeHint or a limit is reached. The limits are checked before each row
                // for max_rows and after each row for max_bytes, so at least one row is written whenever no limit is reached yet.
                rows.writeRows(chunkStream, channel.request(), sizeHint, limit);
                if (rows.allRowsWritten() || limitReached()) {
                    // Rows after a reached limit are dropped with the page.
                    encoded = true;
                }
                return chunkStream.moveToBytesReference();
            } catch (Exception e) {
                logger.warn(() -> Strings.format("failure encoding a page of stream [%s]", request.streamId()), e);
                encoded = true;
                IOUtils.closeWhileHandlingException(chunkStream);
                throw e;
            } finally {
                if (encoded) {
                    releaseInFlight(page);
                }
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }
}
