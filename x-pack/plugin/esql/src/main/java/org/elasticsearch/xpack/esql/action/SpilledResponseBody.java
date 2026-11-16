/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.spill.ResultPages;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.formatter.TextFormatter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The REST bodies of a response whose pages are in a run: a sequence of parts, each over what is in memory, the next
 * obtained through the continuation once the next page has been read, since a chunk must be at hand when the REST
 * layer asks for it and an iterator cannot express the wait. The page a part renders is released once the part is
 * complete; on a disconnect the REST layer lets a pending continuation complete before it closes the response, whose
 * close hook then releases the cursor and any page still held.
 */
final class SpilledResponseBody {

    private SpilledResponseBody() {}

    /**
     * The JSON body, and the other content types the builder renders: rows, or columns each over a walk of its own.
     */
    static ChunkedRestResponseBodyPart xContent(EsqlQueryResponse response, RestRequest request, RestChannel channel) throws IOException {
        XContentBody body = new XContentBody(response, request, channel);
        response.addCloseHook(body::close);
        return body.first();
    }

    /**
     * A text body: the delimited and the markdown formats render a page's rows at a time; the table format needs its
     * columns sized by {@link #widths} first.
     */
    static ChunkedRestResponseBodyPart text(TextFormat format, RestRequest request, EsqlQueryResponse response, @Nullable int[] widths) {
        TextBody body = new TextBody(format, request, response, widths);
        response.addCloseHook(body::close);
        return body.first();
    }

    /**
     * Sizes the table format's columns over every row, through a cursor, for a response whose pages are in a run.
     */
    static void widths(EsqlQueryResponse response, ActionListener<int[]> listener) {
        int[] width = TextFormatter.widths(response.columns(), Collections.emptyIterator());
        List<DataType> types = response.columns().stream().map(ColumnInfoImpl::type).toList();
        ResultPages.Cursor cursor = response.result().open();
        new Walk(
            cursor,
            page -> TextFormatter.widen(width, ResponseValueUtils.pagesToValues(types, List.of(page), response.zoneId())),
            ActionListener.runBefore(listener.map(ignored -> width), cursor::close)
        ).advance();
    }

    /**
     * Walks a cursor to its end, one read in flight at a time: in place while the reads complete inline, and on the
     * completing thread otherwise. Each page is released after the step.
     */
    private static final class Walk {
        private final ResultPages.Cursor cursor;
        private final CheckedConsumer<Page, Exception> step;
        private final ActionListener<Void> listener;
        private final AtomicInteger turns = new AtomicInteger();
        private boolean reading;
        private boolean done;

        Walk(ResultPages.Cursor cursor, CheckedConsumer<Page, Exception> step, ActionListener<Void> listener) {
            this.cursor = cursor;
            this.step = step;
            this.listener = listener;
        }

        void advance() {
            if (turns.getAndIncrement() != 0) {
                return;
            }
            do {
                next();
            } while (turns.decrementAndGet() != 0);
        }

        private void next() {
            if (done || reading) {
                return;
            }
            if (cursor.hasNext() == false) {
                done = true;
                listener.onResponse(null);
                return;
            }
            reading = true;
            cursor.next(ActionListener.wrap(page -> {
                try (page) {
                    if (page != null) {
                        step.accept(page);
                    }
                } catch (Exception e) {
                    done = true;
                    listener.onFailure(e);
                    return;
                }
                reading = false;
                advance();
            }, e -> {
                done = true;
                listener.onFailure(e);
            }));
        }
    }

    /**
     * What every body shares: the cursor over the result, the page in flight, and the close that releases both. The
     * reads land on whichever thread the store completes on, and the close may come from the REST layer meanwhile, so
     * the two take the body's monitor.
     */
    private abstract static class Body {
        protected final EsqlQueryResponse response;
        @Nullable
        private ResultPages.Cursor cursor;
        @Nullable
        private Page held;
        private boolean closed;

        Body(EsqlQueryResponse response) {
            this.response = response;
        }

        /**
         * The next page with rows of the current walk, or null at its end; a walk is opened when none is. A page without
         * rows is skipped, since a part must have a chunk to give.
         */
        protected void nextPage(ActionListener<Page> listener) {
            ResultPages.Cursor current;
            synchronized (this) {
                if (closed) {
                    listener.onFailure(new IllegalStateException("the response is closed"));
                    return;
                }
                if (cursor == null) {
                    cursor = response.result().open();
                }
                current = cursor;
            }
            if (current.hasNext() == false) {
                endWalk();
                listener.onResponse(null);
                return;
            }
            current.next(listener.delegateFailureAndWrap((l, page) -> {
                if (page != null && page.getPositionCount() == 0) {
                    page.releaseBlocks();
                    nextPage(l);
                    return;
                }
                boolean keep;
                synchronized (this) {
                    keep = closed == false;
                    if (keep) {
                        held = page;
                    }
                }
                if (keep == false) {
                    if (page != null) {
                        page.releaseBlocks();
                    }
                    l.onFailure(new IllegalStateException("the response is closed"));
                    return;
                }
                l.onResponse(page);
            }));
        }

        /**
         * Ends the current walk, so the next page comes from a fresh cursor: the columnar JSON walks once per column.
         */
        protected void endWalk() {
            ResultPages.Cursor current;
            synchronized (this) {
                current = cursor;
                cursor = null;
            }
            Releasables.close(current);
        }

        /**
         * Releases the page a part rendered, once the part is complete.
         */
        protected void pageDone(Page page) {
            synchronized (this) {
                if (held == page) {
                    held = null;
                }
            }
            page.releaseBlocks();
        }

        void close() {
            ResultPages.Cursor current;
            Page page;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                current = cursor;
                cursor = null;
                page = held;
                held = null;
            }
            Releasables.close(current, page == null ? null : page::releaseBlocks);
        }
    }

    // ---- XContent

    private static final class XContentBody extends Body {
        private final ToXContent.Params params;
        private final XContentBuilder builder;
        @Nullable
        private BytesStream target;
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
        @Nullable
        private final boolean[] nullColumns;
        private final boolean columnar;
        private final List<ColumnInfoImpl> columns;
        private final ZoneId zoneId;
        private final BytesRef scratch = new BytesRef();
        /** The column being walked when columnar, else -1. */
        private int column = -1;

        XContentBody(EsqlQueryResponse response, RestRequest request, RestChannel channel) throws IOException {
            super(response);
            this.params = channel.request();
            this.builder = channel.newBuilder(channel.request().getXContentType(), null, true, Streams.noCloseStream(out));
            boolean dropNullColumns = request.paramAsBoolean(EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION, false);
            this.nullColumns = dropNullColumns ? response.nullColumns() : null;
            this.columnar = response.columnar();
            this.columns = response.columns();
            this.zoneId = response.zoneId();
        }

        ChunkedRestResponseBodyPart first() {
            Iterator<? extends ToXContent> prologue = Iterators.concat(
                response.xContentPrologue(params, nullColumns),
                ChunkedToXContentHelper.chunk((b, p) -> b.startArray("values"))
            );
            if (columnar) {
                column = nextColumn(-1);
                if (column < 0) {
                    return new Part(Iterators.concat(prologue, epilogue()), null, true);
                }
                return new Part(Iterators.concat(prologue, ChunkedToXContentHelper.chunk((b, p) -> b.startArray())), null, false);
            }
            return new Part(prologue, null, false);
        }

        private int nextColumn(int after) {
            for (int c = after + 1; c < columns.size(); c++) {
                if (nullColumns == null || nullColumns[c] == false) {
                    return c;
                }
            }
            return -1;
        }

        private Iterator<? extends ToXContent> epilogue() {
            return Iterators.concat(ChunkedToXContentHelper.chunk((b, p) -> b.endArray()), response.xContentEpilogue(params));
        }

        /**
         * The next part: the next page's rows, or the next column's start, or the epilogue.
         */
        void nextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            nextPage(listener.delegateFailureAndWrap((l, page) -> {
                if (page != null) {
                    l.onResponse(new Part(pageChunks(page), page, false));
                    return;
                }
                if (columnar == false) {
                    l.onResponse(new Part(epilogue(), null, true));
                    return;
                }
                int next = nextColumn(column);
                if (next < 0) {
                    l.onResponse(new Part(Iterators.concat(ChunkedToXContentHelper.chunk((b, p) -> b.endArray()), epilogue()), null, true));
                    return;
                }
                column = next;
                l.onResponse(
                    new Part(
                        Iterators.concat(
                            ChunkedToXContentHelper.chunk((b, p) -> b.endArray()),
                            ChunkedToXContentHelper.chunk((b, p) -> b.startArray())
                        ),
                        null,
                        false
                    )
                );
            }));
        }

        private Iterator<? extends ToXContent> pageChunks(Page page) {
            if (columnar == false) {
                return ResponseXContentUtils.rowValues(columns, List.of(page), nullColumns, zoneId);
            }
            PositionToXContent toXContent = PositionToXContent.positionToXContent(
                columns.get(column),
                page.getBlock(column),
                zoneId,
                scratch
            );
            return Iterators.forRange(
                0,
                page.getPositionCount(),
                position -> (builder, params) -> toXContent.positionToXContent(builder, params, position)
            );
        }

        private final class Part implements ChunkedRestResponseBodyPart {
            private final Iterator<? extends ToXContent> serialization;
            @Nullable
            private Page page;
            private final boolean last;

            Part(Iterator<? extends ToXContent> serialization, @Nullable Page page, boolean last) {
                this.serialization = serialization;
                this.page = page;
                this.last = last;
            }

            @Override
            public boolean isPartComplete() {
                return serialization.hasNext() == false;
            }

            @Override
            public boolean isLastPart() {
                return last;
            }

            @Override
            public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                nextPart(listener);
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                try {
                    final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
                    assert target == null;
                    target = chunkStream;
                    while (serialization.hasNext()) {
                        serialization.next().toXContent(builder, params);
                        if (chunkStream.size() >= sizeHint) {
                            break;
                        }
                    }
                    if (serialization.hasNext() == false) {
                        if (page != null) {
                            pageDone(page);
                            page = null;
                        }
                        if (last) {
                            builder.close();
                        } else {
                            builder.flush();
                        }
                    }
                    final var result = chunkStream.moveToBytesReference();
                    target = null;
                    return result;
                } finally {
                    if (target != null) {
                        assert false : "failure encoding chunk";
                        IOUtils.closeWhileHandlingException(target);
                        target = null;
                    }
                }
            }

            @Override
            public String getResponseContentTypeString() {
                return builder.getResponseContentTypeString();
            }
        }
    }

    // ---- text

    private static final class TextBody extends Body {
        private final TextFormat format;
        private final RestRequest request;
        private final List<DataType> types;
        @Nullable
        private final int[] widths;
        private final boolean[] dropColumns;
        @Nullable
        private RecyclerBytesStreamOutput currentOutput;
        private final Writer writer = new OutputStreamWriter(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                assert currentOutput != null;
                currentOutput.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                assert currentOutput != null;
                currentOutput.write(b, off, len);
            }

            @Override
            public void flush() {
                assert currentOutput != null;
                currentOutput.flush();
            }

            @Override
            public void close() {
                assert currentOutput != null;
                currentOutput.flush();
            }
        }, StandardCharsets.UTF_8);

        TextBody(TextFormat format, RestRequest request, EsqlQueryResponse response, @Nullable int[] widths) {
            super(response);
            this.format = format;
            this.request = request;
            this.types = response.columns().stream().map(ColumnInfoImpl::type).toList();
            this.widths = widths;
            this.dropColumns = TextFormat.dropColumns(request, response);
            if (format == TextFormat.PLAIN_TEXT && widths == null) {
                throw new IllegalArgumentException("the table format needs its columns sized first");
            }
        }

        ChunkedRestResponseBodyPart first() {
            Iterator<CheckedConsumer<Writer, IOException>> header = format == TextFormat.PLAIN_TEXT
                ? new TextFormatter(response.columns(), Collections.emptyIterator(), widths, format.hasHeader(request), dropColumns)
                    .header()
                : format.header(request, response);
            // a part must have a chunk to give, even one that writes nothing
            return new Part(header.hasNext() ? header : nothing(), null, false);
        }

        private static Iterator<CheckedConsumer<Writer, IOException>> nothing() {
            return Iterators.single(writer -> {});
        }

        void nextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            nextPage(listener.delegateFailureAndWrap((l, page) -> {
                if (page == null) {
                    l.onResponse(new Part(nothing(), null, true));
                    return;
                }
                Iterator<Iterator<Object>> values = ResponseValueUtils.pagesToValues(types, List.of(page), response.zoneId());
                Iterator<CheckedConsumer<Writer, IOException>> rows = format == TextFormat.PLAIN_TEXT
                    ? new TextFormatter(response.columns(), values, widths, false, dropColumns).formatResults()
                    : format.rows(request, response, values);
                l.onResponse(new Part(rows, page, false));
            }));
        }

        private final class Part implements ChunkedRestResponseBodyPart {
            private final Iterator<CheckedConsumer<Writer, IOException>> chunks;
            @Nullable
            private Page page;
            private final boolean last;

            Part(Iterator<CheckedConsumer<Writer, IOException>> chunks, @Nullable Page page, boolean last) {
                this.chunks = chunks;
                this.page = page;
                this.last = last;
            }

            @Override
            public boolean isPartComplete() {
                return chunks.hasNext() == false;
            }

            @Override
            public boolean isLastPart() {
                return last;
            }

            @Override
            public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                nextPart(listener);
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                try {
                    assert currentOutput == null;
                    currentOutput = new RecyclerBytesStreamOutput(recycler);
                    while (chunks.hasNext() && currentOutput.size() < sizeHint) {
                        chunks.next().accept(writer);
                    }
                    if (chunks.hasNext() == false) {
                        if (page != null) {
                            pageDone(page);
                            page = null;
                        }
                        if (last) {
                            writer.close();
                        } else {
                            writer.flush();
                        }
                    } else {
                        writer.flush();
                    }
                    final var result = currentOutput.moveToBytesReference();
                    currentOutput = null;
                    return result;
                } finally {
                    if (currentOutput != null) {
                        assert false : "failure encoding chunk";
                        IOUtils.closeWhileHandlingException(currentOutput);
                        currentOutput = null;
                    }
                }
            }

            @Override
            public String getResponseContentTypeString() {
                return format.contentType(request);
            }
        }
    }
}
