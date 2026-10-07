/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.PositionToXContent;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Renders a finished {@link EsqlQueryResponse} as {@link NdjsonFormat}: the counterpart of
 * {@link org.elasticsearch.xpack.esql.formatter.arrow.ArrowResponse} for the non-streaming path.
 *
 * <p>The whole result is available up front, so this is a single {@link ChunkedRestResponseBodyPart} written in chunks. It
 * produces a {@code columns} line, then {@code values} lines of {@code batchSize} rows each, then the footer. Lines are
 * not cut at page boundaries: {@code batchSize} counts rows across the whole result, so it means the same thing whether or
 * not the query was streamed. A line may span several {@code encodeChunk} calls.
 *
 * <p>The footer matches the streaming footer ({@code status}, {@code took}, {@code is_partial}, {@code warnings}, the
 * execution statistics, and {@code profile} when requested), with {@code _clusters} added before the statistics when
 * execution metadata was requested. The response is not released here; the owner of the {@link EsqlQueryResponse} reference
 * does that once the body has been sent.
 */
public final class NdjsonResponse implements ChunkedRestResponseBodyPart {
    private enum Phase {
        COLUMNS,
        VALUES,
        FOOTER,
        DONE
    }

    private final EsqlQueryResponse response;
    private final List<ColumnInfoImpl> columns;
    private final List<Page> pages;
    private final ZoneId zoneId;
    private final int batchSize;
    private final boolean[] nullColumns;
    private final List<String> warnings;
    private final ToXContent.Params params;

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

    private Phase phase = Phase.COLUMNS;
    private XContentBuilder builder;                         // the open values or footer line; null between lines

    private final BytesRef scratch = new BytesRef();
    private int pageIndex = 0;                               // values cursor: the page the next row comes from
    private int rowInPage = 0;                               // values cursor: the next row within that page
    private PositionToXContent[] converters;                 // for pages.get(pageIndex); null until the page is first read
    private int rowsInLine = 0;                              // rows already written to the open values line

    private Iterator<? extends ToXContent> footerContent;    // built when the footer line is opened

    /**
     * @param dropNullColumns whether to drop columns that are null in every row, the same way {@code drop_null_columns} does for
     *                        JSON. The decision is made from the finished result, not from index metadata.
     * @param warnings        the warnings to put in the footer
     */
    public NdjsonResponse(
        EsqlQueryResponse response,
        int batchSize,
        boolean dropNullColumns,
        List<String> warnings,
        ToXContent.Params params
    ) {
        this.response = response;
        this.columns = response.columns();
        this.pages = response.pages();
        this.zoneId = response.zoneId();
        this.batchSize = batchSize;
        this.nullColumns = dropNullColumns ? response.nullColumns() : null;
        this.warnings = warnings;
        this.params = params;
    }

    @Override
    public boolean isPartComplete() {
        return phase == Phase.DONE;
    }

    @Override
    public boolean isLastPart() {
        // Even if sent in chunks, the entirety of ES|QL data is available, so it's a single (chunked) part.
        return true;
    }

    @Override
    public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
        assert false : "no continuations";
        listener.onFailure(new IllegalStateException("no continuations available"));
    }

    @Override
    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
        final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
        target = chunkStream;
        try {
            while (phase != Phase.DONE) {
                switch (phase) {
                    case COLUMNS -> encodeColumns(chunkStream);
                    case VALUES -> encodeValues(chunkStream, sizeHint);
                    case FOOTER -> encodeFooter(chunkStream, sizeHint);
                    case DONE -> throw new AssertionError("unreachable");
                }
                if (chunkStream.size() >= sizeHint) {
                    break;
                }
            }
            final var result = chunkStream.moveToBytesReference();
            target = null;
            return result;
        } catch (Exception e) {
            phase = Phase.DONE;
            if (builder != null) {
                IOUtils.closeWhileHandlingException(builder);
                builder = null;
            }
            IOUtils.closeWhileHandlingException(chunkStream);
            target = null;
            throw e;
        }
    }

    private void encodeColumns(RecyclerBytesStreamOutput chunkStream) throws IOException {
        NdjsonLines.writeJson(chunkStream, b -> NdjsonLines.writeColumns(b, columns, nullColumns, params));
        chunkStream.write(NdjsonLines.NEWLINE);
        phase = hasMoreRows() ? Phase.VALUES : Phase.FOOTER;
    }

    /** Writes rows into the open values line, opening it if needed, until the line is full, the rows run out, or the chunk is full. */
    private void encodeValues(RecyclerBytesStreamOutput chunkStream, int sizeHint) throws IOException {
        if (builder == null) {
            builder = XContentFactory.jsonBuilder(out);
            builder.startObject();
            builder.startArray("values");
            rowsInLine = 0;
        }
        while (rowsInLine < batchSize && hasMoreRows()) {
            Page page = pages.get(pageIndex);
            if (converters == null) {
                converters = NdjsonLines.converters(columns, page, nullColumns, zoneId, scratch);
            }
            NdjsonLines.writeRow(builder, params, converters, rowInPage);
            rowInPage++;
            rowsInLine++;
            builder.flush();
            if (chunkStream.size() >= sizeHint) {
                break;
            }
        }
        if (rowsInLine == batchSize || hasMoreRows() == false) {
            builder.endArray();
            builder.endObject();
            builder.close();
            builder = null;
            chunkStream.write(NdjsonLines.NEWLINE);
            phase = hasMoreRows() ? Phase.VALUES : Phase.FOOTER;
        }
    }

    private void encodeFooter(RecyclerBytesStreamOutput chunkStream, int sizeHint) throws IOException {
        if (builder == null) {
            builder = XContentFactory.jsonBuilder(out);
            builder.startObject();
            footerContent = footerContent();
        }
        while (footerContent.hasNext()) {
            footerContent.next().toXContent(builder, params);
            builder.flush();
            if (chunkStream.size() >= sizeHint) {
                break;
            }
        }
        if (footerContent.hasNext() == false) {
            builder.endObject();
            builder.close();
            builder = null;
            chunkStream.write(NdjsonLines.NEWLINE);
            phase = Phase.DONE;
        }
    }

    private Iterator<? extends ToXContent> footerContent() {
        EsqlExecutionInfo executionInfo = response.getExecutionInfo();
        long tookMillis = executionInfo != null && executionInfo.overallTook() != null ? executionInfo.overallTook().millis() : 0L;

        Iterator<? extends ToXContent> status = Iterators.single((b, p) -> {
            NdjsonLines.writeStatusFields(b, 200, tookMillis, response.isPartial(), warnings);
            return b;
        });
        Iterator<? extends ToXContent> clusters = executionInfo != null && executionInfo.hasMetadataToReport()
            ? ChunkedToXContentHelper.field("_clusters", executionInfo, params)
            : Collections.emptyIterator();
        Iterator<? extends ToXContent> stats = Iterators.single((b, p) -> {
            NdjsonLines.writeStats(
                b,
                response.documentsFound(),
                response.valuesLoaded(),
                response.rowsEmitted(),
                response.bytesRead(),
                response.readNanos(),
                response.cpuNanos()
            );
            return b;
        });
        Iterator<? extends ToXContent> profile = response.profile() != null
            ? EsqlQueryResponse.profileXContent(response.profile(), executionInfo).toXContentChunked(params)
            : Collections.emptyIterator();
        return Iterators.concat(status, clusters, stats, profile);
    }

    /** Whether any row remains, moving the cursor past pages that are exhausted or empty. */
    private boolean hasMoreRows() {
        while (pageIndex < pages.size() && rowInPage >= pages.get(pageIndex).getPositionCount()) {
            pageIndex++;
            rowInPage = 0;
            converters = null;
        }
        return pageIndex < pages.size();
    }

    @Override
    public String getResponseContentTypeString() {
        return NdjsonFormat.CONTENT_TYPE;
    }
}
