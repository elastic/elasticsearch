/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PageStreamPublisher.StreamFooter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.PositionToXContent;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;

/**
 * The writers for the three kinds of {@link NdjsonFormat} line. The non-streaming {@link NdjsonResponse} and the streaming
 * {@code EsqlStreamResponseListener} both render through these, so the two modes cannot drift apart.
 */
public final class NdjsonLines {
    private static final Logger logger = LogManager.getLogger(NdjsonLines.class);

    public static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);

    private NdjsonLines() {}

    @FunctionalInterface
    public interface JsonWriter {
        void write(XContentBuilder builder) throws IOException;
    }

    /** Writes one complete JSON document into {@code out}, without a trailing newline. */
    public static void writeJson(RecyclerBytesStreamOutput out, JsonWriter writer) throws IOException {
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

    /**
     * The header line: {@code {"columns":[...]}}. When {@code nullColumns} is non-null, which is the {@code drop_null_columns}
     * case, the line carries {@code all_columns} and {@code columns} holds only the columns not flagged in {@code nullColumns}.
     */
    public static void writeColumns(XContentBuilder builder, List<ColumnInfoImpl> cols, boolean[] nullColumns, ToXContent.Params params)
        throws IOException {
        builder.startObject();
        if (nullColumns != null) {
            builder.startArray("all_columns");
            for (ColumnInfoImpl col : cols) {
                col.toXContent(builder, params);
            }
            builder.endArray();
            builder.startArray("columns");
            for (int c = 0; c < cols.size(); c++) {
                if (nullColumns[c] == false) {
                    cols.get(c).toXContent(builder, params);
                }
            }
            builder.endArray();
        } else {
            builder.startArray("columns");
            for (ColumnInfoImpl col : cols) {
                col.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    /**
     * One converter per column of {@code page}, or {@code null} for a column omitted by {@code nullColumns}. Create these once
     * per page and reuse them for every row.
     */
    public static PositionToXContent[] converters(
        List<ColumnInfoImpl> cols,
        Page page,
        boolean[] nullColumns,
        ZoneId zoneId,
        BytesRef scratch
    ) {
        PositionToXContent[] converters = new PositionToXContent[cols.size()];
        for (int c = 0; c < converters.length; c++) {
            if (nullColumns == null || nullColumns[c] == false) {
                converters[c] = PositionToXContent.positionToXContent(cols.get(c), page.getBlock(c), zoneId, scratch);
            }
        }
        return converters;
    }

    /** One row of a {@code values} line: {@code [v1,v2,...]}. */
    public static void writeRow(XContentBuilder builder, ToXContent.Params params, PositionToXContent[] converters, int row)
        throws IOException {
        builder.startArray();
        for (PositionToXContent converter : converters) {
            if (converter != null) {
                converter.positionToXContent(builder, params, row);
            }
        }
        builder.endArray();
    }

    /** The leading fields of a footer line. */
    public static void writeStatusFields(XContentBuilder builder, int status, long tookMillis, boolean isPartial, List<String> warnings)
        throws IOException {
        builder.field("status", status);
        builder.field("took", tookMillis);
        builder.field(EsqlExecutionInfo.IS_PARTIAL_FIELD.getPreferredName(), isPartial);
        builder.array("warnings", warnings.toArray(String[]::new));
    }

    /** The execution statistics fields of a footer line. */
    public static void writeStats(
        XContentBuilder builder,
        long documentsFound,
        long valuesLoaded,
        long rowsEmitted,
        long bytesRead,
        long readNanos,
        long cpuNanos
    ) throws IOException {
        builder.field("documents_found", documentsFound);
        builder.field("values_loaded", valuesLoaded);
        builder.field("rows_emitted", rowsEmitted);
        builder.field("bytes_read", bytesRead);
        builder.field("read_nanos", readNanos);
        builder.field("cpu_nanos", cpuNanos);
    }

    /** The {@code error} object of a failure footer line. */
    public static void writeError(XContentBuilder builder, Exception error) throws IOException {
        builder.startObject("error");
        Throwable cause = ExceptionsHelper.unwrapCause(error);
        String type = ElasticsearchException.getExceptionName(cause);
        String reason = error instanceof ElasticsearchException ese
            ? ese.getDetailedMessage()
            : (error.getMessage() != null ? error.getMessage() : type);
        builder.field("type", type);
        builder.field("reason", reason);
        builder.endObject();
    }

    /**
     * The footer line as a final {@link ChunkedRestResponseBodyPart}. It respects {@code sizeHint} and may span several
     * {@code encodeChunk} calls, because an optional {@code profile} payload has one entry per driver across all nodes.
     */
    public static final class NdjsonFooterBodyPart implements ChunkedRestResponseBodyPart {
        private final StreamFooter footer;
        private final ToXContent.Params params;
        private boolean encoded = false;

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
        private XContentBuilder builder;                          // created on the first encodeChunk
        private Iterator<? extends ToXContent> contentIterator;  // built on the first encodeChunk

        public NdjsonFooterBodyPart(StreamFooter footer, ToXContent.Params params) {
            this.footer = footer;
            this.params = params;
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
            final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
            target = chunkStream;
            try {
                if (builder == null) {
                    builder = XContentFactory.jsonBuilder(out);
                    builder.startObject();
                    Iterator<? extends ToXContent> statusChunk = Iterators.single((b, p) -> {
                        writeStatusFields(b, footer.status(), footer.tookMillis(), footer.isPartial(), footer.warnings());
                        if (footer.completionInfo() != null) {
                            DriverCompletionInfo ci = footer.completionInfo();
                            writeStats(
                                b,
                                ci.documentsFound(),
                                ci.valuesLoaded(),
                                ci.rowsEmitted(),
                                ci.bytesRead(),
                                ci.readNanos(),
                                ci.cpuNanos()
                            );
                        }
                        if (footer.error() != null) {
                            writeError(b, footer.error());
                        }
                        return b;
                    });
                    contentIterator = footer.profile() != null
                        ? Iterators.concat(statusChunk, footer.profile().toXContentChunked(params))
                        : statusChunk;
                }
                while (contentIterator.hasNext()) {
                    contentIterator.next().toXContent(builder, params);
                    builder.flush();
                    if (chunkStream.size() >= sizeHint) {
                        break;
                    }
                }
                if (contentIterator.hasNext() == false) {
                    builder.endObject();
                    builder.close();
                    builder = null;
                    chunkStream.write(NEWLINE);
                    encoded = true;
                }
                final var result = chunkStream.moveToBytesReference();
                target = null;
                return result;
            } catch (Exception e) {
                logger.error("failure encoding footer chunk", e);
                encoded = true;
                if (builder != null) {
                    IOUtils.closeWhileHandlingException(builder);
                    builder = null;
                }
                IOUtils.closeWhileHandlingException(chunkStream);
                target = null;
                throw e;
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NdjsonFormat.CONTENT_TYPE;
        }
    }
}
