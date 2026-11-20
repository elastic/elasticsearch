/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.List;

/**
 * Writes the rows of one page as NDJSON: one line per row, a JSON array with the values in column order. Both stream endpoints write
 * their rows through this class, {@code POST /_query/stream} and {@code GET /_query/stream/{id}}, so a row looks the same in both.
 * <p>
 * A page is usually written over several chunks. The writer keeps its position in the page, so the next call continues with the row
 * after the last one written.
 * <p>
 * The writer does not own the page. The caller keeps the page alive while rows are written and releases it afterwards. The writer also
 * does not know where pages come from or why a response stops early: a caller with limits passes a {@link RowLimit}.
 * <p>
 * Not thread safe. The HTTP layer writes the chunks of one part one after the other.
 */
final class NdjsonRowWriter {

    static final String NDJSON_CONTENT_TYPE = "application/x-ndjson";
    static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);

    /** Lets the caller stop the rows before the page ends, and count what was written. */
    interface RowLimit {

        /** No limit: every row of the page is written. */
        RowLimit NONE = new RowLimit() {
            @Override
            public boolean reached() {
                return false;
            }

            @Override
            public void rowWritten(int bytes) {}
        };

        /** Checked before each row, with the rows written so far counted. When true, no more rows of this page are written. */
        boolean reached();

        /** Called after each row with the size of its line, including the newline. */
        void rowWritten(int bytes);
    }

    private final Page page;
    private final List<ColumnInfoImpl> columns;
    @Nullable
    private final boolean[] nullColumns;
    private final ZoneId zoneId;

    private PositionToXContent[] converters;
    private int nextRow = 0;
    private boolean stoppedAtLimit = false;

    /**
     * @param nullColumns the columns to leave out of every row, or {@code null} to write every column
     * @param zoneId      the time zone that dates are written in
     */
    NdjsonRowWriter(Page page, List<ColumnInfoImpl> columns, @Nullable boolean[] nullColumns, ZoneId zoneId) {
        this.page = page;
        this.columns = columns;
        this.nullColumns = nullColumns;
        this.zoneId = zoneId;
    }

    /** True when every row of the page has been written. */
    boolean allRowsWritten() {
        return nextRow == page.getPositionCount();
    }

    /**
     * True when the last call of {@link #writeRows} ended because the limit was reached while rows of the page were still unwritten.
     * A limit that is reached by the last row of the page does not count: nothing of this page is left out.
     */
    boolean stoppedAtLimit() {
        return stoppedAtLimit;
    }

    /**
     * Writes rows into the chunk until the chunk reaches {@code sizeHint}, the page ends, or the limit is reached. The size is checked
     * after each row, so a call writes at least one row unless the page has ended or the limit is already reached. Without that, a
     * small {@code sizeHint} would make no progress.
     * <p>
     * When a chunk is full and rows remain, the limit is checked once more before returning, see {@link #stoppedAtLimit()}. A reached
     * limit is therefore reported by the call that wrote the last row, and the caller is not asked for one more chunk that would be
     * empty.
     *
     * @return the number of rows written by this call
     */
    int writeRows(RecyclerBytesStreamOutput chunk, ToXContent.Params params, int sizeHint, RowLimit limit) throws IOException {
        final int rowCount = page.getPositionCount();
        final int colCount = columns.size();
        if (converters == null) {
            // Built once, on the first chunk, and reused for every row of the page.
            BytesRef scratch = new BytesRef();
            PositionToXContent[] built = new PositionToXContent[colCount];
            for (int c = 0; c < colCount; c++) {
                if (nullColumns == null || nullColumns[c] == false) {
                    built[c] = PositionToXContent.positionToXContent(columns.get(c), page.getBlock(c), zoneId, scratch);
                }
            }
            converters = built;
        }
        // Closing a builder closes its stream, but the chunk must stay open until the caller turns it into the bytes of the response.
        final OutputStream out = Streams.noCloseStream(chunk);
        int rows = 0;
        stoppedAtLimit = false;
        while (nextRow < rowCount) {
            if (limit.reached()) {
                stoppedAtLimit = true;
                break;
            }
            int before = chunk.size();
            // Each row is its own JSON value followed by a newline, so it gets its own builder: a builder cannot write two top-level
            // values without a separator. Closing it flushes its buffer into the chunk, so the size of the chunk is exact after
            // every row. Without that it would only advance in steps of about 8 KB.
            try (XContentBuilder builder = XContentFactory.jsonBuilder(out)) {
                builder.startArray();
                for (int col = 0; col < colCount; col++) {
                    if (converters[col] != null) {
                        converters[col].positionToXContent(builder, params, nextRow);
                    }
                }
                builder.endArray();
            }
            chunk.write(NEWLINE);
            nextRow++;
            rows++;
            limit.rowWritten(chunk.size() - before);
            if (chunk.size() >= sizeHint) {
                stoppedAtLimit = nextRow < rowCount && limit.reached();
                break;
            }
        }
        return rows;
    }
}
