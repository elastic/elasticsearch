/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link SourceOperator} that replays an already-materialized list of {@link Page}s, so a coordinator {@code Driver} can post-process
 * the pages the compute produced (e.g. {@code ExpandUnmappedFieldsOperator}).
 *
 * <p>Each {@link #getOutput} emits at most {@code maxPageRows} rows: a page that fits is handed through whole, a larger one is sliced
 * into consecutive row chunks across successive calls. Pairing this bounded chunk per call with a one-iteration driver budget (see
 * {@code ComputeService#expandUnmappedFields}) makes the driver re-dispatch onto the worker pool every {@code maxPageRows} rows, so a
 * long expansion of a single wide page cannot hold a worker thread for its whole scan — the chunked re-dispatch
 * <a href="https://github.com/elastic/elasticsearch/issues/160286">#160286</a> calls for. A result's row count is already bounded by
 * {@code esql.query.result_truncation_max_size}, so this caps the per-dispatch work regardless of {@code page_size}.
 *
 * <p>The operator takes ownership of the pages. A whole page handed through has its slot nulled; a sliced page is released once its last
 * chunk has been emitted (the row-range slices are independent copies - see {@link org.elasticsearch.compute.data.Block#slice}). So
 * {@link #close} releases the pages not yet emitted, including a partially sliced current page. Page release is idempotent regardless.
 */
final class PageListSourceOperator extends SourceOperator {
    private final List<Page> pages;
    private final int maxPageRows;
    /** Index of the next page to read; a slot is nulled once that page has been fully emitted. */
    private int next = 0;
    /** Row offset within {@code pages.get(next)} when the current page is being emitted in chunks; 0 when positioned at a page start. */
    private int offset = 0;
    private boolean finished = false;

    /**
     * @param maxPageRows the maximum number of rows emitted per {@link #getOutput}; a larger page is sliced into chunks of this size.
     */
    PageListSourceOperator(List<Page> pages, int maxPageRows) {
        assert maxPageRows > 0 : "maxPageRows must be positive but was " + maxPageRows;
        this.pages = new ArrayList<>(pages);
        this.maxPageRows = maxPageRows;
    }

    @Override
    public Page getOutput() {
        if (finished || next >= pages.size()) {
            return null;
        }
        Page page = pages.get(next);
        int rowCount = page.getPositionCount();
        // A page that already fits in one chunk is handed straight downstream with no copy, matching LimitOperator's pass-through.
        if (offset == 0 && rowCount <= maxPageRows) {
            pages.set(next, null);
            next++;
            return page;
        }
        // A partial row range always returns an independent copy, so releasing the original once its last chunk is emitted is correct.
        int end = Math.min(offset + maxPageRows, rowCount);
        Page chunk = page.slice(offset, end);
        offset = end;
        if (offset >= rowCount) {
            page.releaseBlocks();
            pages.set(next, null);
            next++;
            offset = 0;
        }
        return chunk;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished || next >= pages.size();
    }

    @Override
    public void close() {
        List<Page> remaining = new ArrayList<>(pages);
        pages.clear();
        Releasables.closeExpectNoException(remaining);
    }

    @Override
    public String toString() {
        return "PageListSourceOperator[pages=" + pages.size() + ", maxPageRows=" + maxPageRows + "]";
    }
}
