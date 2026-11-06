/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;

/**
 * The page run: a framed run whose records are pages, written with {@link SpillPages} at the writer's transport
 * version. A page is handed over whole: the writer takes its own reference and releases it once the page's bytes are in
 * the stream, so the operator may drop the page at once and its memory goes back when the last reference does.
 */
public final class PageRunWriter {

    private final FramedRunWriter writer;

    public PageRunWriter(FramedRunWriter writer) {
        this.writer = writer;
    }

    public FramedRunWriter writer() {
        return writer;
    }

    /**
     * Queues a page into a partition; the listener gets the record's offset once the page is serialized.
     */
    public void append(int partition, Page page, ActionListener<Long> listener) {
        page.allowPassingToDifferentDriver();
        Page copy = page.shallowCopy();
        writer.append(partition, out -> SpillPages.write(copy, out), copy::releaseBlocks, listener);
    }

    public void finish(ActionListener<FramedRun> listener) {
        writer.finish(listener);
    }

    public void abort(ActionListener<Void> listener) {
        writer.abort(listener);
    }
}
