/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * The result of one read from the storage: the metadata as it was just before the pages were read, and the pages that cover the requested
 * range.
 * <p>
 * It owns its pages until {@link #takePages()} hands them on. Closing it releases the pages it still owns, so closing it twice, or after
 * the pages were taken, does nothing. That makes it safe to close on a failure path without knowing how far the pages got.
 */
public final class StreamPages implements Releasable {

    private final StreamMetadata metadata;
    private List<Page> pages;

    /**
     * @param metadata the metadata read <b>before</b> the pages. If its status is terminal, an empty read really is the end of the result.
     * @param pages    shallow copies of the stored pages, trimmed to the requested range
     */
    public StreamPages(StreamMetadata metadata, List<Page> pages) {
        this.metadata = metadata;
        this.pages = pages;
    }

    public StreamMetadata metadata() {
        return metadata;
    }

    /** The pages still owned by this object: empty once they were taken or released. */
    public synchronized List<Page> pages() {
        return pages;
    }

    /** Hands the pages, and their ownership, to the caller. Afterwards this object holds none, and closing it releases nothing. */
    public synchronized List<Page> takePages() {
        List<Page> taken = pages;
        pages = List.of();
        return taken;
    }

    public long rows() {
        long rows = 0;
        for (Page page : pages()) {
            rows += page.getPositionCount();
        }
        return rows;
    }

    public boolean isTerminal() {
        return metadata.status() != StreamMetadata.Status.RUNNING;
    }

    @Override
    public void close() {
        for (Page page : takePages()) {
            page.releaseBlocks();
        }
    }
}
