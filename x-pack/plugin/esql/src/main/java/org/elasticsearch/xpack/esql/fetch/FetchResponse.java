/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The fetched columns of the documents a {@link FetchRequest} named, as pages whose rows follow the request: the shards in
 * request order, and the documents of each shard in the order the request listed them. A request that fails for one of
 * its shards fails as a whole, like a fetch request of {@code _search}, so a response always has a row for every document.
 * <p>
 * The channel that sends it charges the serialized bytes to the breaker of the sending node until they are written. The
 * node that reads it gets the pages through {@link #takePages()}, and the response releases the pages nobody took.
 */
public final class FetchResponse extends TransportResponse implements Releasable {
    private final RefCounted refs = AbstractRefCounted.of(this::closeInternal);
    private final List<Page> pages;
    private final DriverCompletionInfo completionInfo;
    private final long tookNanos;
    private final long setupNanos;
    private boolean pagesTaken;

    /**
     * @param completionInfo the counters of the fetch driver, and its profile when the query is profiled
     * @param tookNanos      from receiving the request to building the response
     * @param setupNanos     the part of {@code tookNanos} spent before the driver started
     */
    public FetchResponse(List<Page> pages, DriverCompletionInfo completionInfo, long tookNanos, long setupNanos) {
        this.pages = List.copyOf(pages);
        this.completionInfo = completionInfo;
        this.tookNanos = tookNanos;
        this.setupNanos = setupNanos;
    }

    /**
     * @param in reads the pages with the block factory of the receiving node, which accounts them on its breaker
     */
    public FetchResponse(BlockStreamInput in, ThreadContext threadContext) throws IOException {
        int pageCount = in.readVInt();
        List<Page> read = new ArrayList<>();
        boolean success = false;
        try {
            for (int p = 0; p < pageCount; p++) {
                read.add(new Page(in));
            }
            this.completionInfo = DriverCompletionInfo.readFrom(in, threadContext);
            this.tookNanos = in.readVLong();
            this.setupNanos = in.readVLong();
            success = true;
        } finally {
            if (success == false) {
                // the pages read so far are on the breaker already
                Releasables.closeExpectNoException(read);
            }
        }
        this.pages = Collections.unmodifiableList(read);
    }

    /**
     * Writes the pages. The channel that sends the response charges the bytes written, until they are sent.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(pages);
        completionInfo.writeTo(out);
        out.writeVLong(tookNanos);
        out.writeVLong(setupNanos);
    }

    /**
     * Hands the pages to the caller, which releases them. Only once.
     */
    public List<Page> takePages() {
        if (pagesTaken) {
            assert false : "the pages of a fetch response were taken already";
            throw new IllegalStateException("the pages of a fetch response were taken already");
        }
        pagesTaken = true;
        return pages;
    }

    public DriverCompletionInfo completionInfo() {
        return completionInfo;
    }

    public long tookNanos() {
        return tookNanos;
    }

    public long setupNanos() {
        return setupNanos;
    }

    /**
     * The rows the pages hold, one for each document of the request.
     */
    public int rows() {
        int rows = 0;
        for (Page page : pages) {
            rows += page.getPositionCount();
        }
        return rows;
    }

    @Override
    public void incRef() {
        refs.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refs.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return refs.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refs.hasReferences();
    }

    @Override
    public void close() {
        decRef();
    }

    private void closeInternal() {
        if (pagesTaken == false) {
            Releasables.closeExpectNoException(pages);
        }
    }
}
