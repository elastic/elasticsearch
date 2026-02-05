/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

public final class ExchangeResponse extends TransportResponse implements Releasable {
    private static final TransportVersion ESQL_STREAMING_LOOKUP_JOIN = TransportVersion.fromName("esql_streaming_lookup_join");
    private final RefCounted counted = AbstractRefCounted.of(this::closeInternal);
    private final Page page;
    private final boolean finished;
    private boolean pageTaken;
    private final BlockFactory blockFactory;
    private long reservedBytes = 0;

    public ExchangeResponse(BlockFactory blockFactory, Page page, boolean finished) {
        this.blockFactory = blockFactory;
        this.page = page;
        this.finished = finished;
    }

    public ExchangeResponse(BlockStreamInput in) throws IOException {
        this.blockFactory = in.blockFactory();
        this.page = readPage(in);
        this.finished = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (page != null) {
            long bytes = page.ramBytesUsedByBlocks();
            blockFactory.breaker().addEstimateBytesAndMaybeBreak(bytes, "serialize exchange response");
            reservedBytes += bytes;
        }
        writePage(out, page);
        out.writeBoolean(finished);
    }

    /**
     * Take the ownership of the page responded by {@link RemoteSink}. This can be null and out of order.
     */
    @Nullable
    public Page takePage() {
        if (pageTaken) {
            assert false : "Page was taken already";
            throw new IllegalStateException("Page was taken already");
        }
        pageTaken = true;
        return page;
    }

    public long ramBytesUsedByPage() {
        if (page != null) {
            return page.ramBytesUsedByBlocks();
        } else {
            return 0;
        }
    }

    /**
     * Returns true if the {@link RemoteSink} is already completed. In this case, the {@link ExchangeSourceHandler}
     * can stop polling pages and finish itself.
     */
    public boolean finished() {
        return finished;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExchangeResponse response = (ExchangeResponse) o;
        return finished == response.finished && Objects.equals(page, response.page);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page, finished);
    }

    @Override
    public void incRef() {
        counted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return counted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return counted.decRef();
    }

    @Override
    public boolean hasReferences() {
        return counted.hasReferences();
    }

    @Override
    public void close() {
        counted.decRef();
    }

    private void closeInternal() {
        blockFactory.breaker().addWithoutBreaking(-reservedBytes);
        if (pageTaken == false && page != null) {
            page.releaseBlocks();
        }
    }

    /**
     * Reads a page from the stream, handling BatchPage deserialization based on transport version.
     */
    private static Page readPage(BlockStreamInput in) throws IOException {
        boolean hasPage = in.readBoolean();
        if (hasPage == false) {
            return null;
        }
        boolean isBatchPage = false;
        if (in.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
            isBatchPage = in.readBoolean();
        }
        return isBatchPage ? new BatchPage(in) : new Page(in);
    }

    /**
     * Writes a page to the stream, handling BatchPage serialization based on transport version.
     * @param out the output stream
     * @param page the page to write, or null if no page
     */
    private static void writePage(StreamOutput out, Page page) throws IOException {
        boolean hasPage = page != null;
        out.writeBoolean(hasPage);
        if (hasPage == false) {
            return;
        }
        boolean isBatchPage = page instanceof BatchPage;
        if (out.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
            out.writeBoolean(isBatchPage);
        } else {
            // For older versions, BatchPage is not supported
            if (isBatchPage) {
                throw new IllegalStateException("BatchPage serialization is not supported on remote node");
            }
        }
        // this will correctly Page.writeTo() or BatchPage.writeTo(), depending on the type of page
        page.writeTo(out);
    }
}
