/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * The fetched columns of the documents a {@link FetchRequest} named, as pages whose rows follow the request: the shards in
 * request order, and the documents of each shard in the order the request listed them. A shard that failed adds no rows.
 * <p>
 * The node that sends it accounts the pages on its breaker while it serializes them. The node that reads it gets the
 * pages through {@link #takePages()}, and the response releases the pages nobody took.
 */
public final class FetchResponse extends TransportResponse implements Releasable {
    /**
     * @param rows    the rows of the shard in the pages, every document the request asked the shard for, or 0 when it
     *                failed
     * @param failure why the shard returned no rows, or {@code null}
     */
    public record ShardResult(ShardId shardId, int rows, @Nullable Exception failure) implements Writeable {
        static ShardResult succeeded(ShardId shardId, int rows) {
            return new ShardResult(shardId, rows, null);
        }

        static ShardResult failed(ShardId shardId, Exception failure) {
            return new ShardResult(shardId, 0, failure);
        }

        static ShardResult readFrom(StreamInput in) throws IOException {
            return new ShardResult(new ShardId(in), in.readVInt(), in.readException());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeVInt(rows);
            out.writeException(failure);
        }
    }

    private final RefCounted refs = AbstractRefCounted.of(this::closeInternal);
    private final BlockFactory blockFactory;
    private final List<ShardResult> shardResults;
    private final List<Page> pages;
    private final DriverCompletionInfo completionInfo;
    private final long tookNanos;
    private final long setupNanos;
    private long reservedBytes;
    private boolean pagesTaken;

    /**
     * @param blockFactory   accounts the pages while the response is serialized
     * @param completionInfo the counters of the fetch drivers, and their profiles when the query is profiled
     * @param tookNanos      from receiving the request to building the response
     * @param setupNanos     the part of {@code tookNanos} spent before the drivers started
     */
    public FetchResponse(
        BlockFactory blockFactory,
        List<ShardResult> shardResults,
        List<Page> pages,
        DriverCompletionInfo completionInfo,
        long tookNanos,
        long setupNanos
    ) {
        this.blockFactory = blockFactory;
        this.shardResults = List.copyOf(shardResults);
        this.pages = List.copyOf(pages);
        this.completionInfo = completionInfo;
        this.tookNanos = tookNanos;
        this.setupNanos = setupNanos;
    }

    /**
     * @param in reads the pages with the block factory of the receiving node, which accounts them on its breaker
     */
    public FetchResponse(BlockStreamInput in, ThreadContext threadContext) throws IOException {
        this.blockFactory = in.blockFactory();
        this.shardResults = in.readCollectionAsImmutableList(ShardResult::readFrom);
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
                releasePages(read);
            }
        }
        this.pages = Collections.unmodifiableList(read);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        long bytes = 0;
        for (Page page : pages) {
            bytes += page.ramBytesUsedByBlocks();
        }
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(bytes, "serialize esql fetch response");
        reservedBytes += bytes;
        out.writeCollection(shardResults);
        out.writeCollection(pages);
        completionInfo.writeTo(out);
        out.writeVLong(tookNanos);
        out.writeVLong(setupNanos);
    }

    public List<ShardResult> shardResults() {
        return shardResults;
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
     * The rows the pages hold, the sum of the rows of every shard.
     */
    public int rows() {
        int rows = 0;
        for (ShardResult result : shardResults) {
            rows += result.rows();
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
        blockFactory.breaker().addWithoutBreaking(-reservedBytes);
        if (pagesTaken == false) {
            releasePages(pages);
        }
    }

    static void releasePages(List<Page> pages) {
        Iterator<Releasable> releasables = Iterators.map(pages.iterator(), page -> page::releaseBlocks);
        Releasables.closeExpectNoException(Releasables.wrap(releasables));
    }
}
