/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Server-side batch operator for remote fetch over bidirectional exchange.
 * Consumes one handles page per batch and emits one or more fetched pages
 * carrying the same batch ID.
 */
final class RemoteFetchBatchOperator implements Operator {
    private final RemoteFetcher remoteFetcher;
    private final Deque<Page> outputQueue = new ArrayDeque<>();
    private boolean finished;
    private Exception failure;

    RemoteFetchBatchOperator(RemoteFetcher remoteFetcher) {
        this.remoteFetcher = remoteFetcher;
    }

    @Override
    public boolean needsInput() {
        return finished == false && failure == null;
    }

    @Override
    public void addInput(Page page) {
        try {
            if (failure != null) {
                return;
            }
            List<RemoteFetchHandle> handles = decodeHandles(page);
            // This executes on the target data node against retained local shard contexts;
            // it does not fan out additional transport requests per handle from here.
            List<Page> fetched = remoteFetcher.fetch(handles);
            enqueue(fetched);
        } catch (Exception e) {
            failure = e;
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return (finished || failure != null) && outputQueue.isEmpty();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return outputQueue.isEmpty() == false || failure != null;
    }

    @Override
    public Page getOutput() {
        if (failure == null) {
            return outputQueue.pollFirst();
        }
        Exception e = failure;
        failure = null;
        if (e instanceof RuntimeException re) {
            throw re;
        }
        throw new IllegalStateException("remote fetch batch operator failed", e);
    }

    @Override
    public void close() {
        for (Page page : outputQueue) {
            Releasables.closeExpectNoException(page::releaseBlocks);
        }
        outputQueue.clear();
    }

    private static List<RemoteFetchHandle> decodeHandles(Page page) {
        if (page.getBlockCount() != 1) {
            throw new IllegalStateException("expected a single handle block but got [" + page.getBlockCount() + "]");
        }
        BytesRefBlock handlesBlock = page.getBlock(0);
        List<RemoteFetchHandle> handles = new ArrayList<>(page.getPositionCount());
        BytesRef scratch = new BytesRef();
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (handlesBlock.isNull(position)) {
                throw new IllegalStateException("remote fetch handle block cannot contain nulls");
            }
            if (handlesBlock.getValueCount(position) != 1) {
                throw new IllegalStateException("remote fetch handle block must have exactly one value per row");
            }
            handles.add(RemoteFetchHandle.fromBytesRef(handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch)));
        }
        return handles;
    }

    private void enqueue(List<Page> fetchedPages) {
        if (fetchedPages.isEmpty()) {
            return;
        }
        boolean success = false;
        try {
            outputQueue.addAll(fetchedPages);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(fetchedPages));
            }
        }
    }

    @FunctionalInterface
    interface RemoteFetcher {
        List<Page> fetch(List<RemoteFetchHandle> handles) throws Exception;
    }
}
