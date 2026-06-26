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
 * Data-node-side batch operator for remote fetch over bidirectional exchange.
 * <p>
 * It consumes one page of handles that has already been partitioned to a single retained target session and
 * executes one local fetch call for that full handle batch. This operator does not perform transport fanout.
 */
final class RemoteFetchDataNodeBatchOperator implements Operator {
    private final RemoteFetcher batchFetcher;
    private final Deque<Page> outputQueue = new ArrayDeque<>();
    private boolean finished;
    private Exception failure;

    RemoteFetchDataNodeBatchOperator(RemoteFetcher batchFetcher) {
        this.batchFetcher = batchFetcher;
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
            validateSingleTargetSession(handles);
            // This executes on the target data node against retained local shard contexts;
            // it does not fan out additional transport requests per handle from here.
            List<Page> fetched = batchFetcher.fetch(handles);
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

    private static void validateSingleTargetSession(List<RemoteFetchHandle> handles) {
        if (handles.size() < 2) {
            return;
        }
        RemoteFetchHandle first = handles.getFirst();
        for (int i = 1; i < handles.size(); i++) {
            RemoteFetchHandle current = handles.get(i);
            if (first.nodeId().equals(current.nodeId()) == false
                || first.retainedSessionId().equals(current.retainedSessionId()) == false) {
                throw new IllegalStateException(
                    "remote fetch batch must contain handles from a single target session but saw ["
                        + first.nodeId()
                        + "/"
                        + first.retainedSessionId()
                        + "] and ["
                        + current.nodeId()
                        + "/"
                        + current.retainedSessionId()
                        + "]"
                );
            }
        }
    }

    @FunctionalInterface
    interface RemoteFetcher {
        List<Page> fetch(List<RemoteFetchHandle> handles) throws Exception;
    }
}
