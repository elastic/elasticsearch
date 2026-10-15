/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Routes each page's rows to hash-partitioned exchange sinks while preserving block ownership.
 */
public final class PartitionedExchangeSink implements ExchangeSink {

    private final List<ExchangeSink> sinks;
    private final HashPartitioner partitioner;
    private final int[] offsets;

    private int[] partitions = new int[0];
    private int[] positions = new int[0];

    public PartitionedExchangeSink(List<ExchangeSink> sinks, HashPartitioner partitioner) {
        this.sinks = List.copyOf(sinks);
        this.partitioner = Objects.requireNonNull(partitioner);
        if (this.sinks.size() != partitioner.partitionCount()) {
            throw new IllegalArgumentException(
                "sink count [" + this.sinks.size() + "] must match partition count [" + partitioner.partitionCount() + "]"
            );
        }
        if (this.sinks.isEmpty()) {
            throw new IllegalArgumentException("at least one exchange sink is required");
        }
        this.offsets = new int[this.sinks.size() + 1];
    }

    @Override
    public void addPage(Page page) {
        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            page.releaseBlocks();
            return;
        }
        ensureCapacity(positionCount);
        boolean ownershipTransferred = false;
        try {
            partitioner.partition(page, partitions);
            int firstPartition = partitions[0];
            boolean allRowsSharePartition = true;
            for (int position = 1; position < positionCount; position++) {
                if (partitions[position] != firstPartition) {
                    allRowsSharePartition = false;
                    break;
                }
            }
            if (allRowsSharePartition) {
                sinks.get(firstPartition).addPage(page);
                ownershipTransferred = true;
                return;
            }

            Arrays.fill(offsets, 0);
            for (int position = 0; position < positionCount; position++) {
                offsets[partitions[position] + 1]++;
            }
            for (int partition = 1; partition < offsets.length; partition++) {
                offsets[partition] += offsets[partition - 1];
            }
            for (int position = positionCount - 1; position >= 0; position--) {
                int partition = partitions[position];
                positions[--offsets[partition + 1]] = position;
            }

            for (int partition = 0; partition < sinks.size(); partition++) {
                int start = offsets[partition + 1];
                int end = partition + 1 == sinks.size() ? positionCount : offsets[partition + 2];
                ExchangeSink sink = sinks.get(partition);
                if (start == end || sink.isFinished()) {
                    continue;
                }
                Page partitionPage = page.filter(false, positions, start, end - start);
                boolean partitionPageTransferred = false;
                try {
                    sink.addPage(partitionPage);
                    partitionPageTransferred = true;
                } finally {
                    if (partitionPageTransferred == false) {
                        partitionPage.releaseBlocks();
                    }
                }
            }
        } finally {
            if (ownershipTransferred == false) {
                page.releaseBlocks();
            }
        }
    }

    private void ensureCapacity(int positionCount) {
        if (partitions.length < positionCount) {
            partitions = new int[positionCount];
            positions = new int[positionCount];
        }
    }

    @Override
    public void finish() {
        sinks.forEach(ExchangeSink::finish);
    }

    @Override
    public boolean isFinished() {
        return sinks.stream().allMatch(ExchangeSink::isFinished);
    }

    @Override
    public void addCompletionListener(ActionListener<Void> listener) {
        try (var listeners = new RefCountingListener(listener)) {
            for (ExchangeSink sink : sinks) {
                sink.addCompletionListener(listeners.acquire());
            }
        }
    }

    @Override
    public IsBlockedResult waitForWriting() {
        for (ExchangeSink sink : sinks) {
            IsBlockedResult blocked = sink.waitForWriting();
            if (blocked.listener().isDone() == false) {
                return blocked;
            }
        }
        return Operator.NOT_BLOCKED;
    }
}
