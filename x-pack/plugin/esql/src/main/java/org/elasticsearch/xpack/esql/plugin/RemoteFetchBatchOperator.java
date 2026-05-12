/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Server-side batch operator for remote fetch over bidirectional exchange.
 * Consumes one handles page per batch and emits one or more fetched pages
 * carrying the same batch id.
 */
final class RemoteFetchBatchOperator implements Operator {
    private final List<RemoteFetchService.FetchField> fields;
    private final IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts;
    private final PlannerSettings plannerSettings;
    private final RemoteFetchService remoteFetchService;
    private final PhysicalPlan pushdownPlan;
    private final Deque<Page> outputQueue = new ArrayDeque<>();
    private boolean finished;
    private Exception failure;

    RemoteFetchBatchOperator(
        List<RemoteFetchService.FetchField> fields,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        PlannerSettings plannerSettings,
        PhysicalPlan pushdownPlan,
        RemoteFetchService remoteFetchService
    ) {
        this.fields = List.copyOf(fields);
        this.shardContexts = shardContexts;
        this.plannerSettings = plannerSettings;
        this.pushdownPlan = pushdownPlan;
        this.remoteFetchService = remoteFetchService;
    }

    @Override
    public boolean needsInput() {
        return finished == false && failure == null;
    }

    @Override
    public void addInput(Page page) {
        if (failure != null) {
            page.releaseBlocks();
            return;
        }
        try {
            BatchMetadata metadata = page.batchMetadata();
            if (metadata == null) {
                throw new IllegalStateException("remote fetch batch page missing metadata");
            }
            List<RemoteFetchHandle> handles = decodeHandles(page);
            List<Page> fetched = remoteFetchService.executeFetchForExchange(handles, fields, shardContexts, plannerSettings);
            fetched = remoteFetchService.executePushdownForExchange(fetched, pushdownPlan, shardContexts);
            enqueueWithBatchMetadata(metadata.batchId(), fetched);
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
        if (failure != null) {
            Exception e = failure;
            failure = null;
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException("remote fetch batch operator failed", e);
        }
        return outputQueue.pollFirst();
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
        BytesRefBlock handlesBlock = (BytesRefBlock) page.getBlock(0);
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

    private void enqueueWithBatchMetadata(long batchId, List<Page> fetchedPages) {
        if (fetchedPages.isEmpty()) {
            outputQueue.add(Page.createBatchMarkerPage(batchId, 0));
            return;
        }
        int pageIndex = 0;
        for (Page fetchedPage : fetchedPages) {
            Block[] blocks = new Block[fetchedPage.getBlockCount()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = fetchedPage.getBlock(i);
                blocks[i].incRef();
            }
            boolean isLast = pageIndex == fetchedPages.size() - 1;
            outputQueue.add(new Page(new BatchMetadata(batchId, pageIndex, isLast), blocks));
            pageIndex++;
            fetchedPage.releaseBlocks();
        }
    }
}
