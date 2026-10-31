/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.IntFunction;

/**
 * The co-located join's streaming lookup. The right index has several shards and each left row has its matches on exactly one
 * of them ({@link PartnerShards}), so this keeps one {@link StreamingLookupFromIndexOperator} per right shard, opened the first
 * time a row needs that shard, and sends each row to the one of its shard. A streaming lookup keeps its lookup session open for
 * the whole query, so a batch pays neither a transport request of its own nor a new shard context and lookup plan, which the
 * per-page lookup of {@link LookupFromIndexOperator} pays for every page.
 * <p>
 * The output order is not the input order: each shard's lookup emits its joined rows as they come back. That is fine for a join.
 */
public final class ColocatedStreamingLookupOperator implements Operator {
    private final PartnerShards partnerShards;
    private final int maxOutstandingBatches;
    private final IntFunction<StreamingLookupFromIndexOperator> lookupForShard;
    private final String index;
    private final List<NamedExpression> loadFields;
    private final boolean innerJoin;
    private final Map<Integer, StreamingLookupFromIndexOperator> lookups = new TreeMap<>();
    private boolean finished = false;
    /**
     * The shard to ask for output first, so that one shard's results don't starve the others.
     */
    private int nextOutput = 0;

    /**
     * @param maxOutstandingBatches the most batches in flight over all shards
     * @param lookupForShard        opens the streaming lookup against one right shard
     */
    public ColocatedStreamingLookupOperator(
        PartnerShards partnerShards,
        int maxOutstandingBatches,
        IntFunction<StreamingLookupFromIndexOperator> lookupForShard,
        String index,
        List<NamedExpression> loadFields,
        boolean innerJoin
    ) {
        this.partnerShards = partnerShards;
        this.maxOutstandingBatches = maxOutstandingBatches;
        this.lookupForShard = lookupForShard;
        this.index = index;
        this.loadFields = loadFields;
        this.innerJoin = innerJoin;
    }

    @Override
    public boolean needsInput() {
        if (finished) {
            return false;
        }
        int outstanding = 0;
        for (StreamingLookupFromIndexOperator lookup : lookups.values()) {
            outstanding += lookup.activeBatchCount();
        }
        return outstanding < maxOutstandingBatches;
    }

    @Override
    public void addInput(Page page) {
        List<PartnerShards.Group> groups;
        try {
            groups = partnerShards.split(page);
        } catch (RuntimeException e) {
            page.releaseBlocks();
            throw e;
        }
        if (groups.isEmpty()) {
            // No row can match: every key is null.
            page.releaseBlocks();
            return;
        }
        if (groups.size() == 1 && groups.get(0).positions() == null) {
            lookup(groups.get(0).shard()).addInput(page);
            return;
        }
        Page[] subPages = new Page[groups.size()];
        try {
            for (int i = 0; i < subPages.length; i++) {
                subPages[i] = page.filter(false, groups.get(i).positions());
            }
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(releaseAll(subPages));
            throw e;
        } finally {
            page.releaseBlocks();
        }
        for (int i = 0; i < subPages.length; i++) {
            Page subPage = subPages[i];
            subPages[i] = null;
            try {
                // takes ownership of the sub-page, even when it fails
                lookup(groups.get(i).shard()).addInput(subPage);
            } catch (RuntimeException e) {
                Releasables.closeExpectNoException(releaseAll(subPages));
                throw e;
            }
        }
    }

    private static Releasable releaseAll(Page[] pages) {
        return () -> {
            for (Page p : pages) {
                if (p != null) {
                    p.releaseBlocks();
                }
            }
        };
    }

    private StreamingLookupFromIndexOperator lookup(int shard) {
        return lookups.computeIfAbsent(shard, lookupForShard::apply);
    }

    @Override
    public Page getOutput() {
        if (lookups.isEmpty()) {
            return null;
        }
        List<StreamingLookupFromIndexOperator> all = new ArrayList<>(lookups.values());
        for (int i = 0; i < all.size(); i++) {
            int idx = (nextOutput + i) % all.size();
            Page output = all.get(idx).getOutput();
            if (output != null) {
                nextOutput = idx + 1;
                return output;
            }
        }
        return null;
    }

    @Override
    public void finish() {
        finished = true;
        for (StreamingLookupFromIndexOperator lookup : lookups.values()) {
            lookup.finish();
        }
    }

    @Override
    public boolean isFinished() {
        if (finished == false) {
            return false;
        }
        for (StreamingLookupFromIndexOperator lookup : lookups.values()) {
            if (lookup.isFinished() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        for (StreamingLookupFromIndexOperator lookup : lookups.values()) {
            if (lookup.canProduceMoreDataWithoutExtraInput()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Blocked while every shard's lookup that still has work is blocked, until the first of them can move on. A lookup
     * without work (no batch in flight and not finishing) does not count: it reports "not blocked" only because it is idle.
     */
    @Override
    public IsBlockedResult isBlocked() {
        List<IsBlockedResult> blocked = new ArrayList<>();
        for (StreamingLookupFromIndexOperator lookup : lookups.values()) {
            boolean busy = lookup.activeBatchCount() > 0 || (finished && lookup.isFinished() == false);
            if (busy == false) {
                continue;
            }
            IsBlockedResult result = lookup.isBlocked();
            if (result.listener().isDone()) {
                return NOT_BLOCKED;
            }
            blocked.add(result);
        }
        if (blocked.isEmpty()) {
            return NOT_BLOCKED;
        }
        if (blocked.size() == 1) {
            return blocked.get(0);
        }
        SubscribableListener<Void> oneOf = new SubscribableListener<>();
        StringBuilder reason = new StringBuilder();
        for (IsBlockedResult r : blocked) {
            r.listener().addListener(oneOf);
            if (reason.isEmpty() == false) {
                reason.append(" OR ");
            }
            reason.append(r.reason());
        }
        return new IsBlockedResult(oneOf, reason.toString());
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lookups.values().toArray(StreamingLookupFromIndexOperator[]::new));
    }

    /**
     * The sum of the per-shard lookups' statuses.
     */
    @Override
    public Status status() {
        long pagesReceived = 0;
        long pagesEmitted = 0;
        long rowsReceived = 0;
        long rowsEmitted = 0;
        long planningNanos = 0;
        long processNanos = 0;
        long bytesRead = 0;
        Map<String, Set<String>> plans = new HashMap<>();
        for (StreamingLookupFromIndexOperator lookup : lookups.values()) {
            var status = (StreamingLookupFromIndexOperator.StreamingLookupStatus) lookup.status();
            pagesReceived += status.pagesReceived();
            pagesEmitted += status.pagesEmitted();
            rowsReceived += status.rowsReceived();
            rowsEmitted += status.rowsEmitted();
            planningNanos += status.planningNanos();
            processNanos += status.processNanos();
            bytesRead += status.bytesRead();
            status.planToWorkers().forEach((plan, workers) -> plans.computeIfAbsent(plan, k -> new HashSet<>()).addAll(workers));
        }
        return new StreamingLookupFromIndexOperator.StreamingLookupStatus(
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            planningNanos,
            processNanos,
            plans,
            bytesRead
        );
    }

    @Override
    public String toString() {
        return "StreamingLookupOperator[index="
            + index
            + " load_fields="
            + loadFields
            + " colocated=true"
            + (innerJoin ? " join=inner" : "")
            + " shards="
            + lookups.keySet()
            + "]";
    }
}
