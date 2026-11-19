/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;

import java.util.function.Supplier;

/**
 * Operator for the {@link org.elasticsearch.xpack.esql.plan.physical.IntoExec.Mode#MERGE_STATS} phase of an {@code INTO} command.
 * Receives one {@code [total, created]} row from each upstream driver and sums
 * them into a single final row. Emits {@code [0, 0]} when no input pages arrive.
 * Where more features are added to {@code INTO} in the future, this operator will be extended to merge additional statistics.
 * <p>
 * It runs once per query on the coordinator, after every writer has reported, which makes it the one place that can
 * refresh the destination exactly once. With {@link IntoOptions#refresh()} it holds the stats row back until that
 * refresh completes, so the written documents are searchable when the query returns.
 */
public final class IntoMergeStatsOperator implements Operator {
    /**
     * @param refresher refreshes the destination when the merged stats show writes, or {@code null} when no refresh is wanted
     */
    public record Factory(@Nullable Refresher refresher) implements OperatorFactory {
        /** Captures the caller's thread context now, so the refresh runs with the querying user's permissions. */
        public static Factory create(Client client, String destination, IntoOptions options, IntoPageWriter.Factory.Indexing indexing) {
            boolean refresh = options.refresh() && switch (indexing) {
                case INDEX -> true;
                case DISCARD -> false;
            };
            if (refresh == false) {
                return new Factory(null);
            }
            return new Factory(new Refresher(client, client.threadPool().getThreadContext().newRestorableContext(false), destination));
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new IntoMergeStatsOperator(driverContext, refresher);
        }

        @Override
        public String describe() {
            return "IntoMergeStatsOperator";
        }
    }

    /** Sends the single post-write refresh of the destination. */
    public record Refresher(Client client, Supplier<ThreadContext.StoredContext> caller, String destination) {
        void refresh(ActionListener<Void> listener) {
            try (ThreadContext.StoredContext ignored = caller.get()) {
                client.execute(RefreshAction.INSTANCE, new RefreshRequest(destination), listener.map(response -> null));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private final DriverContext driverContext;
    private final BlockFactory blockFactory;
    @Nullable
    private final Refresher refresher;

    private long totalSum;
    private long createdSum;
    private boolean finished;
    private boolean outputEmitted;
    // Set once a refresh is sent; done when it settles. Its failure is kept in refreshFailure.
    @Nullable
    private SubscribableListener<Void> refreshing;
    @Nullable
    private volatile ElasticsearchStatusException refreshFailure;

    private IntoMergeStatsOperator(DriverContext driverContext, @Nullable Refresher refresher) {
        this.driverContext = driverContext;
        this.blockFactory = driverContext.blockFactory();
        this.refresher = refresher;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            LongBlock totalBlock = page.getBlock(0);
            LongBlock createdBlock = page.getBlock(1);
            for (int i = 0; i < page.getPositionCount(); i++) {
                totalSum += totalBlock.getLong(totalBlock.getFirstValueIndex(i));
                createdSum += createdBlock.getLong(createdBlock.getFirstValueIndex(i));
            }
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        if (refresher != null && createdSum > 0) {
            startRefresh(refresher);
        }
    }

    private void startRefresh(Refresher refresher) {
        SubscribableListener<Void> done = new SubscribableListener<>();
        refreshing = done;
        driverContext.addAsyncAction();
        // Complete the async action before waking the driver, so a woken driver sees no pending actions
        refresher.refresh(ActionListener.runAfter(ActionListener.wrap(ignored -> {}, this::recordRefreshFailure), () -> {
            driverContext.removeAsyncAction();
            done.onResponse(null);
        }));
    }

    private void recordRefreshFailure(Exception cause) {
        refreshFailure = new ElasticsearchStatusException(
            "INTO [{}] refresh failed: {}",
            ExceptionsHelper.status(cause),
            cause,
            refresher.destination(),
            cause.getMessage()
        );
    }

    private boolean refreshPending() {
        return refreshing != null && refreshing.isDone() == false;
    }

    @Override
    public IsBlockedResult isBlocked() {
        return refreshPending() ? new IsBlockedResult(refreshing, "INTO refresh " + refresher.destination()) : NOT_BLOCKED;
    }

    @Override
    public boolean isFinished() {
        return outputEmitted;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return finished && outputEmitted == false && refreshPending() == false;
    }

    @Override
    public Page getOutput() {
        if (finished == false || refreshPending()) {
            return null;
        }
        ElasticsearchStatusException failure = refreshFailure;
        if (failure != null) {
            throw failure;
        }
        outputEmitted = true;
        return new IntoStats(totalSum, createdSum).toPage(blockFactory);
    }

    @Override
    public void close() {}
}
