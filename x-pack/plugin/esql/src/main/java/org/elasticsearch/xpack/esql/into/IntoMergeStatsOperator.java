/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Operator for the {@link org.elasticsearch.xpack.esql.plan.physical.IntoExec.Mode#MERGE_STATS} phase of an {@code INTO} command.
 * Receives one {@code [total, created]} row from each upstream driver and sums
 * them into a single final row. Emits {@code [0, 0]} when no input pages arrive.
 * Where more features are added to {@code INTO} in the future, this operator will be extended to merge additional statistics.
 * <p>
 * It runs once per query on the coordinator, after every writer has reported, which makes it the one place that can
 * refresh the destination exactly once.
 */
public final class IntoMergeStatsOperator implements Operator {
    private static final Logger logger = LogManager.getLogger(IntoMergeStatsOperator.class);

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
            return new Factory(
                refresh ? new Refresher(client, client.threadPool().getThreadContext().newRestorableContext(false), destination) : null
            );
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

    /**
     * Sends the single post-write refresh of the destination. The destination is refreshed by the name the query gave, so for a
     * data stream or an alias that is every backing or aliased index, not only the one that was written to.
     */
    public record Refresher(Client client, Supplier<ThreadContext.StoredContext> caller, String destination) {
        void refresh(ActionListener<Void> listener) {
            send(new RefreshRequest(destination), ActionListener.notifyOnce(listener), true);
        }

        private void send(RefreshRequest request, ActionListener<Void> listener, boolean retryIfAMemberIsClosed) {
            try (ThreadContext.StoredContext ignored = caller.get()) {
                client.execute(RefreshAction.INSTANCE, request, ActionListener.wrap(response -> {
                    if (response.getFailedShards() > 0) {
                        listener.onFailure(firstShardFailure(response));
                    } else if (response.getTotalShards() > 0 && response.getSuccessfulShards() == 0) {
                        // A primary that is not available is counted in the total, but as neither a success nor a failure
                        listener.onFailure(
                            new ElasticsearchStatusException("no shard of the destination was refreshed", RestStatus.SERVICE_UNAVAILABLE)
                        );
                    } else {
                        listener.onResponse(null);
                    }
                }, e -> {
                    // Only a failure of the action itself gets here, such as resolving the targets. A shard that fails to refresh is
                    // in the response above, and a closed one is not retried: the write index may have closed after the targets were
                    // resolved, and skipping it would report success without having refreshed the rows that were written.
                    if (retryIfAMemberIsClosed && ExceptionsHelper.unwrapCause(e) instanceof IndexClosedException) {
                        // An alias can have closed members that the write never touched, which the strict options reject. Only now
                        // are unavailable targets skipped, since the caller was already authorized for the destination by the first
                        // attempt. Skipping them up front would also make security drop a destination that the caller may not
                        // refresh, and the refresh would then succeed having refreshed nothing.
                        RefreshRequest skippingClosedMembers = new RefreshRequest(destination);
                        skippingClosedMembers.indicesOptions(
                            IndicesOptions.builder(skippingClosedMembers.indicesOptions())
                                .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                                .build()
                        );
                        send(skippingClosedMembers, listener, false);
                    } else {
                        listener.onFailure(e);
                    }
                }));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private static Exception firstShardFailure(BroadcastResponse response) {
        List<DefaultShardOperationFailedException> failures = response.getShardFailures();
        if (failures.isEmpty()) {
            return new ElasticsearchException("[{}] shards failed to refresh", response.getFailedShards());
        }
        Throwable cause = failures.getFirst().getCause();
        return cause instanceof Exception e ? e : new ElasticsearchException(failures.getFirst().reason(), cause);
    }

    private final DriverContext driverContext;
    private final BlockFactory blockFactory;
    @Nullable
    private final Refresher refresher;

    private long totalSum;
    private long createdSum;
    private boolean finished;
    private boolean outputEmitted;
    // Set once a refresh is sent; done when it settles. A failure is a warning of the driver context.
    @Nullable
    private SubscribableListener<Void> refreshing;

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
        // TODO: this only counts creates. Once INTO can replace documents, refresh when any document was written (created + updated),
        // or a run that only replaces would skip the refresh.
        if (refresher != null && createdSum > 0) {
            startRefresh(refresher);
        }
    }

    private void startRefresh(Refresher refresher) {
        refreshing = new SubscribableListener<>();
        driverContext.addAsyncAction();
        // Complete the async action before waking the driver, so a woken driver sees no pending actions
        refresher.refresh(ActionListener.runAfter(ActionListener.wrap(ignored -> {}, this::recordRefreshFailure), () -> {
            driverContext.removeAsyncAction();
            refreshing.onResponse(null);
        }));
    }

    /**
     * A refresh that fails is a warning, not a failure of the query: the rows are stored whatever the refresh does, and a failed query
     * invites a retry, which stores them again when their ids are generated.
     */
    private void recordRefreshFailure(Exception cause) {
        // Broadcast replication wraps the failure of a shard in an exception that has no message of its own, and a failure from another
        // node is wrapped again with the details of that node, which are internal. The warning says what actually went wrong, and the
        // cluster admin gets the wrappers, with the whole failure, from the log.
        Throwable underlying = ExceptionsHelper.unwrapCause(cause);
        String hint = ExceptionsHelper.status(underlying) == RestStatus.FORBIDDEN
            ? " (refreshing requires the [maintenance] privilege on the destination; use \"refresh\": false to skip it)"
            : "";
        driverContext.addWarning(
            Strings.format(
                "INTO [%s] wrote [%d] documents successfully, only the refresh failed, so they may not be searchable yet: %s%s",
                refresher.destination(),
                createdSum,
                underlying.getMessage(),
                hint
            )
        );
        logger.warn(
            () -> Strings.format(
                "INTO [%s] wrote [%d] documents but the refresh failed: %s",
                refresher.destination(),
                createdSum,
                wrappers(cause)
            ),
            cause
        );
    }

    /** The failure and what it wraps, outermost first, which is where the node that refused is named. */
    private static String wrappers(Throwable cause) {
        StringBuilder chain = new StringBuilder();
        // A cause chain can be cyclic, and this runs in the failure callback, so it must end
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = cause; t != null && seen.add(t); t = t.getCause()) {
            chain.append(chain.isEmpty() ? "" : " <- ").append(t);
        }
        return chain.toString();
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
        outputEmitted = true;
        return new IntoStats(totalSum, createdSum).toPage(blockFactory);
    }

    @Override
    public void close() {}
}
