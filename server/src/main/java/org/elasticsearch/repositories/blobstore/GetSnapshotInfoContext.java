/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * A context through which a consumer can act on one or more {@link SnapshotInfo} via {@link Repository#getSnapshotInfo}.
 */
final class GetSnapshotInfoContext implements ActionListener<SnapshotInfo> {

    private static final Logger logger = LogManager.getLogger(GetSnapshotInfoContext.class);

    /**
     * Snapshot ids to fetch info for.
     */
    private final List<SnapshotId> snapshotIds;

    /**
     * Stop fetching additional {@link SnapshotInfo} if an exception is encountered.
     */
    private final boolean abortOnFailure;

    /**
     * If this supplier returns true, indicates that the task that initiated this context has been cancelled and that not further fetching
     * of {@link SnapshotInfo} should be started.
     */
    private final BooleanSupplier isCancelled;

    /**
     * Listener resolved when fetching {@link SnapshotInfo} has completed. If resolved successfully, no more calls to
     * {@link #consumer} will be made. Only resolves exceptionally if {@link #abortOnFailure} is true in case one or more
     * {@link SnapshotInfo} failed to be fetched.
     * This listener is always invoked on the {@link ThreadPool.Names#SNAPSHOT_META} pool.
     */
    private final ActionListener<Void> doneListener;

    /**
     * {@link BiConsumer} invoked for each {@link SnapshotInfo} that is fetched with this instance and the {@code SnapshotInfo} as
     * arguments. This consumer is always invoked on the {@link ThreadPool.Names#SNAPSHOT_META} pool.
     */
    private final BiConsumer<GetSnapshotInfoContext, SnapshotInfo> consumer;

    private final CountDown counter;

    GetSnapshotInfoContext(
        Collection<SnapshotId> snapshotIds,
        boolean abortOnFailure,
        BooleanSupplier isCancelled,
        BiConsumer<GetSnapshotInfoContext, SnapshotInfo> consumer,
        ActionListener<Void> listener
    ) {
        if (snapshotIds.isEmpty()) {
            throw new IllegalArgumentException("no snapshot ids to fetch given");
        }
        this.snapshotIds = List.copyOf(snapshotIds);
        this.counter = new CountDown(snapshotIds.size());
        this.abortOnFailure = abortOnFailure;
        this.isCancelled = isCancelled;
        this.consumer = consumer;
        this.doneListener = listener;
    }

    List<SnapshotId> snapshotIds() {
        return snapshotIds;
    }

    /**
     * @return true if fetching {@link SnapshotInfo} should be stopped after encountering any exception
     */
    boolean abortOnFailure() {
        return abortOnFailure;
    }

    /**
     * @return true if fetching {@link SnapshotInfo} has been cancelled
     */
    boolean isCancelled() {
        return isCancelled.getAsBoolean();
    }

    /**
     * @return true if fetching {@link SnapshotInfo} is either complete or should be stopped because of an error
     */
    boolean done() {
        return counter.isCountedDown();
    }

    @Override
    public void onResponse(SnapshotInfo snapshotInfo) {
        assert Repository.assertSnapshotMetaThread();
        try {
            consumer.accept(this, snapshotInfo);
        } catch (Exception e) {
            assert false : e;
            onFailure(e);
            return;
        }
        if (counter.countDown()) {
            try {
                doneListener.onResponse(null);
            } catch (Exception e) {
                assert false : e;
                failDoneListener(e);
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        assert Repository.assertSnapshotMetaThread();
        if (abortOnFailure || isCancelled()) {
            if (counter.fastForward()) {
                failDoneListener(e);
            }
        } else {
            logger.warn("failed to fetch snapshot info", e);
            if (counter.countDown()) {
                doneListener.onResponse(null);
            }
        }
    }

    private void failDoneListener(Exception failure) {
        try {
            doneListener.onFailure(failure);
        } catch (Exception ex) {
            assert false : ex;
            throw ex;
        }
    }
}
