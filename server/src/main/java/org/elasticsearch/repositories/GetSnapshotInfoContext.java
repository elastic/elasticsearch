/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * Describes the context of fetching {@link SnapshotInfo} via {@link Repository#getSnapshotInfo(GetSnapshotInfoContext)}.
 */
public final class GetSnapshotInfoContext implements ActionListener<SnapshotInfo> {

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
     * {@link #onSnapshotInfo} will be made. Only resolves exceptionally if {@link #abortOnFailure} is true in case one or more
     * {@link SnapshotInfo} failed to be fetched.
     */
    private final ActionListener<Void> doneListener;

    /**
     * {@link BiConsumer} invoked for each {@link SnapshotInfo} that is fetched with this instance and the {@code SnapshotInfo} as
     * arguments.
     */
    private final BiConsumer<GetSnapshotInfoContext, SnapshotInfo> onSnapshotInfo;

    private final CountDown counter;

    public GetSnapshotInfoContext(
        Collection<SnapshotId> snapshotIds,
        boolean abortOnFailure,
        BooleanSupplier isCancelled,
        BiConsumer<GetSnapshotInfoContext, SnapshotInfo> onSnapshotInfo,
        ActionListener<Void> doneListener
    ) {
        assert snapshotIds.isEmpty() == false : "no snapshot ids to fetch given";
        this.snapshotIds = List.copyOf(snapshotIds);
        this.counter = new CountDown(snapshotIds.size());
        this.abortOnFailure = abortOnFailure;
        this.isCancelled = isCancelled;
        this.onSnapshotInfo = onSnapshotInfo;
        this.doneListener = doneListener;
    }

    public List<SnapshotId> snapshotIds() {
        return snapshotIds;
    }

    /**
     * @return true if fetching {@link SnapshotInfo} should be stopped after encountering any exception
     */
    public boolean failFast() {
        return abortOnFailure;
    }

    /**
     * @return true if fetching {@link SnapshotInfo} has been cancelled
     */
    public boolean isCancelled() {
        return isCancelled.getAsBoolean();
    }

    /**
     * @return true if fetching {@link SnapshotInfo} is either complete or should be stopped because of an error
     */
    public boolean done() {
        return counter.isCountedDown();
    }

    @Override
    public void onResponse(SnapshotInfo snapshotInfo) {
        try {
            onSnapshotInfo.accept(this, snapshotInfo);
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
        if (abortOnFailure) {
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
