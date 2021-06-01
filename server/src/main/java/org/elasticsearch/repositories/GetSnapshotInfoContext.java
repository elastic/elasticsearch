/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * Describes the context of fetching {@link SnapshotInfo} via {@link Repository#getSnapshotInfo(GetSnapshotInfoContext)}.
 */
public final class GetSnapshotInfoContext implements ActionListener<SnapshotInfo> {

    /**
     * Snapshot ids to fetch info for
     */
    private final List<SnapshotId> snapshotIds;

    /**
     * Stop fetching additional {@link SnapshotInfo} if an exception is encountered.
     */
    private final boolean failFast;

    private volatile boolean failed = false;

    private final BooleanSupplier isCancelled;

    private final ActionListener<Void> doneListener;

    private final BiConsumer<GetSnapshotInfoContext, SnapshotInfo> onSnapshotInfo;

    // TODO: enhance org.elasticsearch.common.util.concurrent.CountDown to allow for an atomic check and try-countdown and use it to
    //       simplify the logic here
    private final AtomicInteger counter;

    private final AtomicReference<Exception> exception = new AtomicReference<>();

    public GetSnapshotInfoContext(Collection<SnapshotId> snapshotIds,
                                  boolean failFast,
                                  BooleanSupplier isCancelled,
                                  BiConsumer<GetSnapshotInfoContext, SnapshotInfo> onSnapshotInfo,
                                  ActionListener<Void> doneListener) {
        this.snapshotIds = List.copyOf(snapshotIds);
        this.counter = new AtomicInteger(snapshotIds.size());
        this.failFast = failFast;
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
        return failFast;
    }

    /**
     * @return true if fetching {@link SnapshotInfo} has been cancelled
     */
    public boolean isCancelled() {
        return isCancelled.getAsBoolean();
    }

    /**
     * @return true if fetching {@link SnapshotInfo} has been stopped
     */
    public boolean stopped() {
        return failed;
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
        if (counter.decrementAndGet() == 0) {
            try {
                doneListener.onResponse(null);
            } catch (Exception e) {
                assert false : e;
                doneListener.onFailure(e);
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (failFast) {
            failed = true;
            if (counter.getAndSet(0) > 0) {
                failDoneListener(e);
            }
        } else {
            final Exception failure = exception.updateAndGet(ex -> {
                if (ex == null) {
                    return e;
                }
                ex.addSuppressed(e);
                return ex;
            });
            if (counter.decrementAndGet() == 0) {
                failDoneListener(failure);
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
