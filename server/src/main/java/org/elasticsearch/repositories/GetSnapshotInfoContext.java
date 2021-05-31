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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

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

    private final Consumer<SnapshotInfo> onSnapshotInfo;

    // TODO: enhance org.elasticsearch.common.util.concurrent.CountDown to allow for an atomic check and try-countdown and use it to
    //       simplify the logic here
    private final AtomicInteger counter;

    private final AtomicReference<Exception> exception = new AtomicReference<>();

    public GetSnapshotInfoContext(Collection<SnapshotId> snapshotIds,
                                  boolean failFast,
                                  BooleanSupplier isCancelled,
                                  Consumer<SnapshotInfo> onSnapshotInfo,
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

    public boolean failFast() {
        return failFast;
    }

    public boolean isCancelled() {
        return isCancelled.getAsBoolean();
    }

    public boolean isDone() {
        return failed;
    }

    @Override
    public void onResponse(SnapshotInfo snapshotInfo) {
        onSnapshotInfo.accept(snapshotInfo);
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
                try {
                    doneListener.onFailure(e);
                } catch (Exception ex) {
                    assert false : ex;
                    throw ex;
                }
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
                try {
                    doneListener.onFailure(failure);
                } catch (Exception ex) {
                    assert false : ex;
                    throw ex;
                }
            }
        }
    }
}
