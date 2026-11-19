/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.compute.EsqlRefCountingListener;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.function.Consumer;

/**
 * A variant of {@link RefCountingListener} with the following differences:
 * 1. Automatically cancels sub tasks on failure (via runOnTaskFailure)
 * 2. Collects driver profiles from sub tasks.
 * 3. Collects failures and returns the most appropriate exception to the caller.
 * 4. Optionally reports what the sub tasks did when the compute fails, which the failure itself cannot carry.
 */
final class ComputeListener implements Releasable {
    private static final Logger logger = LogManager.getLogger(ComputeListener.class);

    private final DriverCompletionInfo.AtomicAccumulator completionInfoAccumulator = new DriverCompletionInfo.AtomicAccumulator();
    private final EsqlRefCountingListener refs;
    private final Runnable runOnFailure;

    ComputeListener(Runnable runOnFailure, ActionListener<DriverCompletionInfo> delegate) {
        this(runOnFailure, delegate, usage -> {});
    }

    /**
     * Same as {@link #ComputeListener(Runnable, ActionListener)}, and when the compute fails the usage of its sub tasks is also
     * reported to {@code onFailureUsage}, which a failure cannot carry.
     * <p>
     * That usage is what the sub tasks that succeeded did, plus what the ones that failed said they did through
     * {@link ComputeCompletionListener#onFailure(Exception, DriverCompletionInfo)}. A sub task that failed without saying
     * what it did adds nothing. The usage of a nested {@code ComputeListener} that fails is reported to its own consumer only:
     * to its parent it is a sub task that failed without saying what it did.
     *
     * @param onFailureUsage called before the failure is delivered to {@code delegate}
     */
    ComputeListener(Runnable runOnFailure, ActionListener<DriverCompletionInfo> delegate, Consumer<DriverCompletionInfo> onFailureUsage) {
        this.runOnFailure = runOnFailure;
        // listener that executes after all the sub-listeners refs (created via acquireCompute) have completed
        ActionListener<Void> onSuccess = delegate.delegateFailure((l, ignored) -> l.onResponse(completionInfoAccumulator.finish()));
        this.refs = new EsqlRefCountingListener(new ActionListener<Void>() {
            @Override
            public void onResponse(Void ignored) {
                onSuccess.onResponse(ignored);
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    onFailureUsage.accept(completionInfoAccumulator.finish());
                } catch (Exception usageFailure) {
                    // reporting usage is best effort and must never hide the failure of the compute
                    logger.warn("failed to report the usage of a failed compute", usageFailure);
                } finally {
                    delegate.onFailure(e);
                }
            }
        });
    }

    /**
     * Acquires a new listener that doesn't collect result
     */
    ActionListener<Void> acquireAvoid() {
        var listener = ActionListener.assertAtLeastOnce(refs.acquire());
        return listener.delegateResponse((l, e) -> {
            try {
                runOnFailure.run();
            } finally {
                l.onFailure(e);
            }
        });
    }

    /**
     * Acquires a new listener that collects compute result, and the usage of the compute if it fails.
     */
    ComputeCompletionListener acquireCompute() {
        final ActionListener<Void> delegate = acquireAvoid();
        return new ComputeCompletionListener() {
            @Override
            public void onResponse(DriverCompletionInfo info) {
                try {
                    completionInfoAccumulator.accumulate(info);
                    delegate.onResponse(null);
                } catch (Exception e) {
                    delegate.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e, DriverCompletionInfo usage) {
                try {
                    completionInfoAccumulator.accumulate(usage);
                } finally {
                    // the slot is released and the compute fails even if the usage cannot be added up
                    delegate.onFailure(e);
                }
            }
        };
    }

    @Override
    public void close() {
        refs.close();
    }
}
