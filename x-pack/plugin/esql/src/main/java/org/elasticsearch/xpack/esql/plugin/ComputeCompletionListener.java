/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.DriverCompletionInfo;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Listener for the completion of a compute, which is also told what the drivers did when the compute fails.
 * <p>
 * A plain {@code ActionListener<DriverCompletionInfo>} only learns about the work of a compute when it succeeds, so the CPU
 * and bytes read of a query that fails or is cancelled halfway are lost. {@code ComputeService#runCompute} calls
 * {@link #onFailure(Exception, DriverCompletionInfo)} when the listener it is given is one of these. Any other listener
 * keeps getting a plain failure, and so does one that is wrapped, for example by {@code map}, {@code delegateFailure} or
 * {@code notifyOnce}: to learn the usage, a listener that wraps must itself be a {@code ComputeCompletionListener}.
 */
interface ComputeCompletionListener extends ActionListener<DriverCompletionInfo> {

    /**
     * The compute failed. {@code usage} is what its drivers did before they stopped.
     */
    void onFailure(Exception e, DriverCompletionInfo usage);

    @Override
    default void onFailure(Exception e) {
        onFailure(e, DriverCompletionInfo.EMPTY);
    }

    /**
     * Same as {@link ActionListener#notifyOnce}, and still a {@code ComputeCompletionListener}: only the first completion, with or
     * without usage, reaches {@code delegate}.
     */
    static ComputeCompletionListener notifyOnce(ComputeCompletionListener delegate) {
        AtomicBoolean completed = new AtomicBoolean();
        return new ComputeCompletionListener() {
            @Override
            public void onResponse(DriverCompletionInfo info) {
                if (completed.compareAndSet(false, true)) {
                    delegate.onResponse(info);
                }
            }

            @Override
            public void onFailure(Exception e, DriverCompletionInfo usage) {
                if (completed.compareAndSet(false, true)) {
                    delegate.onFailure(e, usage);
                }
            }
        };
    }
}
