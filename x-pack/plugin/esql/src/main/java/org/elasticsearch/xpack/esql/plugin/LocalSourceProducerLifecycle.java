/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.RemoteSink;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator-local source-producer completion. Compute and the coordinator fetch of that
 * producer's sink are independent: {@code ExchangeService.finishSinkHandler} can finish the
 * buffer and deliver a normal terminal fetch, so a drained sink is not proof of successful
 * compute, and successful compute is not proof that its rows were fetched.
 * <p>
 * Successful publication waits for both sides. A fatal failure or cancellation completes the
 * listener immediately and wins the exactly-once terminal guard, so the counterpart callback
 * is not required.
 */
final class LocalSourceProducerLifecycle {
    private final boolean failFast;
    private final SourceOutcomeAccumulator sourceOutcomes;
    private final ActionListener<DriverCompletionInfo> listener;
    private final AtomicReference<DriverCompletionInfo> computeInfo = new AtomicReference<>();
    private final AtomicBoolean sinkFinished = new AtomicBoolean();
    private final AtomicBoolean published = new AtomicBoolean();
    private final AtomicBoolean externalPartial = new AtomicBoolean();
    private final List<String> toleratedFailures = Collections.synchronizedList(new ArrayList<>());
    private final ActionListener<DriverCompletionInfo> computeListener;

    private LocalSourceProducerLifecycle(
        boolean failFast,
        SourceOutcomeAccumulator sourceOutcomes,
        ActionListener<DriverCompletionInfo> listener
    ) {
        this.failFast = failFast;
        this.sourceOutcomes = sourceOutcomes;
        this.listener = listener;
        this.computeListener = new ActionListener<>() {
            @Override
            public void onResponse(DriverCompletionInfo completionInfo) {
                computeInfo.set(completionInfo);
                publishIfReady();
            }

            @Override
            public void onFailure(Exception e) {
                if (isFatal(e)) {
                    failOnce(e);
                    return;
                }
                recordToleratedFailure(e);
                computeInfo.compareAndSet(null, DriverCompletionInfo.EMPTY);
                publishIfReady();
            }
        };
    }

    /**
     * Registers the coordinator fetch against {@code remoteSink} and returns the compute
     * listener that {@code runCompute} should complete.
     */
    static LocalSourceProducerLifecycle register(
        ExchangeSourceHandler exchangeSource,
        RemoteSink remoteSink,
        boolean failFast,
        SourceOutcomeAccumulator sourceOutcomes,
        ActionListener<DriverCompletionInfo> listener
    ) {
        LocalSourceProducerLifecycle lifecycle = new LocalSourceProducerLifecycle(failFast, sourceOutcomes, listener);
        exchangeSource.addRemoteSink(remoteSink, failFast, () -> {}, 1, lifecycle.remoteSinkListener());
        return lifecycle;
    }

    ActionListener<DriverCompletionInfo> computeListener() {
        return computeListener;
    }

    static String toleratedFailureWarning(Exception e) {
        return "external source failed, results may be incomplete: " + ExceptionsHelper.unwrapCause(e).getMessage();
    }

    private ActionListener<Void> remoteSinkListener() {
        return new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                sinkFinished.set(true);
                publishIfReady();
            }

            @Override
            public void onFailure(Exception e) {
                if (isFatal(e)) {
                    failOnce(e);
                    return;
                }
                recordToleratedFailure(e);
                sinkFinished.set(true);
                publishIfReady();
            }
        };
    }

    private void recordToleratedFailure(Exception e) {
        sourceOutcomes.recordExternalFailure(e);
        externalPartial.set(true);
        toleratedFailures.add(toleratedFailureWarning(e));
    }

    private boolean isFatal(Exception e) {
        if (failFast) {
            return true;
        }
        if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
            return true;
        }
        return EsqlCCSUtils.canAllowPartial(e) == false;
    }

    private void failOnce(Exception e) {
        if (published.compareAndSet(false, true)) {
            listener.onFailure(e);
        }
    }

    private void publishIfReady() {
        DriverCompletionInfo info = computeInfo.get();
        if (info != null && sinkFinished.get() && published.compareAndSet(false, true)) {
            // A finished fetch after finishSinkHandler(e) is not proof the producer produced rows.
            // Reader-leniency DriverCompletionInfo.partial() is not an execution failure.
            if (externalPartial.get()) {
                info = info.withPartial().withAdditionalWarnings(toleratedFailures);
            } else {
                sourceOutcomes.recordExternalSuccess();
            }
            listener.onResponse(info);
        }
    }
}
