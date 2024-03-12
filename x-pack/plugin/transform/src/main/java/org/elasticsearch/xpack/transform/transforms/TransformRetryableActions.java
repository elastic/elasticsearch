/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

/**
 * Maintains the state of all currently scheduled retry tasks for all Transforms on this node.
 * This state is not expected to be persisted across the cluster or persisted across cluster restarts.
 * This class is thread safe.
 */
public class TransformRetryableActions {
    private static final Logger logger = LogManager.getLogger(TransformRetryableActions.class);
    private static final Exception stopCall = new CancellableThreads.ExecutionCancelledException("Stop called.");
    private final ThreadPool threadPool;
    private final TransformAuditor auditor;
    private final Map<TransformKey, TransformRetryableAction<?>> unattendedTransformRetryables = new ConcurrentHashMap<>();

    public TransformRetryableActions(ThreadPool threadPool, TransformAuditor auditor) {
        this.threadPool = threadPool;
        this.auditor = auditor;
    }

    /**
     * @param transformId the id of the Transform, used for auditing and unique identification in combination with name
     * @param name the name of this action, used for auditing and unique identification in combination with transformId
     * @param action the function to register.  The retryable action will continuously call this action by passing it the provided listener.
     *               The listener in the consumer must be notified when the task succeeds or fails via the {@link ActionListener#onResponse}
     *               and {@link ActionListener#onFailure} API respectively.  If the listener is not called, the retry logic will break.
     * @param listener this listener will be notified via #onResponse when the task succeeds or via #onFailure when there are no more
     *                 retries left (or runWhile returns false).
     * @param retryScheduledListener this listener will be notified after the first call. If true, another thread has started the retry
     *                               process. If false, the original call was successful, and no retries will happen.
     * @param runWhile this function will be called before every retry.  This can be used to halt the retry logic based on some boolean
     *                 criteria.  When retries are stopped via this method, the retry logic will call the listener's onFailure method with
     *                 the error causing the retries.
     */
    <Response> Runnable register(
        String transformId,
        String name,
        Consumer<ActionListener<Response>> action,
        ActionListener<Response> listener,
        ActionListener<Boolean> retryScheduledListener,
        Supplier<Boolean> runWhile
    ) {
        return unattendedTransformRetryables.compute(new TransformKey(transformId, name), (k, v) -> {
            if (v != null) {
                // do not create another retryable action for the same transform + function
                logger.debug("[{}] {} already exists for transform", k.transformId(), k.functionName());
                return v;
            } else {
                return new TransformRetryableAction<>(
                    k.transformId(),
                    action,
                    ActionListener.runAfter(listener, () -> unattendedTransformRetryables.remove(k)),
                    retryScheduledListener,
                    runWhile
                );
            }
        });
    }

    /**
     * Stops all currently running retries and deregisters all retries for this transformId.
     */
    void stopAll(String transformId) {
        // copy to an temp list, avoiding a modification exception since the cancel function will run a listener that removes from the map
        retriesForTransform(transformId).toList().forEach(task -> {
            var msg = format("Canceling %s", task.getKey());
            logger.debug("[{}] {}", task.getKey().transformId(), msg);
            auditor.info(task.getKey().transformId(), msg);
            task.getValue().cancel(stopCall);
        });
    }

    private Stream<Map.Entry<TransformKey, TransformRetryableAction<?>>> retriesForTransform(String transformId) {
        return unattendedTransformRetryables.entrySet().stream().filter(entry -> entry.getKey().transformId().equals(transformId));
    }

    boolean isStopRetryableCall(Exception e) {
        return e == stopCall;
    }

    List<TransformRetryDescription> retries(String transformId) {
        return retriesForTransform(transformId).map(entry -> entry.getValue().retryDescription())
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();
    }

    private class TransformRetryableAction<Response> extends RetryableAction<Response> implements Runnable {
        private static final Logger logger = LogManager.getLogger(TransformRetryableAction.class);
        private final String transformId;
        private final Consumer<ActionListener<Response>> action;
        private final ActionListener<Boolean> retryScheduledListener;
        private final Supplier<Boolean> runWhile;
        private final AtomicBoolean isRetrying = new AtomicBoolean(false);
        private final AtomicReference<Instant> firstOccurrence = new AtomicReference<>();
        private final AtomicReference<String> errorMessage = new AtomicReference<>();
        private final AtomicInteger retryCount = new AtomicInteger(0);

        TransformRetryableAction(
            String transformId,
            Consumer<ActionListener<Response>> action,
            ActionListener<Response> listener,
            ActionListener<Boolean> retryScheduledListener,
            Supplier<Boolean> runWhile
        ) {
            super(
                logger,
                threadPool,
                // First retry before 1 minute
                TimeValue.timeValueMinutes(1),
                // Then retry before 2 minutes
                TimeValue.timeValueMinutes(2),
                // But retry "forever"
                TimeValue.MAX_VALUE,
                listener,
                threadPool.generic()
            );
            this.transformId = transformId;
            this.action = action;
            this.retryScheduledListener = retryScheduledListener;
            this.runWhile = runWhile;
        }

        @Override
        public void tryAction(ActionListener<Response> listener) {
            if (isRetrying.compareAndSet(false, true)) {
                onFirstRun(listener);
            } else {
                action.accept(listener.delegateResponse((l, e) -> {
                    retryCount.incrementAndGet();
                    l.onFailure(e);
                }));
            }
        }

        private void onFirstRun(ActionListener<Response> listener) {
            action.accept(ActionListener.wrap(r -> {
                // call succeeded, no need to retry
                retryScheduledListener.onResponse(false);
                listener.onResponse(r);
            }, e -> {
                var msg = format("Retrying Transform action due to error: %s.", ExceptionsHelper.unwrapCause(e));
                logger.debug(() -> format("[{}] {}", transformId, msg), e);
                auditor.info(transformId, msg);

                retryCount.incrementAndGet();
                firstOccurrence.set(Instant.now());
                errorMessage.set(msg);

                retryScheduledListener.onResponse(true);
                listener.onFailure(e);
            }));
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return runWhile.get();
        }

        private Optional<TransformRetryDescription> retryDescription() {
            if (retryCount.get() > 0) {
                return Optional.of(new TransformRetryDescription(errorMessage.get(), retryCount.get(), firstOccurrence.get()));
            }
            return Optional.empty();
        }
    }

    private record TransformKey(String transformId, String functionName) {}

    /**
     * Message containing why the Transform is retrying, how many times it has retried, and when the first call happened.
     */
    public record TransformRetryDescription(String message, int retryCount, Instant occurrence) {}
}
