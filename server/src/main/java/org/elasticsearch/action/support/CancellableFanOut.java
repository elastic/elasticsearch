/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;

import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Allows an action to fan-out to several sub-actions and accumulate their results, but which reacts to a cancellation by releasing all
 * references to itself, and hence the partially-accumulated results, allowing them to be garbage-collected. This is a useful protection for
 * cases where the results may consume a lot of heap (e.g. stats) but the final response may be delayed by a single slow node for long
 * enough that the client gives up.
 * <p>
 * Note that it's easy to accidentally capture another reference to this class when implementing it, and this will prevent the early release
 * of any accumulated results. Beware of lambdas and method references. You must test your implementation carefully (using e.g.
 * {@code ReachabilityChecker}) to make sure it doesn't do this.
 */
public abstract class CancellableFanOut<Item, ItemResponse, FinalResponse> {

    private static final Logger logger = LogManager.getLogger(CancellableFanOut.class);

    /**
     * Run the fan-out action.
     *
     * @param task          The task to watch for cancellations. If {@code null} or not a {@link CancellableTask} then the fan-out still
     *                      works, just without any cancellation handling.
     * @param itemsIterator The items over which to fan out. Iterated on the calling thread.
     * @param listener      A listener for the final response, which is completed after all the fanned-out actions have completed. It is not
     *                      completed promptly on cancellation. Completed on the thread that handles the final per-item response (or
     *                      the calling thread if there are no items).
     */
    public final void run(@Nullable Task task, Iterator<Item> itemsIterator, ActionListener<FinalResponse> listener) {

        final var cancellableTask = task instanceof CancellableTask ct ? ct : null;

        // Captures the final result as soon as it's known (either on completion or on cancellation) without necessarily completing the
        // outer listener, because we do not want to complete the outer listener until all sub-tasks are complete
        final var resultListener = new SubscribableListener<FinalResponse>();

        // Completes resultListener (either on completion or on cancellation). Captures a reference to 'this', but within an
        // 'AtomicReference' which is cleared, releasing the reference promptly, when executed.
        final var resultListenerCompleter = new AtomicReference<Runnable>(() -> {
            if (cancellableTask != null && cancellableTask.notifyIfCancelled(resultListener)) {
                return;
            }
            // It's important that we complete resultListener before returning, because otherwise there's a risk that a cancellation arrives
            // later which might unexpectedly complete the final listener on a transport thread.
            ActionListener.completeWith(resultListener, this::onCompletion);
        });

        // Collects the per-item listeners up so they can all be completed exceptionally on cancellation. Never completed successfully.
        final var itemCancellationListener = new SubscribableListener<ItemResponse>();
        if (cancellableTask != null) {
            cancellableTask.addListener(() -> {
                assert cancellableTask.isCancelled();
                // probably on a transport thread and we don't know if any of the callbacks are slow so we must avoid running them by
                // blocking the thread which might add a subscriber to resultListener until after we've completed it
                final var semaphore = new Semaphore(0);
                // resultListenerCompleter is currently either a no-op, or else it immediately completes resultListener with a cancellation
                // while it has no subscribers, so either way this semaphore is not held for long
                resultListenerCompleter.getAndSet(semaphore::acquireUninterruptibly).run();
                semaphore.release();
                // finally, release refs to all the per-item listeners (without calling onItemFailure, so this is also fast)
                cancellableTask.notifyIfCancelled(itemCancellationListener);
            });
        }

        try (var refs = new RefCountingRunnable(new SubtasksCompletionHandler<>(resultListenerCompleter, resultListener, listener))) {
            while (itemsIterator.hasNext()) {
                final var item = itemsIterator.next();

                // Captures a reference to 'this', but within a 'notifyOnce' so it is released promptly when completed.
                final ActionListener<ItemResponse> itemResponseListener = ActionListener.notifyOnce(new ActionListener<>() {
                    @Override
                    public void onResponse(ItemResponse itemResponse) {
                        try {
                            onItemResponse(item, itemResponse);
                        } catch (Exception e) {
                            logger.error(
                                () -> Strings.format(
                                    "unexpected exception handling [%s] for item [%s] in [%s]",
                                    itemResponse,
                                    item,
                                    CancellableFanOut.this
                                ),
                                e
                            );
                            assert false : e;
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (cancellableTask != null && cancellableTask.isCancelled()) {
                            // Completed on cancellation so it is released promptly, but there's no need to handle the exception.
                            return;
                        }
                        onItemFailure(item, e); // must not throw, enforced by the ActionListener#notifyOnce wrapper
                    }

                    @Override
                    public String toString() {
                        return "[" + CancellableFanOut.this + "][" + listener + "][" + item + "]";
                    }
                });

                if (cancellableTask != null) {
                    if (cancellableTask.isCancelled()) {
                        return;
                    }

                    // Register this item's listener for prompt cancellation notification.
                    itemCancellationListener.addListener(itemResponseListener);
                }

                // Process the item, capturing a ref to make sure the outer listener is completed after this item is processed.
                ActionListener.run(ActionListener.releaseAfter(itemResponseListener, refs.acquire()), l -> sendItemRequest(item, l));
            }
        } catch (Exception e) {
            // NB the listener may have been completed already (by exiting this try block) so this exception may not be sent to the caller,
            // but we cannot do anything else with it; an exception here is a bug anyway.
            logger.error("unexpected failure in [" + this + "][" + listener + "]", e);
            assert false : e;
            throw e;
        }
    }

    /**
     * Run the action (typically by sending a transport request) for an individual item. Called in sequence on the thread that invoked
     * {@link #run}. May not be called for every item if the task is cancelled during the iteration.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract void sendItemRequest(Item item, ActionListener<ItemResponse> listener);

    /**
     * Handle a successful response for an item. May be called concurrently for multiple items. Not called if the task is cancelled. Must
     * not throw any exceptions.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract void onItemResponse(Item item, ItemResponse itemResponse);

    /**
     * Handle a failure for an item. May be called concurrently for multiple items. Not called if the task is cancelled. Must not throw any
     * exceptions.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract void onItemFailure(Item item, Exception e);

    /**
     * Called when responses for all items have been processed, on the thread that processed the last per-item response or possibly the
     * thread which called {@link #run} if all items were processed before {@link #run} returns. Not called if the task is cancelled.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract FinalResponse onCompletion() throws Exception;

    private static class SubtasksCompletionHandler<FinalResponse> implements Runnable {
        private final AtomicReference<Runnable> resultListenerCompleter;
        private final SubscribableListener<FinalResponse> resultListener;
        private final ActionListener<FinalResponse> listener;

        private SubtasksCompletionHandler(
            AtomicReference<Runnable> resultListenerCompleter,
            SubscribableListener<FinalResponse> resultListener,
            ActionListener<FinalResponse> listener
        ) {
            this.resultListenerCompleter = resultListenerCompleter;
            this.resultListener = resultListener;
            this.listener = listener;
        }

        @Override
        public void run() {
            // When all sub-tasks are complete, pass the result from resultListener to the outer listener.
            resultListenerCompleter.getAndSet(() -> {}).run();
            // May block (very briefly) if there's a concurrent cancellation, so that we are sure the resultListener is now complete and
            // therefore the outer listener is completed on this thread.
            assert resultListener.isDone();
            resultListener.addListener(listener);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + listener.toString() + "]";
        }
    }
}
