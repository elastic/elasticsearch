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
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;

import java.util.Iterator;

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
     *                      the calling thread if there are no items), or on the cancelling thread if cancelled.
     */
    public final void run(@Nullable Task task, Iterator<Item> itemsIterator, ActionListener<FinalResponse> listener) {

        final var cancellableTask = task instanceof CancellableTask ct ? ct : null;

        // Captures the final result as soon as it's known (either on completion or on cancellation) without necessarily completing the
        // outer listener, because we do not want to complete the outer listener until all sub-tasks are complete
        final var resultListener = new SubscribableListener<FinalResponse>();

        // Completes resultListener (either on completion or on cancellation). Captures a reference to 'this', but within a 'RunOnce' so it
        // is released promptly when executed.
        final var resultListenerCompleter = new RunOnce(() -> {
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
                resultListenerCompleter.run();
                cancellableTask.notifyIfCancelled(itemCancellationListener);
            });
        }

        try (var refs = new RefCountingRunnable(() -> {
            // When all sub-tasks are complete, pass the result from resultListener to the outer listener.
            resultListenerCompleter.run();
            // If not cancelled then resultListener is always complete by this point so the outer listener is completed on this thread.
            // If it's being concurrently cancelled then the outer listener may be completed with the TaskCancelledException on the
            // cancelling thread.
            resultListener.addListener(listener);
        })) {
            while (itemsIterator.hasNext()) {
                final var item = itemsIterator.next();

                // Captures a reference to 'this', but within a 'notifyOnce' so it is released promptly when completed.
                final ActionListener<ItemResponse> itemResponseListener = ActionListener.notifyOnce(new ActionListener<>() {
                    @Override
                    public void onResponse(ItemResponse itemResponse) {
                        onItemResponse(item, itemResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (cancellableTask != null && cancellableTask.isCancelled()) {
                            // Completed on cancellation so it is released promptly, but there's no need to handle the exception.
                            return;
                        }
                        onItemFailure(item, e);
                    }

                    @Override
                    public String toString() {
                        return "[" + CancellableFanOut.this + "][" + item + "]";
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
                sendItemRequest(item, ActionListener.releaseAfter(itemResponseListener, refs.acquire()));
            }
        } catch (Exception e) {
            // NB the listener may have been completed already (by exiting this try block) so this exception may not be sent to the caller,
            // but we cannot do anything else with it; an exception here is a bug anyway.
            logger.error("unexpected failure in [" + this + "]", e);
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
     * Handle a successful response for an item. May be called concurrently for multiple items. Not called if the task is cancelled.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract void onItemResponse(Item item, ItemResponse itemResponse);

    /**
     * Handle a failure for an item. May be called concurrently for multiple items. Not called if the task is cancelled.
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
}
