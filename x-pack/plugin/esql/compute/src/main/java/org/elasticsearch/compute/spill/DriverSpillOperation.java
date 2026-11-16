/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.Nullable;

import java.util.function.Consumer;

/**
 * A storage operation issued by an operator: an async action on the driver context for as long as it runs, so that the
 * driver waits for it before it closes, and a blocked state the operator returns while it waits. The operation is
 * registered before it is issued, on the driver thread, which is before the driver's last loop iteration by
 * construction; its completion, from whichever thread, deregisters it after the outcome is recorded, so that an operator
 * that wakes up finds the outcome in place.
 */
public final class DriverSpillOperation<T> {

    private final String reason;
    private final SpillCompletion<T> completion = new SpillCompletion<>();

    private DriverSpillOperation(String reason) {
        this.reason = reason;
    }

    /**
     * Registers the async action and issues the operation through {@code issue}, which receives the listener to hand to
     * the backend. If {@code issue} throws before completing the listener, the operation fails with that exception and
     * the action is deregistered; if it throws after completing it, the exception is the issuer's bug and propagates,
     * since a second completion would deregister the action twice and let the driver close under another operation.
     */
    public static <T> DriverSpillOperation<T> issue(DriverContext driverContext, String reason, Consumer<ActionListener<T>> issue) {
        DriverSpillOperation<T> operation = new DriverSpillOperation<>(reason);
        driverContext.addAsyncAction();
        ActionListener<T> listener = ActionListener.runAfter(operation.completion.listener(), driverContext::removeAsyncAction);
        try {
            issue.accept(listener);
        } catch (RuntimeException e) {
            if (operation.completion.isDone()) {
                throw e;
            }
            listener.onFailure(e);
        }
        return operation;
    }

    public SpillCompletion<T> completion() {
        return completion;
    }

    public boolean isDone() {
        return completion.isDone();
    }

    @Nullable
    public T result() {
        return completion.result();
    }

    @Nullable
    public Exception failure() {
        return completion.failure();
    }

    /**
     * What the operator returns from its blocked check while this operation runs.
     */
    public IsBlockedResult blocked() {
        return completion.blocked(reason);
    }
}
