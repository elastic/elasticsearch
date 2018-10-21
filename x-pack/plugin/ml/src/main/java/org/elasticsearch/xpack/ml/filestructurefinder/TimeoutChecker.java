/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.grok.Grok;

import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class can be used to keep track of when a long running operation started and
 * to check whether it has run for longer than permitted.
 *
 * An object should be constructed at the beginning of the operation and then the
 * {@link #check} method called periodically during the processing of the operation.
 *
 * This class does not use the {@link Thread#interrupt} mechanism because some other
 * methods already convert interruptions to other types of exceptions (for example
 * {@link Grok#captures}) and this would lead to non-uniform exception types and
 * misleading error messages in the event that the interrupt was handled by one of
 * these methods.  The code in the long running operation would still have to
 * periodically call {@link Thread#interrupted}, so it is not much more of an
 * inconvenience to have to periodically call this class's {@link #check} method.
 */
public class TimeoutChecker implements Closeable {

    private final String operation;
    private final ScheduledFuture<?> future;
    private final TimeValue timeout;
    private volatile boolean timeoutExceeded;

    /**
     * The constructor should be called at the start of the operation whose duration
     * is to be checked, as the timeout is measured relative to time of construction.
     * @param operation A description of the operation whose duration is to be checked.
     * @param timeout The timeout period.  If <code>null</code> then there is no timeout.
     * @param scheduler Used to schedule the timer.  This may be <code>null</code>
     *                  in the case where {@code timeout} is also <code>null</code>.
     */
    public TimeoutChecker(String operation, TimeValue timeout, ScheduledExecutorService scheduler) {
        this.operation = operation;
        this.timeout = timeout;
        this.future = (timeout != null) ? scheduler.schedule(this::setTimeoutExceeded, timeout.nanos(), TimeUnit.NANOSECONDS) : null;
    }

    /**
     * Stops the timer if running.
     */
    @Override
    public void close() {
        FutureUtils.cancel(future);
    }

    /**
     * Check whether the operation has been running longer than the permitted time.
     * @param where Which stage of the operation is currently in progress?
     * @throws ElasticsearchTimeoutException If the operation is found to have taken longer than the permitted time.
     */
    public void check(String where) {

        if (timeoutExceeded) {
            throw new ElasticsearchTimeoutException("Aborting " + operation + " during [" + where +
                "] as it has taken longer than the timeout of [" + timeout + "]");
        }
    }

    private void setTimeoutExceeded() {
        timeoutExceeded = true;
    }
}
