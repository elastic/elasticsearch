/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.Lifecycle;

import java.util.Objects;

/**
 * {@code AbstractLifecycleRunnable} is a service-lifecycle aware {@link AbstractRunnable}.
 * <p>
 * This simplifies the running and rescheduling of {@link Lifecycle}-based {@code Runnable}s.
 */
public abstract class AbstractLifecycleRunnable extends AbstractRunnable {
    /**
     * The monitored lifecycle for the associated service.
     */
    private final Lifecycle lifecycle;
    /**
     * The service's logger (note: this is passed in!).
     */
    private final Logger logger;

    /**
     * {@link AbstractLifecycleRunnable} must be aware of the actual {@code lifecycle} to react properly.
     *
     * @param lifecycle The lifecycle to react too
     * @param logger The logger to use when logging
     * @throws NullPointerException if any parameter is {@code null}
     */
    public AbstractLifecycleRunnable(Lifecycle lifecycle, Logger logger) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle must not be null");
        this.logger = Objects.requireNonNull(logger, "logger must not be null");
    }

    /**
     * {@inheritDoc}
     * <p>
     * This invokes {@link #doRunInLifecycle()} <em>only</em> if the {@link #lifecycle} is not stopped or closed. Otherwise it exits
     * immediately.
     */
    @Override
    protected final void doRun() throws Exception {
        // prevent execution if the service is stopped
        if (lifecycle.stoppedOrClosed()) {
            logger.trace("lifecycle is stopping. exiting");
            return;
        }

        doRunInLifecycle();
    }

    /**
     * Perform runnable logic, but only if the {@link #lifecycle} is <em>not</em> stopped or closed.
     *
     * @throws InterruptedException if the run method throws an {@link InterruptedException}
     */
    protected abstract void doRunInLifecycle() throws Exception;

    /**
     * {@inheritDoc}
     * <p>
     * This overrides the default behavior of {@code onAfter} to add the caveat that it only runs if the {@link #lifecycle} is <em>not</em>
     * stopped or closed.
     * <p>
     * Note: this does not guarantee that it won't be stopped concurrently as it invokes {@link #onAfterInLifecycle()},
     * but it's a solid attempt at preventing it. For those that use this for rescheduling purposes, the next invocation would be
     * effectively cancelled immediately if that's the case.
     *
     * @see #onAfterInLifecycle()
     */
    @Override
    public final void onAfter() {
        if (lifecycle.stoppedOrClosed() == false) {
            onAfterInLifecycle();
        }
    }

    /**
     * This method is invoked in the finally block of the run method, but it is only executed if the {@link #lifecycle} is <em>not</em>
     * stopped or closed.
     * <p>
     * This method is most useful for rescheduling the next iteration of the current runnable.
     */
    protected void onAfterInLifecycle() {
        // nothing by default
    }
}
