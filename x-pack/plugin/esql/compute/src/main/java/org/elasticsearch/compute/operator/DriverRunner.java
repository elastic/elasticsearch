/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Run a set of drivers to completion.
 */
public abstract class DriverRunner {
    /**
     * Start a driver.
     */
    protected abstract void start(Driver driver, ActionListener<Void> driverListener);

    /**
     * Run all drivers to completion asynchronously.
     */
    public void runToCompletion(List<Driver> drivers, ActionListener<Void> listener) {
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDown counter = new CountDown(drivers.size());
        for (Driver driver : drivers) {
            ActionListener<Void> driverListener = new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    done();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.getAndUpdate(first -> {
                        if (first == null) {
                            return e;
                        }
                        if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
                            return first;
                        } else {
                            if (ExceptionsHelper.unwrap(first, TaskCancelledException.class) != null) {
                                return e;
                            } else {
                                if (first != e) {
                                    first.addSuppressed(e);
                                }
                                return first;
                            }
                        }
                    });
                    for (Driver d : drivers) {
                        if (driver != d) {
                            d.cancel("Driver [" + driver.sessionId() + "] was cancelled or failed");
                        }
                    }
                    done();
                }

                private void done() {
                    if (counter.countDown()) {
                        for (Driver d : drivers) {
                            Releasables.close(d.driverContext().getSnapshot().releasables());
                        }
                        Exception error = failure.get();
                        if (error != null) {
                            listener.onFailure(error);
                        } else {
                            listener.onResponse(null);
                        }
                    }
                }
            };

            start(driver, driverListener);
        }
    }

    public static void runToCompletion(Executor executor, List<Driver> drivers) {
        DriverRunner runner = new DriverRunner() {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                Driver.start(executor, driver, driverListener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        runner.runToCompletion(drivers, future);
        future.actionGet();
    }
}
