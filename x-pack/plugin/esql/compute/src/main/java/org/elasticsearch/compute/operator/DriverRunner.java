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
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
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
                            if (d.status().status() == DriverStatus.Status.QUEUED) {
                                d.close();
                            } else {
                                cleanUpDriverContext(d.driverContext());
                            }
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

    /**
     * Run all the of the listed drivers in the supplier {@linkplain ThreadPool}.
     * @return the headers added to the context while running the drivers
     */
    public static Map<String, List<String>> runToCompletion(ThreadPool threadPool, int maxIterations, List<Driver> drivers) {
        DriverRunner runner = new DriverRunner() {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                Driver.start(threadPool.executor("esql"), driver, maxIterations, driverListener);
            }
        };
        AtomicReference<Map<String, List<String>>> responseHeaders = new AtomicReference<>();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        runner.runToCompletion(drivers, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                responseHeaders.set(threadPool.getThreadContext().getResponseHeaders());
                future.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                future.onFailure(e);
            }
        });
        future.actionGet();
        return responseHeaders.get();
    }

    /** Cleans up an outstanding resources from the context. For now, it's just releasables. */
    static void cleanUpDriverContext(DriverContext driverContext) {
        var itr = driverContext.getSnapshot().releasables().iterator();
        while (itr.hasNext()) {
            Releasables.closeExpectNoException(itr.next());
            itr.remove();
        }
    }
}
