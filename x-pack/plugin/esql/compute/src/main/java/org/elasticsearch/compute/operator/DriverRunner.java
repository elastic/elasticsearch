/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.List;

/**
 * Run a set of drivers to completion.
 */
public abstract class DriverRunner {
    private final ThreadContext threadContext;

    public DriverRunner(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    /**
     * Start a driver.
     */
    protected abstract void start(Driver driver, ActionListener<Void> driverListener);

    /**
     * Run all drivers to completion asynchronously.
     */
    public void runToCompletion(List<Driver> drivers, ActionListener<Void> listener) {
        var responseHeadersCollector = new ResponseHeadersCollector(threadContext);
        var failure = new FailureCollector();
        CountDown counter = new CountDown(drivers.size());
        for (int i = 0; i < drivers.size(); i++) {
            Driver driver = drivers.get(i);
            ActionListener<Void> driverListener = new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    done();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.unwrapAndCollect(e);
                    for (Driver d : drivers) {
                        if (driver != d) {
                            d.cancel("Driver [" + driver.sessionId() + "] was cancelled or failed");
                        }
                    }
                    done();
                }

                private void done() {
                    responseHeadersCollector.collect();
                    if (counter.countDown()) {
                        responseHeadersCollector.finish();
                        Exception error = failure.getFailure();
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
}
