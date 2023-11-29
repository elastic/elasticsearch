/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicArray<Map<String, List<String>>> responseHeaders = new AtomicArray<>(drivers.size());
        CountDown counter = new CountDown(drivers.size());
        for (int i = 0; i < drivers.size(); i++) {
            Driver driver = drivers.get(i);
            int driverIndex = i;
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
                    responseHeaders.setOnce(driverIndex, threadContext.getResponseHeaders());
                    if (counter.countDown()) {
                        mergeResponseHeaders(responseHeaders);
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

    private void mergeResponseHeaders(AtomicArray<Map<String, List<String>>> responseHeaders) {
        final Map<String, Set<String>> merged = new HashMap<>();
        for (int i = 0; i < responseHeaders.length(); i++) {
            final Map<String, List<String>> resp = responseHeaders.get(i);
            if (resp == null || resp.isEmpty()) {
                continue;
            }
            for (Map.Entry<String, List<String>> e : resp.entrySet()) {
                // Use LinkedHashSet to retain the order of the values
                merged.computeIfAbsent(e.getKey(), k -> new LinkedHashSet<>(e.getValue().size())).addAll(e.getValue());
            }
        }
        for (Map.Entry<String, Set<String>> e : merged.entrySet()) {
            for (String v : e.getValue()) {
                threadContext.addResponseHeader(e.getKey(), v);
            }
        }
    }
}
