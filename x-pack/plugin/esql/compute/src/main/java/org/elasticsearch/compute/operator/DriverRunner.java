/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.List;

/**
 * Run a set of drivers to completion.
 */
public abstract class DriverRunner {
    /**
     * Start a driver.
     */
    protected abstract void start(Driver driver, ActionListener<Void> done);

    /**
     * Run all drivers to completion asynchronously.
     */
    public void runToCompletion(List<Driver> drivers, ActionListener<List<Driver.Result>> listener) {
        if (drivers.isEmpty()) {
            listener.onResponse(List.of());
            return;
        }
        CountDown counter = new CountDown(drivers.size());
        AtomicArray<Driver.Result> results = new AtomicArray<>(drivers.size());

        for (int d = 0; d < drivers.size(); d++) {
            int index = d;
            Driver driver = drivers.get(index);
            ActionListener<Void> done = new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    results.setOnce(index, Driver.Result.success());
                    if (counter.countDown()) {
                        done();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    results.set(index, Driver.Result.failure(e));
                    drivers.forEach(Driver::cancel);
                    if (counter.countDown()) {
                        done();
                    }
                }

                private void done() {
                    listener.onResponse(results.asList());
                }
            };
            start(driver, done);
        }
    }
}
