/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class SideChannelSupplierTests extends ESTestCase {
    public void testManyThreads() throws ExecutionException, InterruptedException {
        OnlyOneAtATimeSupplier supplier = new OnlyOneAtATimeSupplier();
        int threads = 5;
        int iters = 100_000;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> wait = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                wait.add(exec.submit(() -> {
                    for (int i = 0; i < iters; i++) {
                        supplier.get().close();
                    }
                }));
            }
            for (Future<?> w : wait) {
                w.get();
            }
        } finally {
            exec.shutdown();
        }
    }

    class OnlyOneAtATimeSupplier extends SideChannel.Supplier<OnlyOneAtATime> {
        @Override
        protected OnlyOneAtATime build() {
            return new OnlyOneAtATime(this);
        }
    }

    class OnlyOneAtATime extends SideChannel {
        private static final AtomicBoolean built = new AtomicBoolean();

        protected OnlyOneAtATime(Supplier<?> mySupplier) {
            super(mySupplier);
            if (built.getAndSet(true)) {
                throw new IllegalStateException("built more than one");
            }
        }

        @Override
        protected void closeSideChannel() {
            built.set(false);
        }
    }
}
