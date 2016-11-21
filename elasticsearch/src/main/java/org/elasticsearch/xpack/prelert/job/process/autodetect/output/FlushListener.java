/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.output;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class FlushListener {

    final ConcurrentMap<String, CountDownLatch> awaitingFlushed = new ConcurrentHashMap<>();
    final AtomicBoolean cleared = new AtomicBoolean(false);

    boolean waitForFlush(String flushId, long timeout) {
        if (cleared.get()) {
            return false;
        }

        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch previous = awaitingFlushed.putIfAbsent(flushId, latch);
        if (previous != null) {
            latch = previous;
        }
        try {
            return latch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    void acknowledgeFlush(String flushId) {
        CountDownLatch latch = awaitingFlushed.get(flushId);
        if (latch == null) {
            return;
        }

        latch.countDown();
    }

    void clear() {
        if (cleared.compareAndSet(false, true)) {
            Iterator<ConcurrentMap.Entry<String, CountDownLatch>> latches = awaitingFlushed.entrySet().iterator();
            while (latches.hasNext()) {
                latches.next().getValue().countDown();
                latches.remove();
            }
        }
    }

}
