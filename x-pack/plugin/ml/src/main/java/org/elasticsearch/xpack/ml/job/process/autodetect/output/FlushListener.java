/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class FlushListener {

    final ConcurrentMap<String, FlushAcknowledgementHolder> awaitingFlushed = new ConcurrentHashMap<>();
    final RunOnce onClear = new RunOnce(() -> {
        Iterator<ConcurrentMap.Entry<String, FlushAcknowledgementHolder>> latches = awaitingFlushed.entrySet().iterator();
        while (latches.hasNext()) {
            latches.next().getValue().latch.countDown();
            latches.remove();
        }
    });

    @Nullable
    FlushAcknowledgement waitForFlush(String flushId, Duration timeout) throws Exception {
        if (onClear.hasRun()) {
            return null;
        }

        FlushAcknowledgementHolder holder = awaitingFlushed.computeIfAbsent(flushId, (key) -> new FlushAcknowledgementHolder(flushId));
        if (holder.latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            if (holder.flushException != null) {
                throw holder.flushException;
            }
            return holder.flushAcknowledgement;
        }
        return null;
    }

    void acknowledgeFlush(FlushAcknowledgement flushAcknowledgement, @Nullable Exception exception) {
        // acknowledgeFlush(...) could be called before waitForFlush(...)
        // a flush api call writes a flush command to the analytical process and then via a different thread the
        // result reader then reads whether the flush has been acked.
        String flushId = flushAcknowledgement.getId();
        FlushAcknowledgementHolder holder = awaitingFlushed.computeIfAbsent(flushId, (key) -> new FlushAcknowledgementHolder(flushId));
        holder.flushAcknowledgement = flushAcknowledgement;
        holder.flushException = exception;
        holder.latch.countDown();
    }

    void clear(String flushId) {
        awaitingFlushed.remove(flushId);
    }

    void clear() {
        onClear.run();
    }

    private static class FlushAcknowledgementHolder {

        private final CountDownLatch latch;
        private volatile FlushAcknowledgement flushAcknowledgement;
        private volatile Exception flushException;

        private FlushAcknowledgementHolder(String flushId) {
            this.flushAcknowledgement = new FlushAcknowledgement(flushId, 0L, true);
            this.latch = new CountDownLatch(1);
        }
    }
}
