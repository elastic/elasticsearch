/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.categorize;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * A placeholder class simulating the actions of the native categorize process.
 * Most methods consume data without performing any action however, after a call to
 * {@link #flushJob(FlushJobParams)} a {@link FlushAcknowledgement}
 * message is expected on the {@link #readResults()} stream. This class writes the flush
 * acknowledgement immediately.
 */
public class BlackHoleCategorizeProcess implements CategorizeProcess {

    private static final String FLUSH_ID = "flush-1";

    private final ZonedDateTime startTime;
    private final BlockingQueue<AutodetectResult> results = new LinkedBlockingDeque<>();
    private volatile boolean open = true;

    public BlackHoleCategorizeProcess() {
        startTime = ZonedDateTime.now();
    }

    @Override
    public void restoreState(CheckedConsumer<OutputStream, IOException> restorer) {
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void writeRecord(String[] record) throws IOException {
    }

    /**
     * Accept the request do nothing with it but write the flush acknowledgement to {@link #readResults()}
     * @param params Should interim results be generated
     * @return {@link #FLUSH_ID}
     */
    @Override
    public String flushJob(FlushJobParams params) {
        FlushAcknowledgement flushAcknowledgement = new FlushAcknowledgement(FLUSH_ID, null);
        AutodetectResult result = new AutodetectResult(null, null, null, null, null, null, null, null, null, null, flushAcknowledgement);
        results.add(result);
        return FLUSH_ID;
    }

    @Override
    public void flushStream() {
    }

    @Override
    public void close() {
        if (open) {
            AutodetectResult result = new AutodetectResult(null, null, null, null, null, null, null, null, null, null, null);
            results.add(result);
            open = false;
        }
    }

    @Override
    public void kill() {
        open = false;
    }

    @Override
    public Iterator<AutodetectResult> readResults() {
        // Create a custom iterator here, because LinkedBlockingDeque iterator and stream are not blocking when empty:
        return new Iterator<AutodetectResult>() {

            AutodetectResult result;

            @Override
            public boolean hasNext() {
                try {
                    while (open) {
                        result = results.poll(100, TimeUnit.MILLISECONDS);
                        if (result != null) {
                            return true;
                        }
                    }
                    result = results.poll();
                    return result != null;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            @Override
            public AutodetectResult next() {
                return result;
            }
        };
    }

    @Override
    public ZonedDateTime getProcessStartTime() {
        return startTime;
    }

    @Override
    public boolean isProcessAlive() {
        return open;
    }

    @Override
    public boolean isProcessAliveAfterWaiting() {
        try {
            Thread.sleep(45);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return open;
    }

    @Override
    public String readError() {
        return "";
    }
}
