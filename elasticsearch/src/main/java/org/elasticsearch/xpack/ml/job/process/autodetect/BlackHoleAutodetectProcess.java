/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A placeholder class simulating the actions of the native Autodetect process.
 * Most methods consume data without performing any action however, after a call to
 * {@link #flushJob(InterimResultsParams)} a {@link org.elasticsearch.xpack.ml.job.process.autodetect.output.FlushAcknowledgement}
 * message is expected on the {@link #readAutodetectResults()} ()} stream. This class writes the flush
 * acknowledgement immediately.
 */
public class BlackHoleAutodetectProcess implements AutodetectProcess {

    private static final Logger LOGGER = Loggers.getLogger(BlackHoleAutodetectProcess.class);
    private static final String FLUSH_ID = "flush-1";

    private final ZonedDateTime startTime;

    private final BlockingQueue<AutodetectResult> results = new ArrayBlockingQueue<>(128);

    public BlackHoleAutodetectProcess() {
        startTime = ZonedDateTime.now();
    }

    @Override
    public void writeRecord(String[] record) throws IOException {
    }

    @Override
    public void writeResetBucketsControlMessage(DataLoadParams params) throws IOException {
    }

    @Override
    public void writeUpdateConfigMessage(String config) throws IOException {
    }

    /**
     * Accept the request do nothing with it but write the flush acknowledgement to {@link #readAutodetectResults()}
     * @param params Should interim results be generated
     * @return {@link #FLUSH_ID}
     */
    @Override
    public String flushJob(InterimResultsParams params) throws IOException {
        FlushAcknowledgement flushAcknowledgement = new FlushAcknowledgement(FLUSH_ID);
        AutodetectResult result = new AutodetectResult(null, null, null, null, null, null, null, null, flushAcknowledgement);
        results.add(result);
        return FLUSH_ID;
    }

    @Override
    public void flushStream() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Iterator<AutodetectResult> readAutodetectResults() {
        // Create a custom iterator here, because ArrayBlockingQueue iterator and stream are not blocking when empty:
        return new Iterator<AutodetectResult>() {

            AutodetectResult result;

            @Override
            public boolean hasNext() {
                try {
                    result = results.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return true;
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
        return true;
    }

    @Override
    public String readError() {
        return "";
    }
}
