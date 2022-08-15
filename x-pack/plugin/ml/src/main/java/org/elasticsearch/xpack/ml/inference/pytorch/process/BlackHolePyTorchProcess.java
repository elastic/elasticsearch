/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;
import org.elasticsearch.xpack.ml.process.BlackHoleResultIterator;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class BlackHolePyTorchProcess implements PyTorchProcess {

    private final ZonedDateTime startTime;
    private volatile boolean running = true;
    private final BlockingQueue<PyTorchResult> results = new LinkedBlockingDeque<>();

    public BlackHolePyTorchProcess() {
        startTime = ZonedDateTime.now();
    }

    @Override
    public void loadModel(String modelId, String index, PyTorchStateStreamer stateStreamer, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public Iterator<PyTorchResult> readResults() {
        return new BlackHoleResultIterator<>(results, () -> running);
    }

    @Override
    public void writeInferenceRequest(BytesReference jsonRequest) throws IOException {}

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void writeRecord(String[] record) throws IOException {}

    @Override
    public void persistState() throws IOException {}

    @Override
    public void persistState(long snapshotTimestampMs, String snapshotId, String snapshotDescription) throws IOException {}

    @Override
    public void flushStream() throws IOException {}

    @Override
    public void kill(boolean awaitCompletion) throws IOException {
        running = false;
    }

    @Override
    public ZonedDateTime getProcessStartTime() {
        return startTime;
    }

    @Override
    public boolean isProcessAlive() {
        return running;
    }

    @Override
    public boolean isProcessAliveAfterWaiting() {
        try {
            Thread.sleep(45);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return running;
    }

    @Override
    public String readError() {
        return "";
    }

    @Override
    public void close() throws IOException {
        running = false;
    }
}
