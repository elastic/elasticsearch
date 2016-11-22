/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.prelert.job.results.AutodetectResult;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;

/**
 * A placeholder class simulating the actions of the native Autodetect process.
 * Most methods consume data without performing any action however, after a call to
 * {@link #flushJob(InterimResultsParams)} a {@link org.elasticsearch.xpack.prelert.job.process.autodetect.output.FlushAcknowledgement}
 * message is expected on the {@link #getProcessOutStream()} stream. This class writes the flush
 * acknowledgement immediately.
 */
public class BlackHoleAutodetectProcess implements AutodetectProcess, Closeable {

    private static final Logger LOGGER = Loggers.getLogger(BlackHoleAutodetectProcess.class);
    private static final String FLUSH_ID = "flush-1";

    private final PipedInputStream processOutStream;
    private final PipedInputStream persistStream;
    private PipedOutputStream pipedProcessOutStream;
    private PipedOutputStream pipedPersistStream;
    private final ZonedDateTime startTime;

    public BlackHoleAutodetectProcess() {
        processOutStream = new PipedInputStream();
        persistStream = new PipedInputStream();
        try {
            // jackson tries to read the first 4 bytes:
            // if we don't do this the autodetect communication would fail starting
            pipedProcessOutStream = new PipedOutputStream(processOutStream);
            pipedProcessOutStream.write(' ');
            pipedProcessOutStream.write(' ');
            pipedProcessOutStream.write(' ');
            pipedProcessOutStream.write('[');
            pipedProcessOutStream.flush();
            pipedPersistStream = new PipedOutputStream(persistStream);
        } catch (IOException e) {
            LOGGER.error("Error connecting PipedOutputStream", e);
        }
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
     * Accept the request do nothing with it but write the flush acknowledgement to {@link #getProcessOutStream()}
     * @param params Should interim results be generated
     * @return {@link #FLUSH_ID}
     */
    @Override
    public String flushJob(InterimResultsParams params) throws IOException {
        FlushAcknowledgement flushAcknowledgement = new FlushAcknowledgement(FLUSH_ID);
        AutodetectResult result = new AutodetectResult(null, null, null, null, null, null, flushAcknowledgement);
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.value(result);
        pipedProcessOutStream.write(builder.string().getBytes(StandardCharsets.UTF_8));
        pipedProcessOutStream.flush();
        return FLUSH_ID;
    }

    @Override
    public void flushStream() throws IOException {
    }

    @Override
    public void close() throws IOException {
        pipedProcessOutStream.write(']');
        pipedProcessOutStream.close();
        pipedPersistStream.close();
    }

    @Override
    public InputStream getProcessOutStream() {
        return processOutStream;
    }

    @Override
    public InputStream getPersistStream() {
        return persistStream;
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
