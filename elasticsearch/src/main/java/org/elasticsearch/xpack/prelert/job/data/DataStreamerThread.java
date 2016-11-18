/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.data;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.DataLoadParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

// NORELEASE - Use ES ThreadPool
public final class DataStreamerThread extends Thread {
    private static final Logger LOGGER = Loggers.getLogger(DataStreamerThread.class);

    private DataCounts stats;
    private final String jobId;
    private final String contentEncoding;
    private final DataLoadParams params;
    private final InputStream input;
    private final DataStreamer dataStreamer;
    private ElasticsearchException jobException;
    private IOException iOException;

    public DataStreamerThread(DataStreamer dataStreamer, String jobId, String contentEncoding,
            DataLoadParams params, InputStream input) {
        super("DataStreamer-" + jobId);

        this.dataStreamer = dataStreamer;
        this.jobId = jobId;
        this.contentEncoding = contentEncoding;
        this.params = params;
        this.input = input;
    }

    @Override
    public void run() {
        try {
            stats = dataStreamer.streamData(contentEncoding, jobId, input, params);
        } catch (ElasticsearchException e) {
            jobException = e;
        } catch (IOException e) {
            iOException = e;
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                LOGGER.warn("Exception closing the data input stream", e);
            }
        }
    }

    /**
     * This method should only be called <b>after</b> the thread
     * has joined other wise the result could be <code>null</code>
     * (best case) or undefined.
     */
    public DataCounts getDataCounts() {
        return stats;
    }

    /**
     * If a Job exception was thrown during the run of this thread it
     * is accessed here. Only call this method after the thread has joined.
     */
    public Optional<ElasticsearchException> getJobException() {
        return Optional.ofNullable(jobException);
    }

    /**
     * If an IOException was thrown during the run of this thread it
     * is accessed here. Only call this method after the thread has joined.
     */
    public Optional<IOException> getIOException() {
        return Optional.ofNullable(iOException);
    }

    public String getJobId() {
        return jobId;
    }
}
