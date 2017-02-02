/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Iterator;

/**
 * Interface representing the native C++ autodetect process
 */
public interface AutodetectProcess extends Closeable {

    /**
     * Write the record to autodetect. The record parameter should not be encoded
     * (i.e. length encoded) the implementation will appy the corrrect encoding.
     *
     * @param record Plain array of strings, implementors of this class should
     *               encode the record appropriately
     * @throws IOException If the write failed
     */
    void writeRecord(String[] record) throws IOException;

    /**
     * Write the reset buckets control message
     * @param params Reset bucket options
     * @throws IOException If write reset mesage fails
     */
    void writeResetBucketsControlMessage(DataLoadParams params) throws IOException;

    /**
     * Write an update configuration message
     * @param config Config message
     * @throws IOException If the write config message fails
     */
    void writeUpdateConfigMessage(String config) throws IOException;

    /**
     * Flush the job pushing any stale data into autodetect.
     * Every flush command generates a unique flush Id which will be output
     * in a flush acknowledgment by the autodetect process once the flush has
     * been processed.
     *
     * @param params Should interim results be generated
     * @return The flush Id
     * @throws IOException If the flush failed
     */
    String flushJob(InterimResultsParams params) throws IOException;

    /**
     * Flush the output data stream
     */
    void flushStream() throws IOException;

    /**
     * @return stream of autodetect results.
     */
    Iterator<AutodetectResult> readAutodetectResults();

    /**
     * The time the process was started
     * @return Process start time
     */
    ZonedDateTime getProcessStartTime();

    /**
     * Returns true if the process still running.
     * Methods such as {@link #flushJob(InterimResultsParams)} are essentially
     * asynchronous the command will be continue to execute in the process after
     * the call has returned. This method tests whether something catastrophic
     * occurred in the process during its execution.
     * @return True if the process is still running
     */
    boolean isProcessAlive();

    /**
     * Read any content in the error output buffer.
     * @return An error message or empty String if no error.
     */
    String readError();
}
