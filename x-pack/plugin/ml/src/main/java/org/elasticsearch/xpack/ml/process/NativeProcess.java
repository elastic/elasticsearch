/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZonedDateTime;

/**
 * Interface representing a native C++ process
 */
public interface NativeProcess extends Closeable {

    /**
     * Is the process ready to receive data?
     * @return {@code true} if the process is ready to receive data
     */
    boolean isReady();

    /**
     * Write the record to the process. The record parameter should not be encoded
     * (i.e. length encoded) the implementation will apply the correct encoding.
     *
     * @param record Plain array of strings, implementors of this class should
     *               encode the record appropriately
     * @throws IOException If the write failed
     */
    void writeRecord(String[] record) throws IOException;

    /**
     * Ask the process to persist its state in the background
     * @throws IOException If writing the request fails
     */
    void persistState() throws IOException;

    /**
     * Flush the output data stream
     */
    void flushStream() throws IOException;

    /**
     * Kill the process.  Do not wait for it to stop gracefully.
     */
    void kill() throws IOException;

    /**
     * The time the process was started
     * @return Process start time
     */
    ZonedDateTime getProcessStartTime();

    /**
     * Returns true if the process still running.
     * Methods instructing the process are essentially
     * asynchronous; the command will be continue to execute in the process after
     * the call has returned.
     * This method tests whether something catastrophic
     * occurred in the process during its execution.
     * @return True if the process is still running
     */
    boolean isProcessAlive();

    /**
     * Check whether the process terminated given a grace period.
     *
     * Processing errors are highly likely caused by the process being unexpectedly
     * terminated.
     *
     * Workaround: As we can not easily check if the process is alive, we rely on
     * the logPipe being ended. As the loghandler runs in another thread which
     * might fall behind this one, we give it a grace period.
     *
     * @return false if process has ended for sure, true if it probably still runs
     */
    boolean isProcessAliveAfterWaiting();

    /**
     * Read any content in the error output buffer.
     * @return An error message or empty String if no error.
     */
    String readError();
}
