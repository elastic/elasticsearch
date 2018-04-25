/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xpack.ml.job.process.normalizer.output.NormalizerResultHandler;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface representing the native C++ normalizer process
 */
public interface NormalizerProcess extends Closeable {

    /**
     * Write the record to normalizer. The record parameter should not be encoded
     * (i.e. length encoded) the implementation will appy the corrrect encoding.
     *
     * @param record Plain array of strings, implementors of this class should
     *               encode the record appropriately
     * @throws IOException If the write failed
     */
    void writeRecord(String[] record) throws IOException;

    /**
     * Create a result handler for this process's results.
     * @return results handler
     */
    NormalizerResultHandler createNormalizedResultsHandler();

    /**
     * Returns true if the process still running.
     * @return True if the process is still running
     */
    boolean isProcessAlive();

    /**
     * Read any content in the error output buffer.
     * @return An error message or empty String if no error.
     */
    String readError();
}
