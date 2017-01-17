/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.data;

import org.elasticsearch.xpack.ml.job.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;

import java.io.InputStream;
import java.util.function.Consumer;

public interface DataProcessor {

    /**
     * Passes data to the native process.
     * This is a blocking call that won't return until all the data has been
     * written to the process.
     *
     * An ElasticsearchStatusException will be thrown is any of these error conditions occur:
     * <ol>
     *     <li>If a configured field is missing from the CSV header</li>
     *     <li>If JSON data is malformed and we cannot recover parsing</li>
     *     <li>If a high proportion of the records the timestamp field that cannot be parsed</li>
     *     <li>If a high proportion of the records chronologically out of order</li>
     * </ol>
     *
     * @param jobId     the jobId
     * @param input     Data input stream
     * @param params    Data processing parameters
     * @return Count of records, fields, bytes, etc written
     */
    DataCounts processData(String jobId, InputStream input, DataLoadParams params);

    /**
     * Flush the running job, ensuring that the native process has had the
     * opportunity to process all data previously sent to it with none left
     * sitting in buffers.
     *
     * @param jobId The job to flush
     * @param interimResultsParams Parameters about whether interim results calculation
     * should occur and for which period of time
     */
    void flushJob(String jobId, InterimResultsParams interimResultsParams);

    void openJob(String jobId, boolean ignoreDowntime, Consumer<Exception> handler);

    /**
     * Stop the running job and mark it as finished.<br>
     *  @param jobId The job to stop
     *
     */
    void closeJob(String jobId);
}
