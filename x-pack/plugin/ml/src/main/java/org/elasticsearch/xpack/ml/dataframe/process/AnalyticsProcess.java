/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.xpack.ml.process.NativeProcess;

import java.io.IOException;
import java.util.Iterator;

public interface AnalyticsProcess<ProcessResult> extends NativeProcess {

    /**
     * Writes a control message that informs the process
     * all data has been sent
     * @throws IOException If an error occurs writing to the process
     */
    void writeEndOfDataMessage() throws IOException;

    /**
     * @return stream of data frame analytics results.
     */
    Iterator<ProcessResult> readAnalyticsResults();

    /**
     * Read anything left in the stream before
     * closing the stream otherwise if the process
     * tries to write more after the close it gets
     * a SIGPIPE
     */
    void consumeAndCloseOutputStream();

    /**
     *
     * @return the process config
     */
    AnalyticsProcessConfig getConfig();
}
