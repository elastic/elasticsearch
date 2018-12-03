/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.analytics.process;

import org.elasticsearch.xpack.ml.process.NativeProcess;

import java.io.IOException;

public interface AnalyticsProcess extends NativeProcess {

    /**
     * Writes a control message that informs the process
     * all data has been sent
     * @throws IOException If an error occurs writing to the process
     */
    void writeEndOfDataMessage() throws IOException;
}
