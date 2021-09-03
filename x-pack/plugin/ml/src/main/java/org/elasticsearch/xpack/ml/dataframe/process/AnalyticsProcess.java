/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.client.Client;
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
     *
     * @return the process config
     */
    AnalyticsProcessConfig getConfig();

    /**
     * Restores the model state from a previously persisted one
     * @param client the client to use for fetching the state documents
     * @param stateDocIdPrefix the prefix of ids of the state documents
     */
    void restoreState(Client client, String stateDocIdPrefix) throws IOException;
}
