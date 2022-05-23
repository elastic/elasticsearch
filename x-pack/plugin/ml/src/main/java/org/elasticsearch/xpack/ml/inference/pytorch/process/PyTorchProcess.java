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
import org.elasticsearch.xpack.ml.process.NativeProcess;

import java.io.IOException;
import java.util.Iterator;

/**
 * Interface representing the native C++ pytorch process
 */
public interface PyTorchProcess extends NativeProcess {

    /**
     * Load the model into the process
     * @param modelId the model id
     * @param index the index where the model is stored
     * @param stateStreamer the pytorch state streamer
     * @param listener a listener that gets notified when the loading has completed
     */
    void loadModel(String modelId, String index, PyTorchStateStreamer stateStreamer, ActionListener<Boolean> listener);

    /**
     * @return stream of pytorch results
     */
    Iterator<PyTorchResult> readResults();

    /**
     * Writes an inference request to the process
     * @param jsonRequest the inference request as json
     * @throws IOException If writing the request fails
     */
    void writeInferenceRequest(BytesReference jsonRequest) throws IOException;
}
