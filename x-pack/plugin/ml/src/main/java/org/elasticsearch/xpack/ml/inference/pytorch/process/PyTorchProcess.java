/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.pytorch.ModelStorage;
import org.elasticsearch.xpack.ml.process.NativeProcess;

import java.io.IOException;
import java.util.Iterator;

public interface PyTorchProcess extends NativeProcess {

    Iterator<PyTorchResult> readResults();

    /**
     * Writes an inference request to the process and returns the request id
     */
    String writeInferenceRequest(double[] inputs) throws IOException;

    void loadModel(PyTorchStateStreamer stateStreamer, ModelStorage storage) throws IOException;
}
