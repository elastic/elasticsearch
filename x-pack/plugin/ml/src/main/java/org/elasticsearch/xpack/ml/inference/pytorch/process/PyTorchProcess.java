/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.xpack.ml.process.NativeProcess;

import java.io.IOException;

public interface PyTorchProcess extends NativeProcess {

    void loadModel(String modelBase64, int modelSizeAfterUnbase64) throws IOException;
}
