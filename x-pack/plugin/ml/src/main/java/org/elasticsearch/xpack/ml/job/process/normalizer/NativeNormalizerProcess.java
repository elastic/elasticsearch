/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xpack.ml.job.process.normalizer.output.NormalizerResultHandler;
import org.elasticsearch.xpack.ml.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.process.NativeController;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;

/**
 * Normalizer process using native code.
 */
class NativeNormalizerProcess extends AbstractNativeProcess implements NormalizerProcess {

    private static final String NAME = "normalizer";

    NativeNormalizerProcess(String jobId, NativeController nativeController, InputStream logStream, OutputStream processInStream,
                            InputStream processOutStream, Duration processConnectTimeout) {
        super(jobId, nativeController, logStream, processInStream, processOutStream, null, 0, Collections.emptyList(), (ignore) -> {},
            processConnectTimeout);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void persistState() {
        // nothing to persist
    }

    @Override
    public NormalizerResultHandler createNormalizedResultsHandler() {
        return new NormalizerResultHandler(processOutStream());
    }
}
