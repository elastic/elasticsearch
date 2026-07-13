/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xpack.ml.job.process.normalizer.output.NormalizerResultHandler;
import org.elasticsearch.xpack.ml.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;

import java.util.Collections;

/**
 * Normalizer process using native code.
 */
class NativeNormalizerProcess extends AbstractNativeProcess implements NormalizerProcess {

    private static final String NAME = "normalizer";

    NativeNormalizerProcess(String jobId, NativeController nativeController, ProcessPipes processPipes) {
        super(jobId, nativeController, processPipes, 0, Collections.emptyList(), (ignore) -> {});
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
    public void persistState(long snapshotTimestamp, String snapshotId, String snapshotDescription) {}

    @Override
    public NormalizerResultHandler createNormalizedResultsHandler() {
        return new NormalizerResultHandler(processOutStream());
    }
}
