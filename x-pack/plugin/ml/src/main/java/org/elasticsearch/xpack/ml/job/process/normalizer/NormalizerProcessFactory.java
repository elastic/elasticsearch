/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import java.util.concurrent.ExecutorService;

/**
 * Factory interface for creating implementations of {@link NormalizerProcess}
 */
public interface NormalizerProcessFactory {
    /**
     *  Create an implementation of {@link NormalizerProcess}
     *
     * @param executorService Executor service used to start the async tasks a job needs to operate the analytical process
     * @return The process
     */
    NormalizerProcess createNormalizerProcess(String jobId, String quantilesState, Integer bucketSpan, ExecutorService executorService);
}
