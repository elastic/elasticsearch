/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

public class NormalizerFactory {

    private final NormalizerProcessFactory processFactory;
    private final ExecutorService executorService;

    public NormalizerFactory(NormalizerProcessFactory processFactory, ExecutorService executorService) {
        this.processFactory = Objects.requireNonNull(processFactory);
        this.executorService = Objects.requireNonNull(executorService);
    }

    public Normalizer create(String jobId) {
        return new Normalizer(jobId, processFactory, executorService);
    }
}
