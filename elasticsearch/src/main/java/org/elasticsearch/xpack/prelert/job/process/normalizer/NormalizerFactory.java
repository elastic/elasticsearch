/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.normalizer;


/**
 * Factory interface for creating implementations of {@link Normalizer}
 */
public interface NormalizerFactory {
    /**
     *  Create an implementation of {@link Normalizer}
     *
     * @param jobId The job ID
     * @return The normalizer
     */
    Normalizer create(String jobId);
}
