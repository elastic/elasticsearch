/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect;

import org.elasticsearch.xpack.prelert.job.Job;

import java.util.concurrent.ExecutorService;

/**
 * Factory interface for creating implementations of {@link AutodetectProcess}
 */
public interface AutodetectProcessFactory {
    /**
     *  Create an implementation of {@link AutodetectProcess}
     *
     * @param job Job configuration for the analysis process
     * @param ignoreDowntime Should gaps in data be treated as anomalous or as a maintenance window after a job re-start
     * @param executorService Executor service used to start the async tasks a job needs to operate the analytical process
     * @return The process
     */
    AutodetectProcess createAutodetectProcess(Job job, boolean ignoreDowntime, ExecutorService executorService);
}
