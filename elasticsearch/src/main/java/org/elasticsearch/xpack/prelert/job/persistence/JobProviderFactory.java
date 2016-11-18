/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

/**
 * Get a {@linkplain JobProvider}
 * This may create a new JobProvider or return an existing
 * one if it is thread safe and shareable.
 */
public interface JobProviderFactory
{
    /**
     * Get a {@linkplain JobProvider}
     */
    JobProvider jobProvider();
}
