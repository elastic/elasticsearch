/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.exception.ElasticsearchException;

public class JobManagerHolder {

    private final JobManager instance;

    /**
     * Create an empty holder which also means that no job manager gets created.
     */
    public JobManagerHolder() {
        this.instance = null;
    }

    /**
     * Create a holder that allows lazy creation of a job manager.
     *
     */
    public JobManagerHolder(JobManager jobManager) {
        this.instance = jobManager;
    }

    public boolean isEmpty() {
        return instance == null;
    }

    /**
     * Get the instance of the held JobManager.
     *
     * @return job manager instance
     * @throws ElasticsearchException if holder has been created with the empty constructor
     */
    public JobManager getJobManager() {
        if (instance == null) {
            throw new ElasticsearchException("Tried to get job manager although Machine Learning is disabled");
        }
        return instance;
    }
}
