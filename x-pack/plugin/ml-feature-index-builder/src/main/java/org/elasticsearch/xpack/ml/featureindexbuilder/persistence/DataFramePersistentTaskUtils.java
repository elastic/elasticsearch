/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.persistence;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.ml.featureindexbuilder.FeatureIndexBuilder;

public final class DataFramePersistentTaskUtils {

    private DataFramePersistentTaskUtils() {
    }

    /**
     * Check to see if the PersistentTask's cluster state contains the job(s) we
     * are interested in
     */
    public static boolean stateHasDataFrameJobs(String id, ClusterState state) {
        boolean hasJobs = false;
        PersistentTasksCustomMetaData pTasksMeta = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        if (pTasksMeta != null) {
            // If the request was for _all jobs, we need to look through the list of
            // persistent tasks and see if at least once has a DataFrameJob param
            if (id.equals(MetaData.ALL)) {
                hasJobs = pTasksMeta.tasks().stream()
                        .anyMatch(persistentTask -> persistentTask.getTaskName().equals(FeatureIndexBuilder.TASK_NAME));

            } else if (pTasksMeta.getTask(id) != null) {
                // If we're looking for a single job, we can just check directly
                hasJobs = true;
            }
        }
        return hasJobs;
    }
}
