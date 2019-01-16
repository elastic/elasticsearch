/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

public final class DataFramePersistentTaskUtils {

    private DataFramePersistentTaskUtils() {
    }

    /**
     * Check to see if the PersistentTask's cluster state contains the data frame transform(s) we
     * are interested in
     */
    public static boolean stateHasDataFrameTransforms(String id, ClusterState state) {
        boolean hasTransforms = false;
        PersistentTasksCustomMetaData pTasksMeta = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        if (pTasksMeta != null) {
            // If the request was for _all transforms, we need to look through the list of
            // persistent tasks and see if at least one is a data frame task
            if (id.equals(MetaData.ALL)) {
                hasTransforms = pTasksMeta.tasks().stream()
                        .anyMatch(persistentTask -> persistentTask.getTaskName().equals(DataFrameField.TASK_NAME));

            } else if (pTasksMeta.getTask(id) != null) {
                // If we're looking for a single transform, we can just check directly
                hasTransforms = true;
            }
        }
        return hasTransforms;
    }
}
