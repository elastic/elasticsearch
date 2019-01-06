/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.tasks.TaskId;

/**
 * Interface for all upgrade checks.
 */
public interface UpgradeCheck {

    /**
     * Returns the name of the check
     */
    public String getName();

    /**
     * This method is called by Upgrade API to verify if upgrade or reindex for this index is required
     *
     * @param indexMetaData index metadata
     * @return required action or UpgradeActionRequired.NOT_APPLICABLE if this check cannot be performed on the index
     */
    public UpgradeActionRequired actionRequired(IndexMetaData indexMetaData);

    /**
     * Perform the index upgrade
     *
     * @param task          the task that executes the upgrade operation
     * @param indexMetaData index metadata
     * @param state         current cluster state
     * @param listener      the listener that should be called upon completion of the upgrade
     */
    public void upgrade(TaskId task, IndexMetaData indexMetaData, ClusterState state, ActionListener<BulkByScrollResponse> listener);
}
