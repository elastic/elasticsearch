/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;

/**
 * Deletes the index identified by the clone index name stored in the lifecycle state of the managed index (if any was generated)
 * TODO: should we just generalize CleanupShrinkIndexStep to CleanupIndexStep and use it for both shrink and clone?
 */
public class CleanupClonedIndexStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "cleanup-cloned-index";
    private static final Logger logger = LogManager.getLogger(CleanupClonedIndexStep.class);

    public CleanupClonedIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ProjectMetadata currentProject, ActionListener<Void> listener) {
        final String clonedIndexSource = IndexMetadata.INDEX_RESIZE_SOURCE_NAME.get(indexMetadata.getSettings());
        if (Strings.isNullOrEmpty(clonedIndexSource) == false) {
            // the current managed index is a cloned index
            if (currentProject.index(clonedIndexSource) == null) {
                // if the source index does not exist, we'll skip deleting the
                // (managed) cloned index as that will cause data loss
                String policyName = indexMetadata.getLifecyclePolicyName();
                logger.warn(
                    "managed index [{}] as part of policy [{}] is a cloned index and the source index [{}] does not exist "
                        + "anymore. will skip the [{}] step",
                    indexMetadata.getIndex().getName(),
                    policyName,
                    clonedIndexSource,
                    NAME
                );
                listener.onResponse(null);
                return;
            }
        }

        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        final String cloneIndexName = lifecycleState.forceMergeIndexName();
        // if the clone index was not generated there is nothing to delete so we move on
        if (Strings.hasText(cloneIndexName) == false) {
            listener.onResponse(null);
            return;
        }
        getClient(currentProject.id()).admin()
            .indices()
            .delete(new DeleteIndexRequest(cloneIndexName).masterNodeTimeout(TimeValue.MAX_VALUE), new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    // even if not all nodes acked the delete request yet we can consider this operation as successful as
                    // we'll generate a new index name and attempt to clone into the newly generated name
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IndexNotFoundException) {
                        // we can move on if the index was deleted in the meantime
                        listener.onResponse(null);
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
    }

}
