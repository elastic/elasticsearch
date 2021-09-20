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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Deletes the index identified by the shrink index name stored in the lifecycle state of the managed index (if any was generated)
 */
public class CleanupShrinkIndexStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "cleanup-shrink-index";
    private static final Logger logger = LogManager.getLogger(CleanupShrinkIndexStep.class);

    public CleanupShrinkIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Void> listener) {
        final String shrunkenIndexSource = IndexMetadata.INDEX_RESIZE_SOURCE_NAME.get(indexMetadata.getSettings());
        if (Strings.isNullOrEmpty(shrunkenIndexSource) == false) {
            // the current managed index is a shrunk index
            if (currentClusterState.metadata().index(shrunkenIndexSource) == null) {
                // if the source index does not exist, we'll skip deleting the
                // (managed) shrunk index as that will cause data loss
                String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings());
                logger.warn("managed index [{}] as part of policy [{}] is a shrunk index and the source index [{}] does not exist " +
                    "anymore. will skip the [{}] step", indexMetadata.getIndex().getName(), policyName, shrunkenIndexSource, NAME);
                listener.onResponse(null);
                return;
            }
        }

        LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetadata);
        final String shrinkIndexName = lifecycleState.getShrinkIndexName();
        // if the shrink index was not generated there is nothing to delete so we move on
        if (Strings.hasText(shrinkIndexName) == false) {
            listener.onResponse(null);
            return;
        }
        getClient().admin().indices()
            .delete(new DeleteIndexRequest(shrinkIndexName).masterNodeTimeout(TimeValue.MAX_VALUE),
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        // even if not all nodes acked the delete request yet we can consider this operation as successful as
                        // we'll generate a new index name and attempt to shrink into the newly generated name
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
