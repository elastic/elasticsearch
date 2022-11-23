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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Objects;
import java.util.function.Function;

/**
 * Deletes the target index created by an operation such as shrink or rollup and
 * identified the target index name stored in the lifecycle state of the managed
 * index (if any was generated)
 */
public class CleanupTargetIndexStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "cleanup-target-index";
    private static final Logger logger = LogManager.getLogger(CleanupTargetIndexStep.class);

    private final Function<IndexMetadata, String> sourceIndexNameSupplier;
    private final Function<IndexMetadata, String> targetIndexNameSupplier;

    public CleanupTargetIndexStep(
        StepKey key,
        StepKey nextStepKey,
        Client client,
        Function<IndexMetadata, String> sourceIndexNameSupplier,
        Function<IndexMetadata, String> targetIndexNameSupplier
    ) {
        super(key, nextStepKey, client);
        this.sourceIndexNameSupplier = sourceIndexNameSupplier;
        this.targetIndexNameSupplier = targetIndexNameSupplier;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    Function<IndexMetadata, String> getSourceIndexNameSupplier() {
        return sourceIndexNameSupplier;
    }

    Function<IndexMetadata, String> getTargetIndexNameSupplier() {
        return targetIndexNameSupplier;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Void> listener) {
        final String sourceIndexName = sourceIndexNameSupplier.apply(indexMetadata);
        if (Strings.isNullOrEmpty(sourceIndexName) == false) {
            // the current managed index is the target index
            if (currentClusterState.metadata().index(sourceIndexName) == null) {
                // if the source index does not exist, we'll skip deleting the
                // (managed) target index as that will cause data loss
                String policyName = indexMetadata.getLifecyclePolicyName();
                logger.warn(
                    "managed index [{}] has been created as part of policy [{}] and the source index [{}] does not exist "
                        + "anymore. will skip the [{}] step",
                    indexMetadata.getIndex().getName(),
                    policyName,
                    sourceIndexName,
                    NAME
                );
                listener.onResponse(null);
                return;
            }
        }

        final String targetIndexName = targetIndexNameSupplier.apply(indexMetadata);
        // if the target index was not generated there is nothing to delete so we move on
        if (Strings.hasText(targetIndexName) == false) {
            listener.onResponse(null);
            return;
        }
        getClient().admin()
            .indices()
            .delete(new DeleteIndexRequest(targetIndexName).masterNodeTimeout(TimeValue.MAX_VALUE), new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    // even if not all nodes acked the delete request yet we can consider this operation as successful as
                    // we'll generate a new index name and attempt to create an index with the newly generated name
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CleanupTargetIndexStep that = (CleanupTargetIndexStep) o;
        return super.equals(o)
            && Objects.equals(targetIndexNameSupplier, that.targetIndexNameSupplier)
            && Objects.equals(sourceIndexNameSupplier, that.sourceIndexNameSupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetIndexNameSupplier, sourceIndexNameSupplier);
    }
}
