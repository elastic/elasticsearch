/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;

import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.getShrinkIndexName;
import static org.elasticsearch.xpack.core.ilm.SwapAliasesAndDeleteSourceIndexStep.deleteSourceIndexAndTransferAliases;

/**
 * Following shrinking an index and deleting the original index, this step creates an alias with the same name as the original index which
 * points to the new shrunken index to allow clients to continue to use the original index name without being aware that it has shrunk.
 */
public class ShrinkSetAliasStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "aliases";

    public ShrinkSetAliasStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentState, ActionListener<Void> listener) {
        // get source index
        String indexName = indexMetadata.getIndex().getName();
        // get target shrink index
        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        String targetIndexName = getShrinkIndexName(indexName, lifecycleState);
        deleteSourceIndexAndTransferAliases(getClient(), indexMetadata, targetIndexName, listener);
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }

}
