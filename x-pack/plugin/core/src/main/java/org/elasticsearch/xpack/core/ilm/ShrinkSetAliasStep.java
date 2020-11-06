/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.SwapAliasesAndDeleteSourceIndexStep.deleteSourceIndexAndTransferAliases;

/**
 * Following shrinking an index and deleting the original index, this step creates an alias with the same name as the original index which
 * points to the new shrunken index to allow clients to continue to use the original index name without being aware that it has shrunk.
 */
public class ShrinkSetAliasStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "aliases";
    private String shrunkIndexPrefix;

    public ShrinkSetAliasStep(StepKey key, StepKey nextStepKey, Client client, String shrunkIndexPrefix) {
        super(key, nextStepKey, client);
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
    }

    @Override
    public void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentState, Listener listener) {
        // get source index
        String index = indexMetadata.getIndex().getName();
        // get target shrink index
        String targetIndexName = shrunkIndexPrefix + index;
        deleteSourceIndexAndTransferAliases(getClient(), indexMetadata, getMasterTimeout(currentState), targetIndexName, listener);
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shrunkIndexPrefix);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ShrinkSetAliasStep other = (ShrinkSetAliasStep) obj;
        return super.equals(obj) &&
                Objects.equals(shrunkIndexPrefix, other.shrunkIndexPrefix);
    }
}
