/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.util.Objects;

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
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
            .masterNodeTimeout(getMasterTimeout(currentState))
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(index))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(targetIndexName).alias(index));
        // copy over other aliases from original index
        indexMetadata.getAliases().values().spliterator().forEachRemaining(aliasMetadataObjectCursor -> {
            AliasMetadata aliasMetadataToAdd = aliasMetadataObjectCursor.value;
            // inherit all alias properties except `is_write_index`
            aliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                .index(targetIndexName).alias(aliasMetadataToAdd.alias())
                .indexRouting(aliasMetadataToAdd.indexRouting())
                .searchRouting(aliasMetadataToAdd.searchRouting())
                .filter(aliasMetadataToAdd.filter() == null ? null : aliasMetadataToAdd.filter().string())
                .writeIndex(null));
        });
        getClient().admin().indices().aliases(aliasesRequest, ActionListener.wrap(response ->
            listener.onResponse(true), listener::onFailure));
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
