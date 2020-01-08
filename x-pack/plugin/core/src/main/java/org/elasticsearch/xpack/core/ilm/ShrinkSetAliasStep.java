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
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;

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
    public void performDuringNoSnapshot(IndexMetaData indexMetaData, ClusterState currentState, Listener listener) {
        // get source index
        String index = indexMetaData.getIndex().getName();
        // get target shrink index
        String targetIndexName = shrunkIndexPrefix + index;
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(index))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(targetIndexName).alias(index));
        // copy over other aliases from original index
        indexMetaData.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
            AliasMetaData aliasMetaDataToAdd = aliasMetaDataObjectCursor.value;
            // inherit all alias properties except `is_write_index`
            aliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                .index(targetIndexName).alias(aliasMetaDataToAdd.alias())
                .indexRouting(aliasMetaDataToAdd.indexRouting())
                .searchRouting(aliasMetaDataToAdd.searchRouting())
                .filter(aliasMetaDataToAdd.filter() == null ? null : aliasMetaDataToAdd.filter().string())
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
