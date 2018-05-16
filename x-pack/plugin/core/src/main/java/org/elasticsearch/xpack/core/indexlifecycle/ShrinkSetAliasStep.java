/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Objects;

public class ShrinkSetAliasStep extends AsyncActionStep {
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
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, Listener listener) {
        // get source index
        String index = indexMetaData.getIndex().getName();
        // get target shrink index
        String targetIndexName = shrunkIndexPrefix + index;

        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(index))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(targetIndexName).alias(index));

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
