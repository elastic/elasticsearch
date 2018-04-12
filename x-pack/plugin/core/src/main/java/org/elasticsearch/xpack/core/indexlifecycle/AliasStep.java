/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;

public class AliasStep extends AsyncActionStep {
    public static final String NAME = "aliases";

    public AliasStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, Listener listener) {
        // get source index
        String index = indexMetaData.getIndex().getName();
        // get target shrink index
        String targetIndexName = ShrinkStep.SHRUNKEN_INDEX_PREFIX + index;

        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(index))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(targetIndexName).alias(index));

        getClient().admin().indices().aliases(aliasesRequest, ActionListener.wrap(response ->
            listener.onResponse(true), listener::onFailure));
    }
}
