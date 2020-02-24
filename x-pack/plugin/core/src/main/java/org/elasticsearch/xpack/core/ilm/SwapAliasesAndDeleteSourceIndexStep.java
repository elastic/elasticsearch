/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Locale;

/**
 * Following a restore from snapshot operation this swaps all the aliases from the source index to the restored index and delete the
 * source index.
 */
public class SwapAliasesAndDeleteSourceIndexStep extends AsyncActionStep {
    public static final String NAME = "swap-aliases-to-restored";
    private static final Logger logger = LogManager.getLogger(SwapAliasesAndDeleteSourceIndexStep.class);

    private final String restoredIndexPrefix;

    public SwapAliasesAndDeleteSourceIndexStep(StepKey key, StepKey nextStepKey, Client client, String restoredIndexPrefix) {
        super(key, nextStepKey, client);
        this.restoredIndexPrefix = restoredIndexPrefix;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState, ClusterStateObserver observer,
                              Listener listener) {
        String originalIndex = indexMetaData.getIndex().getName();
        final String restoredIndexName = restoredIndexPrefix + originalIndex;
        IndexMetaData restoredIndexMetaData = currentClusterState.metaData().index(restoredIndexName);

        if (restoredIndexMetaData == null) {
            String policyName = indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
            String errorMessage = String.format(Locale.ROOT, "restored index [%s] doesn't exist. stopping execution of lifecycle [%s] for" +
                " index [%s]", restoredIndexName, policyName, originalIndex);
            logger.debug(errorMessage);
            listener.onFailure(new IllegalStateException(errorMessage));
            return;
        }

        // TODO extract and reuse - also see {@link org.elasticsearch.xpack.core.ilm.ShrinkSetAliasStep}
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
            .masterNodeTimeout(getMasterTimeout(currentClusterState))
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(originalIndex))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(restoredIndexName).alias(originalIndex));
        indexMetaData.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
            AliasMetaData aliasMetaDataToAdd = aliasMetaDataObjectCursor.value;
            // inherit all alias properties except `is_write_index`
            aliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                .index(restoredIndexName).alias(aliasMetaDataToAdd.alias())
                .indexRouting(aliasMetaDataToAdd.indexRouting())
                .searchRouting(aliasMetaDataToAdd.searchRouting())
                .filter(aliasMetaDataToAdd.filter() == null ? null : aliasMetaDataToAdd.filter().string())
                .writeIndex(aliasMetaDataToAdd.writeIndex()));
        });

        getClient().admin().indices().aliases(aliasesRequest, new ActionListener<>() {

            @Override
            public void onResponse(AcknowledgedResponse response) {
                listener.onResponse(true);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }
}
