/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;
import java.util.Objects;

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

    public String getRestoredIndexPrefix() {
        return restoredIndexPrefix;
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

        deleteSourceIndexAndTransferAliases(getClient(), indexMetaData, getMasterTimeout(currentClusterState), restoredIndexName, listener);
    }

    /**
     * Executes an {@link IndicesAliasesRequest} to copy over all the aliases from the source to the target index, and remove the source
     * index.
     * <p>
     * The is_write_index will *not* be set on the target index as this operation is currently executed on read-only indices.
     */
    static void deleteSourceIndexAndTransferAliases(Client client, IndexMetaData sourceIndex, TimeValue masterTimeoutValue,
                                                    String targetIndex, Listener listener) {
        String originalIndex = sourceIndex.getIndex().getName();
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
            .masterNodeTimeout(masterTimeoutValue)
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(originalIndex))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(targetIndex).alias(originalIndex));
        // copy over other aliases from source index
        sourceIndex.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
            AliasMetaData aliasMetaDataToAdd = aliasMetaDataObjectCursor.value;
            // inherit all alias properties except `is_write_index`
            aliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                .index(targetIndex).alias(aliasMetaDataToAdd.alias())
                .indexRouting(aliasMetaDataToAdd.indexRouting())
                .searchRouting(aliasMetaDataToAdd.searchRouting())
                .filter(aliasMetaDataToAdd.filter() == null ? null : aliasMetaDataToAdd.filter().string())
                .writeIndex(null));
        });

        client.admin().indices().aliases(aliasesRequest,
            ActionListener.wrap(openIndexResponse -> {
                if (openIndexResponse.isAcknowledged() == false) {
                    throw new ElasticsearchException("alias update request failed to be acknowledged");
                }
                listener.onResponse(true);
            }, listener::onFailure));
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), restoredIndexPrefix);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SwapAliasesAndDeleteSourceIndexStep other = (SwapAliasesAndDeleteSourceIndexStep) obj;
        return super.equals(obj) &&
            Objects.equals(restoredIndexPrefix, other.restoredIndexPrefix);
    }
}
