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
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;

import java.util.Locale;
import java.util.Objects;

/**
 * This step swaps all the aliases from the source index to the restored index and deletes the source index. This is useful in scenarios
 * following a restore from snapshot operation where the restored index will take the place of the source index in the ILM lifecycle.
 */
public class SwapAliasesAndDeleteSourceIndexStep extends AsyncActionStep {
    public static final String NAME = "swap-aliases";
    private static final Logger logger = LogManager.getLogger(SwapAliasesAndDeleteSourceIndexStep.class);

    private final String targetIndexPrefix;

    public SwapAliasesAndDeleteSourceIndexStep(StepKey key, StepKey nextStepKey, Client client, String targetIndexPrefix) {
        super(key, nextStepKey, client);
        this.targetIndexPrefix = targetIndexPrefix;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public String getTargetIndexPrefix() {
        return targetIndexPrefix;
    }

    @Override
    public void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState, ClusterStateObserver observer,
                              ActionListener<Boolean> listener) {
        String originalIndex = indexMetadata.getIndex().getName();
        final String targetIndexName = targetIndexPrefix + originalIndex;
        IndexMetadata targetIndexMetadata = currentClusterState.metadata().index(targetIndexName);

        if (targetIndexMetadata == null) {
            String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
            String errorMessage = String.format(Locale.ROOT, "target index [%s] doesn't exist. stopping execution of lifecycle [%s] for" +
                " index [%s]", targetIndexName, policyName, originalIndex);
            logger.debug(errorMessage);
            listener.onFailure(new IllegalStateException(errorMessage));
            return;
        }

        deleteSourceIndexAndTransferAliases(getClient(), indexMetadata, targetIndexName, listener);
    }

    /**
     * Executes an {@link IndicesAliasesRequest} to copy over all the aliases from the source to the target index, and remove the source
     * index.
     * <p>
     * The is_write_index will *not* be set on the target index as this operation is currently executed on read-only indices.
     */
    static void deleteSourceIndexAndTransferAliases(Client client, IndexMetadata sourceIndex, String targetIndex,
                                                    ActionListener<Boolean> listener) {
        String sourceIndexName = sourceIndex.getIndex().getName();
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
            .masterNodeTimeout(TimeValue.MAX_VALUE)
            .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(sourceIndexName))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(targetIndex).alias(sourceIndexName));
        // copy over other aliases from source index
        sourceIndex.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
            AliasMetadata aliasMetaDataToAdd = aliasMetaDataObjectCursor.value;
            // inherit all alias properties except `is_write_index`
            aliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                .index(targetIndex).alias(aliasMetaDataToAdd.alias())
                .indexRouting(aliasMetaDataToAdd.indexRouting())
                .searchRouting(aliasMetaDataToAdd.searchRouting())
                .filter(aliasMetaDataToAdd.filter() == null ? null : aliasMetaDataToAdd.filter().string())
                .writeIndex(null));
        });

        client.admin().indices().aliases(aliasesRequest,
            ActionListener.wrap(response -> {
                if (response.isAcknowledged() == false) {
                    logger.warn("aliases swap from [{}] to [{}] response was not acknowledged", sourceIndexName, targetIndex);
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
        return Objects.hash(super.hashCode(), targetIndexPrefix);
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
            Objects.equals(targetIndexPrefix, other.targetIndexPrefix);
    }
}
