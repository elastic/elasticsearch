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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * This step swaps all the aliases from the source index to the restored index and deletes the source index. This is useful in scenarios
 * following a restore from snapshot operation where the restored index will take the place of the source index in the ILM lifecycle.
 */
public class SwapAliasesAndDeleteSourceIndexStep extends AsyncActionStep {
    public static final String NAME = "swap-aliases";
    private static final Logger logger = LogManager.getLogger(SwapAliasesAndDeleteSourceIndexStep.class);

    /**
     * Supplier function that returns the name of the target index where aliases will
     * point to
     */
    private final BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier;

    /**
     * if true, this method will create an alias named as the source index and will link it
     * to the target index
     */
    private final boolean createSourceIndexAlias;

    public SwapAliasesAndDeleteSourceIndexStep(StepKey key, StepKey nextStepKey, Client client, String targetIndexPrefix) {
        this(key, nextStepKey, client, (index, lifecycleState) -> targetIndexPrefix + index, true);
    }

    public SwapAliasesAndDeleteSourceIndexStep(
        StepKey key,
        StepKey nextStepKey,
        Client client,
        BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier,
        boolean createSourceIndexAlias
    ) {
        super(key, nextStepKey, client);
        this.targetIndexNameSupplier = targetIndexNameSupplier;
        this.createSourceIndexAlias = createSourceIndexAlias;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    BiFunction<String, LifecycleExecutionState, String> getTargetIndexNameSupplier() {
        return targetIndexNameSupplier;
    }

    boolean getCreateSourceIndexAlias() {
        return createSourceIndexAlias;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentClusterState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        String originalIndex = indexMetadata.getIndex().getName();
        final String targetIndexName = targetIndexNameSupplier.apply(originalIndex, indexMetadata.getLifecycleExecutionState());
        IndexMetadata targetIndexMetadata = currentClusterState.metadata().getProject().index(targetIndexName);

        if (targetIndexMetadata == null) {
            String policyName = indexMetadata.getLifecyclePolicyName();
            String errorMessage = Strings.format(
                "target index [%s] doesn't exist. stopping execution of lifecycle [%s] for index [%s]",
                targetIndexName,
                policyName,
                originalIndex
            );
            logger.debug(errorMessage);
            listener.onFailure(new IllegalStateException(errorMessage));
            return;
        }

        deleteSourceIndexAndTransferAliases(getClient(), indexMetadata, targetIndexName, listener, createSourceIndexAlias);
    }

    /**
     * Executes an {@link IndicesAliasesRequest} to copy over all the aliases from the source to the target index, and remove the source
     * index.
     * <p>
     * The is_write_index will *not* be set on the target index as this operation is currently executed on read-only indices.
     * @param createSourceIndexAlias if true, this method will create an alias named as the source index and will link it
     *                               to the target index
     */
    static void deleteSourceIndexAndTransferAliases(
        Client client,
        IndexMetadata sourceIndex,
        String targetIndex,
        ActionListener<Void> listener,
        boolean createSourceIndexAlias
    ) {
        String sourceIndexName = sourceIndex.getIndex().getName();
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest(TimeValue.MAX_VALUE, TimeValue.THIRTY_SECONDS).addAliasAction(
            IndicesAliasesRequest.AliasActions.removeIndex().index(sourceIndexName)
        );

        if (createSourceIndexAlias) {
            // create an alias with the same name as the source index and link it to the target index
            aliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(targetIndex).alias(sourceIndexName));
        }
        // copy over other aliases from source index
        sourceIndex.getAliases().values().forEach(aliasMetaDataToAdd -> {
            // inherit all alias properties except `is_write_index`
            aliasesRequest.addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(targetIndex)
                    .alias(aliasMetaDataToAdd.alias())
                    .indexRouting(aliasMetaDataToAdd.indexRouting())
                    .searchRouting(aliasMetaDataToAdd.searchRouting())
                    .filter(aliasMetaDataToAdd.filter() == null ? null : aliasMetaDataToAdd.filter().string())
                    .writeIndex(null)
                    .isHidden(aliasMetaDataToAdd.isHidden())
            );
        });

        client.admin().indices().aliases(aliasesRequest, listener.delegateFailureAndWrap((l, response) -> {
            if (response.isAcknowledged() == false) {
                logger.warn("aliases swap from [{}] to [{}] response was not acknowledged", sourceIndexName, targetIndex);
            }
            l.onResponse(null);
        }));
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetIndexNameSupplier, createSourceIndexAlias);
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
        return super.equals(obj)
            && Objects.equals(targetIndexNameSupplier, other.targetIndexNameSupplier)
            && createSourceIndexAlias == other.createSourceIndexAlias;
    }
}
