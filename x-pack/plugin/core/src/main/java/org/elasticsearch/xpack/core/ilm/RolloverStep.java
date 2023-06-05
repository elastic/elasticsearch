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
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;

import java.util.Locale;
import java.util.Objects;

/**
 * Unconditionally rolls over an index using the Rollover API.
 */
public class RolloverStep extends AsyncActionStep {
    private static final Logger logger = LogManager.getLogger(RolloverStep.class);

    public static final String NAME = "attempt-rollover";

    public RolloverStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentClusterState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        String indexName = indexMetadata.getIndex().getName();
        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(indexMetadata.getSettings());
        if (indexingComplete) {
            logger.trace(indexMetadata.getIndex() + " has lifecycle complete set, skipping " + RolloverStep.NAME);
            listener.onResponse(null);
            return;
        }
        IndexAbstraction indexAbstraction = currentClusterState.metadata().getIndicesLookup().get(indexName);
        assert indexAbstraction != null : "expected the index " + indexName + " to exist in the lookup but it didn't";
        final String rolloverTarget;
        DataStream dataStream = indexAbstraction.getParentDataStream();
        if (dataStream != null) {
            assert dataStream.getWriteIndex() != null : "datastream " + dataStream.getName() + " has no write index";
            if (dataStream.getWriteIndex().equals(indexMetadata.getIndex()) == false) {
                logger.warn(
                    "index [{}] is not the write index for data stream [{}]. skipping rollover for policy [{}]",
                    indexName,
                    dataStream.getName(),
                    indexMetadata.getLifecyclePolicyName()
                );
                listener.onResponse(null);
                return;
            }
            rolloverTarget = dataStream.getName();
        } else {
            String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetadata.getSettings());

            if (Strings.isNullOrEmpty(rolloverAlias)) {
                listener.onFailure(
                    new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "setting [%s] for index [%s] is empty or not defined, it must be set to the name of the alias "
                                + "pointing to the group of indices being rolled over",
                            RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                            indexName
                        )
                    )
                );
                return;
            }

            if (indexMetadata.getRolloverInfos().get(rolloverAlias) != null) {
                logger.info(
                    "index [{}] was already rolled over for alias [{}], not attempting to roll over again",
                    indexName,
                    rolloverAlias
                );
                listener.onResponse(null);
                return;
            }

            if (indexMetadata.getAliases().containsKey(rolloverAlias) == false) {
                listener.onFailure(
                    new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "%s [%s] does not point to index [%s]",
                            RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                            rolloverAlias,
                            indexName
                        )
                    )
                );
                return;
            }

            rolloverTarget = rolloverAlias;
        }

        // Calling rollover with no conditions will always roll over the index
        RolloverRequest rolloverRequest = new RolloverRequest(rolloverTarget, null).masterNodeTimeout(TimeValue.MAX_VALUE);
        // We don't wait for active shards when we perform the rollover because the
        // {@link org.elasticsearch.xpack.core.ilm.WaitForActiveShardsStep} step will do so
        rolloverRequest.setWaitForActiveShards(ActiveShardCount.NONE);
        getClient().admin().indices().rolloverIndex(rolloverRequest, listener.wrapResponse((l, response) -> {
            assert response.isRolledOver() : "the only way this rollover call should fail is with an exception";
            if (response.isRolledOver()) {
                l.onResponse(null);
            } else {
                l.onFailure(new IllegalStateException("unexepected exception on unconditional rollover"));
            }
        }));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RolloverStep other = (RolloverStep) obj;
        return super.equals(obj);
    }
}
