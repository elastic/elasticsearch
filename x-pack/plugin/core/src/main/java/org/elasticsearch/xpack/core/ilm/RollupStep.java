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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;

import java.util.Objects;

/**
 * ILM step that invokes the rollup action for an index using a {@link DateHistogramInterval}. The rollup
 * index name is retrieved from the lifecycle state {@link LifecycleExecutionState#rollupIndexName()}
 * index. If a rollup index with the same name has been already successfully created, this step
 * will be skipped.
 */
public class RollupStep extends AsyncActionStep {
    public static final String NAME = "rollup";

    private static final Logger logger = LogManager.getLogger(RollupStep.class);

    private final DateHistogramInterval fixedInterval;

    public RollupStep(StepKey key, StepKey nextStepKey, Client client, DateHistogramInterval fixedInterval) {
        super(key, nextStepKey, client);
        this.fixedInterval = fixedInterval;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        if (lifecycleState.lifecycleDate() == null) {
            throw new IllegalStateException("source index [" + indexMetadata.getIndex().getName() + "] is missing lifecycle date");
        }

        final String policyName = indexMetadata.getLifecyclePolicyName();
        final String indexName = indexMetadata.getIndex().getName();
        final String rollupIndexName = lifecycleState.rollupIndexName();
        if (Strings.hasText(rollupIndexName) == false) {
            listener.onFailure(
                new IllegalStateException(
                    "rollup index name was not generated for policy [" + policyName + "] and index [" + indexName + "]"
                )
            );
            return;
        }

        IndexMetadata rollupIndexMetadata = currentState.metadata().index(rollupIndexName);
        if (rollupIndexMetadata != null) {
            IndexMetadata.RollupTaskStatus rollupIndexStatus = IndexMetadata.INDEX_ROLLUP_STATUS.get(rollupIndexMetadata.getSettings());
            // Rollup index has already been created with the generated name and its status is "success".
            // So we skip index rollup creation.
            if (IndexMetadata.RollupTaskStatus.SUCCESS.equals(rollupIndexStatus)) {
                logger.warn(
                    "skipping [{}] step for index [{}] as part of policy [{}] as the rollup index [{}] already exists",
                    RollupStep.NAME,
                    indexName,
                    policyName,
                    rollupIndexName
                );
                listener.onResponse(null);
            } else {
                logger.warn(
                    "[{}] step for index [{}] as part of policy [{}] found the rollup index [{}] already exists. Deleting it.",
                    RollupStep.NAME,
                    indexName,
                    policyName,
                    rollupIndexName
                );
                // Rollup index has already been created with the generated name but its status is not "success".
                // So we delete the index and proceed with executing the rollup step.
                DeleteIndexRequest deleteRequest = new DeleteIndexRequest(rollupIndexName);
                getClient().admin().indices().delete(deleteRequest, ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        performRollupIndex(indexName, rollupIndexName, listener);
                    } else {
                        listener.onFailure(
                            new IllegalStateException(
                                "failing ["
                                    + RollupStep.NAME
                                    + "] step for index ["
                                    + indexName
                                    + "] as part of policy ["
                                    + policyName
                                    + "] because the rollup index ["
                                    + rollupIndexName
                                    + "] already exists with rollup status ["
                                    + rollupIndexStatus
                                    + "]"
                            )
                        );
                    }
                }, listener::onFailure));
            }
            return;
        }

        performRollupIndex(indexName, rollupIndexName, listener);
    }

    private void performRollupIndex(String indexName, String rollupIndexName, ActionListener<Void> listener) {
        RollupActionConfig config = new RollupActionConfig(fixedInterval);
        RollupAction.Request request = new RollupAction.Request(indexName, rollupIndexName, config).masterNodeTimeout(TimeValue.MAX_VALUE);
        // Currently, RollupAction always acknowledges action was complete when no exceptions are thrown.
        getClient().execute(
            RollupAction.INSTANCE,
            request,
            ActionListener.wrap(response -> listener.onResponse(null), listener::onFailure)
        );
    }

    public DateHistogramInterval getFixedInterval() {
        return fixedInterval;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fixedInterval);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RollupStep other = (RollupStep) obj;
        return super.equals(obj) && Objects.equals(fixedInterval, other.fixedInterval);
    }
}
