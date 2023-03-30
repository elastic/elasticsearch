/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.downsample.DownsampleConfig;

import java.util.Objects;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

/**
 * ILM step that invokes the downsample action for an index using a {@link DateHistogramInterval}. The downsample
 * index name is retrieved from the lifecycle state {@link LifecycleExecutionState#downsampleIndexName()}
 * index. If a downsample index with the same name has been already successfully created, this step
 * will be skipped.
 */
public class DownsampleStep extends AsyncActionStep {
    public static final String NAME = "rollup";

    private static final Logger logger = LogManager.getLogger(DownsampleStep.class);

    private final DateHistogramInterval fixedInterval;

    private final StepKey failureStep;

    public DownsampleStep(StepKey key, StepKey nextStepKey, StepKey failureStep, Client client, DateHistogramInterval fixedInterval) {
        super(key, nextStepKey, client);
        this.failureStep = failureStep;
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
        final String downsampleIndexName = lifecycleState.downsampleIndexName();
        if (Strings.hasText(downsampleIndexName) == false) {
            listener.onFailure(
                new IllegalStateException(
                    "rollup index name was not generated for policy [" + policyName + "] and index [" + indexName + "]"
                )
            );
            return;
        }

        IndexMetadata rollupIndexMetadata = currentState.metadata().index(downsampleIndexName);
        if (rollupIndexMetadata != null) {
            IndexMetadata.DownsampleTaskStatus rollupIndexStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(
                rollupIndexMetadata.getSettings()
            );
            // Rollup index has already been created with the generated name and its status is "success".
            // So we skip index rollup creation.
            if (IndexMetadata.DownsampleTaskStatus.SUCCESS.equals(rollupIndexStatus)) {
                logger.warn(
                    "skipping [{}] step for index [{}] as part of policy [{}] as the rollup index [{}] already exists",
                    DownsampleStep.NAME,
                    indexName,
                    policyName,
                    downsampleIndexName
                );
                listener.onResponse(null);
            } else {
                logger.warn(
                    "[{}] step for index [{}] as part of policy [{}] found the rollup index [{}] already exists. Deleting it.",
                    DownsampleStep.NAME,
                    indexName,
                    policyName,
                    downsampleIndexName
                );

                // On failure we must rewind ILM so that it executes GenerateUniqueIndexNameStep again
                // so that a new downsample index will be created. On the other hand, this means that
                // garbage indices may be left from this process. We will try to cleanup here, but it
                // is not guaranteed 100%
                if (failureStep != null) {
                    LifecycleExecutionState.Builder newLifecycleState = LifecycleExecutionState.builder(lifecycleState);
                    newLifecycleState.setStep(failureStep.name());
                    // newLifecycleState.setFailedStep(failureStep.name());

                    IndexMetadata.builder(indexMetadata).putCustom(ILM_CUSTOM_METADATA_KEY, newLifecycleState.build().asMap()).build();
                }

                // Rollup index has already been created with the generated name but its status is not "success".
                // So we delete the index and proceed with executing the rollup step.
                DeleteIndexRequest deleteRequest = new DeleteIndexRequest(downsampleIndexName);
                getClient().admin().indices().delete(deleteRequest, ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        performDownsampleIndex(indexName, downsampleIndexName, listener);
                    } else {
                        listener.onFailure(
                            new IllegalStateException(
                                "failing ["
                                    + DownsampleStep.NAME
                                    + "] step for index ["
                                    + indexName
                                    + "] as part of policy ["
                                    + policyName
                                    + "] because the rollup index ["
                                    + downsampleIndexName
                                    + "] already exists with rollup status ["
                                    + rollupIndexStatus
                                    + "]"
                            )
                        );
                    }
                }, listener::onFailure));
            }
        } else {
            performDownsampleIndex(indexName, downsampleIndexName, listener);
        }
    }

    private void performDownsampleIndex(String indexName, String rollupIndexName, ActionListener<Void> listener) {
        DownsampleConfig config = new DownsampleConfig(fixedInterval);
        DownsampleAction.Request request = new DownsampleAction.Request(indexName, rollupIndexName, config).masterNodeTimeout(
            TimeValue.MAX_VALUE
        );
        // Currently, DownsampleAction always acknowledges action was complete when no exceptions are thrown.
        getClient().execute(
            DownsampleAction.INSTANCE,
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
        DownsampleStep other = (DownsampleStep) obj;
        return super.equals(obj) && Objects.equals(fixedInterval, other.fixedInterval);
    }
}
