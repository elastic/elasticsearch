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
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.DownsampleAction.DOWNSAMPLED_INDEX_PREFIX;

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
    private final TimeValue waitTimeout;
    private final StepKey nextStepOnSuccess;
    private final StepKey nextStepOnFailure;
    private volatile boolean downsampleFailed;

    public DownsampleStep(
        final StepKey key,
        final StepKey nextStepOnSuccess,
        final StepKey nextStepOnFailure,
        final Client client,
        final DateHistogramInterval fixedInterval,
        final TimeValue waitTimeout
    ) {
        super(key, null, client);
        this.nextStepOnSuccess = nextStepOnSuccess;
        this.nextStepOnFailure = nextStepOnFailure;
        this.fixedInterval = fixedInterval;
        this.waitTimeout = waitTimeout;
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
        final String downsampleIndexName = DOWNSAMPLED_INDEX_PREFIX + "-" + indexName + "-" + fixedInterval;

        IndexMetadata downsampleIndexMetadata = currentState.metadata().index(downsampleIndexName);
        if (downsampleIndexMetadata != null) {
            IndexMetadata.DownsampleTaskStatus downsampleIndexStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(
                downsampleIndexMetadata.getSettings()
            );
            if (IndexMetadata.DownsampleTaskStatus.SUCCESS.equals(downsampleIndexStatus)) {
                // Downsample index has already been created with the generated name and its status is "success".
                // So we skip index downsample creation.
                logger.info(
                    "skipping [{}] step for index [{}] as part of policy [{}] as the downsample index [{}] already exists",
                    DownsampleStep.NAME,
                    indexName,
                    policyName,
                    downsampleIndexName
                );
                listener.onResponse(null);
            }
        }
        performDownsampleIndex(indexName, downsampleIndexName, ActionListener.wrap(listener::onResponse, e -> {
            downsampleFailed = true;
            listener.onFailure(e);
        }));
    }

    void performDownsampleIndex(String indexName, String downsampleIndexName, ActionListener<Void> listener) {
        DownsampleConfig config = new DownsampleConfig(fixedInterval);
        DownsampleAction.Request request = new DownsampleAction.Request(indexName, downsampleIndexName, waitTimeout, config)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        // Currently, DownsampleAction always acknowledges action was complete when no exceptions are thrown.
        getClient().execute(DownsampleAction.INSTANCE, request, listener.delegateFailureAndWrap((l, response) -> l.onResponse(null)));
    }

    @Override
    public final StepKey getNextStepKey() {
        return downsampleFailed ? nextStepOnFailure : nextStepOnSuccess;
    }

    public DateHistogramInterval getFixedInterval() {
        return fixedInterval;
    }

    public TimeValue getWaitTimeout() {
        return waitTimeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fixedInterval, nextStepOnSuccess, nextStepOnFailure, waitTimeout);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DownsampleStep other = (DownsampleStep) obj;
        return super.equals(obj)
            && Objects.equals(fixedInterval, other.fixedInterval)
            && Objects.equals(nextStepOnSuccess, other.nextStepOnSuccess)
            && Objects.equals(nextStepOnFailure, other.nextStepOnFailure)
            && Objects.equals(waitTimeout, other.waitTimeout);
    }
}
