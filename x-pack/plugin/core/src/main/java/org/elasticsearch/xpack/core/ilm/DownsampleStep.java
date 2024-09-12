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
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.Objects;

/**
 * ILM step that invokes the downsample action for an index using a {@link DateHistogramInterval}. The downsample
 * index name is retrieved from the lifecycle state {@link LifecycleExecutionState#downsampleIndexName()}
 * index. If a downsample index with the same name has been already successfully created, this step
 * will be skipped.
 */
public class DownsampleStep extends AsyncActionStep {
    public static final String NAME = "rollup";
    private static final Logger LOGGER = LogManager.getLogger(DownsampleStep.class);

    private final DateHistogramInterval fixedInterval;
    private final TimeValue waitTimeout;

    public DownsampleStep(
        final StepKey key,
        final StepKey nextStepKey,
        final Client client,
        final DateHistogramInterval fixedInterval,
        final TimeValue waitTimeout
    ) {
        super(key, nextStepKey, client);
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
        final String downsampleIndexName = lifecycleState.downsampleIndexName();
        IndexMetadata downsampleIndexMetadata = currentState.metadata().getProject().index(downsampleIndexName);
        if (downsampleIndexMetadata != null) {
            IndexMetadata.DownsampleTaskStatus downsampleIndexStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(
                downsampleIndexMetadata.getSettings()
            );
            if (IndexMetadata.DownsampleTaskStatus.SUCCESS.equals(downsampleIndexStatus)) {
                // Downsample index has already been created with the generated name and its status is "success".
                // So we skip index downsample creation.
                LOGGER.info(
                    "skipping [{}] step for index [{}] as part of policy [{}] as the downsample index [{}] already exists",
                    DownsampleStep.NAME,
                    indexName,
                    policyName,
                    downsampleIndexName
                );
                listener.onResponse(null);
                return;
            }
        }
        performDownsampleIndex(indexName, downsampleIndexName, listener.delegateFailureAndWrap((l, r) -> l.onResponse(r)));
    }

    void performDownsampleIndex(String indexName, String downsampleIndexName, ActionListener<Void> listener) {
        DownsampleConfig config = new DownsampleConfig(fixedInterval);
        DownsampleAction.Request request = new DownsampleAction.Request(
            TimeValue.MAX_VALUE,
            indexName,
            downsampleIndexName,
            waitTimeout,
            config
        );
        // Currently, DownsampleAction always acknowledges action was complete when no exceptions are thrown.
        getClient().execute(DownsampleAction.INSTANCE, request, listener.delegateFailureAndWrap((l, response) -> l.onResponse(null)));
    }

    public DateHistogramInterval getFixedInterval() {
        return fixedInterval;
    }

    public TimeValue getWaitTimeout() {
        return waitTimeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fixedInterval, waitTimeout);
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
        return super.equals(obj) && Objects.equals(fixedInterval, other.fixedInterval) && Objects.equals(waitTimeout, other.waitTimeout);
    }

}
