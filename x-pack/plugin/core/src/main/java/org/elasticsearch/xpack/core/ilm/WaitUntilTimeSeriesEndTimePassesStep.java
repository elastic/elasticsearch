/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.core.ilm.step.info.EmptyInfo;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.time.Instant;
import java.util.function.Supplier;

/**
 * This {@link Step} waits until the {@link org.elasticsearch.index.IndexSettings#TIME_SERIES_END_TIME} passes for time series indices.
 * For regular indices this step doesn't wait at all and the condition is evaluated to true immediately.
 * <p>
 * Note that this step doesn't execute an async/transport action and is able to evaluate its condition based on the local information
 * available however, we want this step to be executed periodically using the `AsyncWaitStep` infrastructure.
 * The condition will be evaluated every {@link LifecycleSettings#LIFECYCLE_POLL_INTERVAL}.
 */
public class WaitUntilTimeSeriesEndTimePassesStep extends AsyncWaitStep {

    public static final String NAME = "check-ts-end-time-passed";
    private final Supplier<Instant> nowSupplier;

    public WaitUntilTimeSeriesEndTimePassesStep(StepKey key, StepKey nextStepKey, Supplier<Instant> nowSupplier) {
        super(key, nextStepKey, null);
        this.nowSupplier = nowSupplier;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void evaluateCondition(Metadata metadata, Index index, Listener listener, TimeValue masterTimeout) {
        IndexMetadata indexMetadata = metadata.getProject().index(index);
        assert indexMetadata != null
            : "the index metadata for index [" + index.getName() + "] must exist in the cluster state for step [" + NAME + "]";

        if (IndexSettings.MODE.get(indexMetadata.getSettings()) != IndexMode.TIME_SERIES) {
            // this index is not a time series index so no need to wait
            listener.onResponse(true, EmptyInfo.INSTANCE);
            return;
        }
        Instant configuredEndTime = IndexSettings.TIME_SERIES_END_TIME.get(indexMetadata.getSettings());
        assert configuredEndTime != null : "a time series index must have an end time configured but [" + index.getName() + "] does not";
        if (nowSupplier.get().isBefore(configuredEndTime)) {
            listener.onResponse(
                false,
                new SingleMessageFieldInfo(
                    Strings.format(
                        "The [%s] setting for index [%s] is [%s]. Waiting until the index's time series end time lapses before"
                            + " proceeding with action [%s] as the index can still accept writes.",
                        IndexSettings.TIME_SERIES_END_TIME.getKey(),
                        index.getName(),
                        configuredEndTime.toEpochMilli(),
                        getKey().action()
                    )
                )
            );
            return;
        }

        listener.onResponse(true, EmptyInfo.INSTANCE);
    }
}
