/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.process.MlController;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.io.IOException;
import java.util.Objects;

public class MlLifeCycleService {

    private final DatafeedRunner datafeedRunner;
    private final MlController mlController;
    private final AutodetectProcessManager autodetectProcessManager;
    private final DataFrameAnalyticsManager analyticsManager;
    private final MlMemoryTracker memoryTracker;

    MlLifeCycleService(ClusterService clusterService, DatafeedRunner datafeedRunner, MlController mlController,
                       AutodetectProcessManager autodetectProcessManager, DataFrameAnalyticsManager analyticsManager,
                       MlMemoryTracker memoryTracker) {
        this.datafeedRunner = Objects.requireNonNull(datafeedRunner);
        this.mlController = Objects.requireNonNull(mlController);
        this.autodetectProcessManager = Objects.requireNonNull(autodetectProcessManager);
        this.analyticsManager = Objects.requireNonNull(analyticsManager);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop();
            }
        });
    }

    public synchronized void stop() {
        try {
            // This prevents data frame analytics from being marked as failed due to exceptions occurring while the node is shutting down.
            analyticsManager.markNodeAsShuttingDown();
            // This prevents datafeeds from sending data to autodetect processes WITHOUT stopping the datafeeds, so they get reassigned.
            // We have to do this first, otherwise the datafeeds could fail if they send data to a dead autodetect process.
            datafeedRunner.isolateAllDatafeedsOnThisNodeBeforeShutdown();
            // This kills autodetect processes WITHOUT closing the jobs, so they get reassigned.
            autodetectProcessManager.killAllProcessesOnThisNode();
            mlController.stop();
        } catch (IOException e) {
            // We're stopping anyway, so don't let this complicate the shutdown sequence
        }
        memoryTracker.stop();
    }
}
