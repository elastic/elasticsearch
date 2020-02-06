/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.process.MlController;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

import java.io.IOException;
import java.util.Objects;

public class MlLifeCycleService {

    private final DatafeedManager datafeedManager;
    private final MlController mlController;
    private final AutodetectProcessManager autodetectProcessManager;
    private final MlMemoryTracker memoryTracker;

    MlLifeCycleService(ClusterService clusterService, DatafeedManager datafeedManager, MlController mlController,
                       AutodetectProcessManager autodetectProcessManager, MlMemoryTracker memoryTracker) {
        this.datafeedManager = Objects.requireNonNull(datafeedManager);
        this.mlController = Objects.requireNonNull(mlController);
        this.autodetectProcessManager = Objects.requireNonNull(autodetectProcessManager);
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
            // This prevents datafeeds from sending data to autodetect processes WITHOUT stopping the
            // datafeeds, so they get reassigned.  We have to do this first, otherwise the datafeeds
            // could fail if they send data to a dead autodetect process.
            datafeedManager.isolateAllDatafeedsOnThisNodeBeforeShutdown();
            // This kills autodetect processes WITHOUT closing the jobs, so they get reassigned.
            autodetectProcessManager.killAllProcessesOnThisNode();
            mlController.stop();
        } catch (IOException e) {
            // We're stopping anyway, so don't let this complicate the shutdown sequence
        }
        memoryTracker.stop();
    }
}
