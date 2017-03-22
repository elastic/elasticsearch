/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class DatafeedStateObserver {

    private static final Logger LOGGER = Loggers.getLogger(DatafeedStateObserver.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public DatafeedStateObserver(ThreadPool threadPool, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    public void waitForState(String datafeedId, TimeValue waitTimeout, DatafeedState expectedState, Consumer<Exception> handler) {
        ClusterStateObserver observer =
                new ClusterStateObserver(clusterService, LOGGER, threadPool.getThreadContext());
        Predicate<ClusterState> predicate = (newState) -> {
            PersistentTasksCustomMetaData tasks = newState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            DatafeedState datafeedState = MlMetadata.getDatafeedState(datafeedId, tasks);
            return datafeedState == expectedState;
        };
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                handler.accept(null);
            }

            @Override
            public void onClusterServiceClose() {
                Exception e = new IllegalArgumentException("Cluster service closed while waiting for datafeed state to change to ["
                        + expectedState + "]");
                handler.accept(new IllegalStateException(e));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                if (predicate.test(clusterService.state())) {
                    handler.accept(null);
                } else {
                    Exception e = new IllegalArgumentException("Timeout expired while waiting for datafeed state to change to ["
                            + expectedState + "]");
                    handler.accept(e);
                }
            }
        }, predicate, waitTimeout);
    }

}
