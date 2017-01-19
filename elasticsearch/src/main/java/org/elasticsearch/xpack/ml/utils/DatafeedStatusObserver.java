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
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.Datafeed;
import org.elasticsearch.xpack.ml.datafeed.DatafeedStatus;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class DatafeedStatusObserver {

    private static final Logger LOGGER = Loggers.getLogger(DatafeedStatusObserver.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public DatafeedStatusObserver(ThreadPool threadPool, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    public void waitForStatus(String datafeedId, TimeValue waitTimeout, DatafeedStatus expectedStatus, Consumer<Exception> handler) {
        ClusterStateObserver observer =
                new ClusterStateObserver(clusterService, LOGGER, threadPool.getThreadContext());
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                handler.accept(null);
            }

            @Override
            public void onClusterServiceClose() {
                Exception e = new IllegalArgumentException("Cluster service closed while waiting for datafeed status to change to ["
                        + expectedStatus + "]");
                handler.accept(new IllegalStateException(e));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                Exception e = new IllegalArgumentException("Timeout expired while waiting for datafeed status to change to ["
                        + expectedStatus + "]");
                handler.accept(e);
            }
        }, new DatafeedStoppedPredicate(datafeedId, expectedStatus), waitTimeout);
    }

    private static class DatafeedStoppedPredicate implements Predicate<ClusterState> {

        private final String datafeedId;
        private final DatafeedStatus expectedStatus;

        DatafeedStoppedPredicate(String datafeedId, DatafeedStatus expectedStatus) {
            this.datafeedId = datafeedId;
            this.expectedStatus = expectedStatus;
        }

        @Override
        public boolean test(ClusterState newState) {
            MlMetadata metadata = newState.getMetaData().custom(MlMetadata.TYPE);
            if (metadata != null) {
                Datafeed datafeed = metadata.getDatafeed(datafeedId);
                if (datafeed != null) {
                    return datafeed.getStatus() == expectedStatus;
                }
            }
            return false;
        }

    }

}
