/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.scheduler.Scheduler;

import java.util.function.Consumer;

public class SchedulerStatusObserver {

    private static final Logger LOGGER = Loggers.getLogger(SchedulerStatusObserver.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public SchedulerStatusObserver(ThreadPool threadPool, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }


    public void waitForStatus(String schedulerId, TimeValue waitTimeout, SchedulerStatus expectedStatus, Consumer<Exception> handler) {
        ClusterStateObserver observer =
                new ClusterStateObserver(clusterService, LOGGER, threadPool.getThreadContext());
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                handler.accept(null);
            }

            @Override
            public void onClusterServiceClose() {
                Exception e = new IllegalArgumentException("Cluster service closed while waiting for scheduler status to change to ["
                        + expectedStatus + "]");
                handler.accept(new IllegalStateException(e));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                Exception e = new IllegalArgumentException("Timeout expired while waiting for scheduler status to change to ["
                        + expectedStatus + "]");
                handler.accept(e);
            }
        }, new SchedulerStoppedPredicate(schedulerId, expectedStatus), waitTimeout);
    }

    private static class SchedulerStoppedPredicate implements ClusterStateObserver.ChangePredicate {

        private final String schedulerId;
        private final SchedulerStatus expectedStatus;

        SchedulerStoppedPredicate(String schedulerId, SchedulerStatus expectedStatus) {
            this.schedulerId = schedulerId;
            this.expectedStatus = expectedStatus;
        }

        @Override
        public boolean apply(ClusterState previousState, ClusterState.ClusterStateStatus previousStatus, ClusterState newState,
                             ClusterState.ClusterStateStatus newStatus) {
            return apply(newState);
        }

        @Override
        public boolean apply(ClusterChangedEvent changedEvent) {
            return apply(changedEvent.state());
        }

        boolean apply(ClusterState newState) {
            PrelertMetadata metadata = newState.getMetaData().custom(PrelertMetadata.TYPE);
            if (metadata != null) {
                Scheduler scheduler = metadata.getScheduler(schedulerId);
                if (scheduler != null) {
                    return scheduler.getStatus() == expectedStatus;
                }
            }
            return false;
        }
    }

}
