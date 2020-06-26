/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotInProgressException;

import java.util.function.Consumer;

/**
 * This is an abstract AsyncActionStep that wraps the performed action listener, checking to see
 * if the action fails due to a snapshot being in progress. If a snapshot is in progress, it
 * registers an observer and waits to try again when a snapshot is no longer running.
 */
public abstract class AsyncRetryDuringSnapshotActionStep extends AsyncActionStep {
    private final Logger logger = LogManager.getLogger(AsyncRetryDuringSnapshotActionStep.class);

    public AsyncRetryDuringSnapshotActionStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public final void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState,
                                    ClusterStateObserver observer, Listener listener) {
        // Wrap the original listener to handle exceptions caused by ongoing snapshots
        SnapshotExceptionListener snapshotExceptionListener = new SnapshotExceptionListener(indexMetadata.getIndex(), listener, observer);
        performDuringNoSnapshot(indexMetadata, currentClusterState, snapshotExceptionListener);
    }

    /**
     * Method to be performed during which no snapshots for the index are already underway.
     */
    abstract void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, Listener listener);

    /**
     * SnapshotExceptionListener is an injected listener wrapper that checks to see if a particular
     * action failed due to a {@code SnapshotInProgressException}. If it did, then it registers a
     * ClusterStateObserver listener waiting for the next time the snapshot is not running,
     * re-running the step's {@link #performAction(IndexMetadata, ClusterState, ClusterStateObserver, Listener)}
     * method when the snapshot is no longer running.
     */
    class SnapshotExceptionListener implements AsyncActionStep.Listener {
        private final Index index;
        private final Listener originalListener;
        private final ClusterStateObserver observer;

        SnapshotExceptionListener(Index index, Listener originalListener, ClusterStateObserver observer) {
            this.index = index;
            this.originalListener = originalListener;
            this.observer = observer;
        }

        @Override
        public void onResponse(boolean complete) {
            originalListener.onResponse(complete);
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof SnapshotInProgressException) {
                try {
                    logger.debug("[{}] attempted to run ILM step but a snapshot is in progress, step will retry at a later time",
                        index.getName());
                    observer.waitForNextChange(
                        new NoSnapshotRunningListener(observer, index.getName(), state -> {
                            IndexMetadata idxMeta = state.metadata().index(index);
                            if (idxMeta == null) {
                                // The index has since been deleted, mission accomplished!
                                originalListener.onResponse(true);
                            }
                            // Re-invoke the performAction method with the new state
                            performAction(idxMeta, state, observer, originalListener);
                        }, originalListener::onFailure),
                        // TODO: what is a good timeout value for no new state received during this time?
                        TimeValue.timeValueHours(12));
                } catch (Exception secondError) {
                    // There was a second error trying to set up an observer,
                    // fail the original listener
                    secondError.addSuppressed(e);
                    originalListener.onFailure(secondError);
                }
            } else {
                originalListener.onFailure(e);
            }
        }
    }

    /**
     * A {@link ClusterStateObserver.Listener} that invokes the given function with the new state,
     * once no snapshots are running. If a snapshot is still running it registers a new listener
     * and tries again. Passes any exceptions to the original exception listener if they occur.
     */
    class NoSnapshotRunningListener implements ClusterStateObserver.Listener {

        private final Consumer<ClusterState> reRun;
        private final Consumer<Exception> exceptionConsumer;
        private final ClusterStateObserver observer;
        private final String indexName;

        NoSnapshotRunningListener(ClusterStateObserver observer, String indexName,
                                  Consumer<ClusterState> reRun,
                                  Consumer<Exception> exceptionConsumer) {
            this.observer = observer;
            this.reRun = reRun;
            this.exceptionConsumer = exceptionConsumer;
            this.indexName = indexName;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            try {
                if (snapshotInProgress(state)) {
                    observer.waitForNextChange(this);
                } else {
                    logger.debug("[{}] retrying ILM step after snapshot has completed", indexName);
                    reRun.accept(state);
                }
            } catch (Exception e) {
                exceptionConsumer.accept(e);
            }
        }

        private boolean snapshotInProgress(ClusterState state) {
            for (SnapshotsInProgress.Entry snapshot : state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries()) {
                if (snapshot.indices().stream()
                    .map(IndexId::getName)
                    .anyMatch(name -> name.equals(indexName))) {
                    // There is a snapshot running with this index name
                    return true;
                }
            }
            // There are no snapshots for this index, so it's okay to proceed with this state
            return false;
        }

        @Override
        public void onClusterServiceClose() {
            // This means the cluster is being shut down, so nothing to do here
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            exceptionConsumer.accept(new IllegalStateException("step timed out while waiting for snapshots to complete"));
        }
    }
}
