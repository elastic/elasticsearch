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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.snapshots.SnapshotInProgressException;

/**
 * This is an abstract AsyncActionStep that wraps the performed action listener, checking to see
 * if the action fails due to a snapshot being in progress. If a snapshot is in progress, it
 * registers an observer and waits to try again when a snapshot is no longer running.
 */
public abstract class AsyncRetryDuringSnapshotActionStep extends AsyncActionStep {
    private static final Logger logger = LogManager.getLogger(AsyncRetryDuringSnapshotActionStep.class);

    public AsyncRetryDuringSnapshotActionStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public final void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState,
                                    ClusterStateObserver observer, ActionListener<Void> listener) {
        // Wrap the original listener to handle exceptions caused by ongoing snapshots
        SnapshotExceptionListener snapshotExceptionListener = new SnapshotExceptionListener(indexMetadata.getIndex(), listener, observer,
                currentClusterState.nodes().getLocalNode());
        performDuringNoSnapshot(indexMetadata, currentClusterState, snapshotExceptionListener);
    }

    /**
     * Method to be performed during which no snapshots for the index are already underway.
     */
    abstract void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Void> listener);

    /**
     * SnapshotExceptionListener is an injected listener wrapper that checks to see if a particular
     * action failed due to a {@code SnapshotInProgressException}. If it did, then it registers a
     * ClusterStateObserver listener waiting for the next time the snapshot is not running,
     * re-running the step's {@link #performAction(IndexMetadata, ClusterState, ClusterStateObserver, ActionListener)}
     * method when the snapshot is no longer running.
     */
    class SnapshotExceptionListener implements ActionListener<Void> {
        private final Index index;
        private final ActionListener<Void> originalListener;
        private final ClusterStateObserver observer;
        private final DiscoveryNode localNode;

        SnapshotExceptionListener(Index index, ActionListener<Void> originalListener, ClusterStateObserver observer,
                                  DiscoveryNode localNode) {
            this.index = index;
            this.originalListener = originalListener;
            this.observer = observer;
            this.localNode = localNode;
        }

        @Override
        public void onResponse(Void unused) {
            originalListener.onResponse(null);
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof SnapshotInProgressException) {
                try {
                    logger.debug("[{}] attempted to run ILM step but a snapshot is in progress, step will retry at a later time",
                            index.getName());
                    final String indexName = index.getName();
                    observer.waitForNextChange(
                            new ClusterStateObserver.Listener() {
                                @Override
                                public void onNewClusterState(ClusterState state) {
                                    if (state.nodes().isLocalNodeElectedMaster() == false) {
                                        originalListener.onFailure(new NotMasterException("no longer master"));
                                        return;
                                    }
                                    try {
                                        logger.debug("[{}] retrying ILM step after snapshot has completed", indexName);
                                        IndexMetadata idxMeta = state.metadata().index(index);
                                        if (idxMeta == null) {
                                            // The index has since been deleted, mission accomplished!
                                            originalListener.onResponse(null);
                                        } else {
                                            // Re-invoke the performAction method with the new state
                                            performAction(idxMeta, state, observer, originalListener);
                                        }
                                    } catch (Exception e) {
                                        originalListener.onFailure(e);
                                    }
                                }

                                @Override
                                public void onClusterServiceClose() {
                                    originalListener.onFailure(new NodeClosedException(localNode));
                                }

                                @Override
                                public void onTimeout(TimeValue timeout) {
                                    originalListener.onFailure(
                                            new IllegalStateException("step timed out while waiting for snapshots to complete"));
                                }
                            },
                            state -> {
                                if (state.nodes().isLocalNodeElectedMaster() == false) {
                                    // ILM actions should only run on master, lets bail on failover
                                    return true;
                                }
                                if (state.metadata().index(index) == null) {
                                    // The index has since been deleted, mission accomplished!
                                    return true;
                                }
                                for (SnapshotsInProgress.Entry snapshot :
                                        state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries()) {
                                    if (snapshot.indices().containsKey(indexName)) {
                                        // There is a snapshot running with this index name
                                        return false;
                                    }
                                }
                                // There are no snapshots for this index, so it's okay to proceed with this state
                                return true;
                            },
                            TimeValue.MAX_VALUE);
                } catch (Exception secondError) {
                    // There was a second error trying to set up an observer,
                    // fail the original listener
                    secondError.addSuppressed(e);
                    assert false : new AssertionError("This should never fail", secondError);
                    originalListener.onFailure(secondError);
                }
            } else {
                originalListener.onFailure(e);
            }
        }
    }
}
