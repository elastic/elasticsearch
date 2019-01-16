/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.IndexId;

import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Deletes a single index.
 */
public class DeleteStep extends AsyncActionStep {
    public static final String NAME = "delete";

    public DeleteStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, ClusterStateObserver observer, Listener listener) {
        getClient().admin().indices()
            .delete(new DeleteIndexRequest(indexMetaData.getIndex().getName()),
                ActionListener.wrap(response -> listener.onResponse(true),
                    exception -> {
                        // TODO: change to SnapshotInProgressException
                        if (exception instanceof IllegalArgumentException) {
                            observer.waitForNextChange(
                                new DeleteObserver(state -> {
                                    IndexMetaData idxMeta = state.metaData().index(indexMetaData.getIndex());
                                    if (idxMeta == null) {
                                        // The index has since been deleted, mission accomplished!
                                        listener.onResponse(true);
                                    }
                                    // Re-invoke the performAction method with the new state
                                    performAction(idxMeta, state, observer, listener);
                                }, listener::onFailure),
                                new WaitForNoSnapshot(indexMetaData.getIndex().getName()));
                        } else {
                            listener.onFailure(exception);
                        }
                    }));
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }

    // A predicate waiting for no snapshot to be executing for a given index
    class WaitForNoSnapshot implements Predicate<ClusterState> {
        private final String indexName;

        WaitForNoSnapshot(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public boolean test(ClusterState state) {
            SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
                // No snapshots are running, new state is acceptable to proceed
                return true;
            }

            for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
                if (snapshot.indices().stream()
                    .map(IndexId::getName)
                    .anyMatch(name -> name.equals(indexName))) {
                    // There is a snapshot running with this index name
                    return false;
                }
            }
            // There are snapshots, but none for this index, so it's okay to proceed with this state
            return true;
        }
    }

    class DeleteObserver implements ClusterStateObserver.Listener {

        private final Consumer<ClusterState> reRun;
        private final Consumer<Exception> listener;

        DeleteObserver(Consumer<ClusterState> reRun, Consumer<Exception> listener) {
            this.reRun = reRun;
            this.listener = listener;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            try {
                reRun.accept(state);
            } catch (Exception e) {
                listener.accept(e);
            }
        }

        @Override
        public void onClusterServiceClose() {
            // No problem, this will be retried next time the cluster starts up or a new master is elected
            listener.accept(new TimeoutException("cluster service is closing"));
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            assert false : "these observers should never time out";
            listener.accept(new IllegalStateException("cluster state observer timed out while waiting for acceptable state"));
        }
    }
}
