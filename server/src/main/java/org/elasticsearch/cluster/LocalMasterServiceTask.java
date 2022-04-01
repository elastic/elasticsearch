/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;

import java.util.List;

/**
 * Used to execute things on the master service thread on nodes that are not necessarily master
 */
public abstract class LocalMasterServiceTask implements ClusterStateTaskListener {

    private final Priority priority;

    public LocalMasterServiceTask(Priority priority) {
        this.priority = priority;
    }

    protected void execute(ClusterState currentState) throws Exception {}

    @Override
    public final void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
        assert false : "not called";
    }

    protected void onPublicationComplete() {}

    public void submit(MasterService masterService, String source) {
        masterService.submitStateUpdateTask(
            source,
            this,
            ClusterStateTaskConfig.build(priority),
            // Uses a new executor each time so that these tasks are not batched, but they never change the cluster state anyway so they
            // don't trigger the publication process and hence batching isn't really needed.
            new ClusterStateTaskExecutor<>() {

                @Override
                public boolean runOnlyOnMaster() {
                    return false;
                }

                @Override
                public String describeTasks(List<LocalMasterServiceTask> tasks) {
                    return ""; // only one task in the batch so the source is enough
                }

                @Override
                public ClusterState execute(ClusterState currentState, List<TaskContext<LocalMasterServiceTask>> taskContexts)
                    throws Exception {
                    final LocalMasterServiceTask thisTask = LocalMasterServiceTask.this;
                    assert taskContexts.size() == 1 && taskContexts.get(0).getTask() == thisTask
                        : "expected one-element task list containing current object but was " + taskContexts;
                    thisTask.execute(currentState);
                    taskContexts.get(0).success(new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            onPublicationComplete();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            LocalMasterServiceTask.this.onFailure(e);
                        }
                    });
                    return currentState;
                }
            }
        );
    }
}
