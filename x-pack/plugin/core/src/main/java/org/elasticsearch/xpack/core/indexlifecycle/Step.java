/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * A {@link LifecycleAction} which deletes the index.
 */
public class Step {
    private final String name;
    private final String action;
    private final String phase;
    private final StepKey nextStepKey;

    public Step(String name, String action, String phase, StepKey nextStepKey) {
        this.name = name;
        this.action = action;
        this.phase = phase;
        this.nextStepKey = nextStepKey;
    }

    public String getName() {
        return name;
    }

    public String getAction() {
        return action;
    }

    public String getPhase() {
        return phase;
    }

    public StepKey getNextStepKey() {
        return nextStepKey;
    }

    public boolean hasNextStep() {
        return nextStepKey != null;
    }

    /**
     * Executes this step and updates the cluster state with the next step to run
     *
     * @param currentState
     * @param client
     * @param nowSupplier
     * @return
     */
    public StepResult execute(ClusterService clusterService, ClusterState currentState, Index index, Client client, LongSupplier nowSupplier) {
//        Example: Delete
//
//        client.admin().indices().prepareDelete(index.getName()).execute(new ActionListener<DeleteIndexResponse>() {
//            @Override
//            public void onResponse(DeleteIndexResponse deleteIndexResponse) {
//                if (deleteIndexResponse.isAcknowledged()) {
//                    submitUpdateNextStepTask(clusterService, nowSupplier, index);
//                }
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//
//            }
//        });
        throw new UnsupportedOperationException("implement me");
    }

    protected void submitUpdateNextStepTask(ClusterService clusterService, LongSupplier nowSupplier, Index index) {
        clusterService.submitStateUpdateTask("update-next-step", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return updateStateWithNextStep(currentState, nowSupplier, index);
            }

            @Override
            public void onFailure(String source, Exception e) {

            }
        });
    }

    protected ClusterState updateStateWithNextStep(ClusterState currentState, LongSupplier nowSupplier, Index index) {
        long now = nowSupplier.getAsLong();
        // fetch details about next step to run and update the cluster state with this information
        Settings newLifecyclePhaseSettings = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_PHASE, nextStepKey.getPhase())
            .put(LifecycleSettings.LIFECYCLE_PHASE_TIME, now)
            .put(LifecycleSettings.LIFECYCLE_ACTION_TIME, now)
            .put(LifecycleSettings.LIFECYCLE_ACTION, nextStepKey.getAction())
            .put(LifecycleSettings.LIFECYCLE_STEP_TIME, now)
            .put(LifecycleSettings.LIFECYCLE_STEP, nextStepKey.getName())
            .build();
        return ClusterState.builder(currentState)
            .metaData(MetaData.builder(currentState.metaData())
                .updateSettings(newLifecyclePhaseSettings, index.getName())).build();
    }

    public static class StepKey {
        private final String phase;

        private final String action;
        private final String name;

        public StepKey(String phase, String action, String name) {
            this.phase = phase;
            this.action = action;
            this.name = name;
        }

        public String getPhase() {
            return phase;
        }

        public String getAction() {
            return action;
        }

        public String getName() {
            return name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(phase, action, name);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StepKey other = (StepKey) obj;
            return Objects.equals(phase, other.phase) && Objects.equals(action, other.action) && Objects.equals(name, other.name);
        }
    }
}
