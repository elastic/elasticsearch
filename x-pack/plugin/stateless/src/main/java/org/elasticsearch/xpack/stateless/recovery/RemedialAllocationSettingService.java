/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.Stateless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This service is executed on master eligible nodes at least once to ensure all indices in the cluster have their stateless allocation
 * settings configured.
 *
 * In this specific case, we do not worry about older masters being elected after remediation and undoing the fixes because serverless
 * is deployed weekly, and the fixes this remediation service is employing will have already been fixed for new indices going forward.
 */
public class RemedialAllocationSettingService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(RemedialAllocationSettingService.class);

    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<RemedialAllocationSettingTask> queue;

    private final AtomicBoolean remediationComplete = new AtomicBoolean(false);

    public RemedialAllocationSettingService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.queue = clusterService.createTaskQueue(
            "remediate-stateful-allocation-settings",
            Priority.URGENT,
            new RemediateAllocationSettingClusterStateExecutor()
        );
    }

    // visible for testing
    boolean isRemediationComplete() {
        return remediationComplete.get();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // wait for the cluster state to be recovered
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        if (remediationComplete.get()) {
            // Remediation is already completed, just remove the listener
            clusterService.removeListener(RemedialAllocationSettingService.this);
            return;
        }

        if (event.localNodeMaster()) {
            // Check to see if we need to remediate any indices.
            boolean remediationRequired = false;
            for (ProjectId projectId : state.metadata().projects().keySet()) {
                ProjectState projectState = state.projectState(projectId);
                for (DataStream dataStream : projectState.metadata().dataStreams().values()) {
                    if (dataStream.getFailureIndices().isEmpty()) {
                        continue;
                    }
                    for (Index failureIndex : dataStream.getFailureIndices()) {
                        boolean allocatorSettingPresent = ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.exists(
                            projectState.metadata().index(failureIndex).getSettings()
                        );
                        if (allocatorSettingPresent == false) {
                            remediationRequired = true;
                        }
                    }
                }
            }

            if (remediationRequired) {
                queue.submitTask("remediate-serverless-index-settings", new RemedialAllocationSettingTask(new ActionListener<>() {
                    @Override
                    public void onResponse(Void result) {
                        logger.info("Failure index recovery settings have been remediated");
                        deregisterAndMarkComplete();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (MasterService.isPublishFailureException(e)) {
                            // Some external issue with publishing state. Leave the listener registered.
                            logger.info("Publication failure while remediating failure indices recovery settings, will try again later");
                        } else {
                            // There seems to have been a problem. Instead of likely failing repeatedly for an indefinite period, log
                            // what we can about the problem and de-register ourselves
                            logger.error("Could not remediate failure index recovery settings", e);
                            deregisterAndMarkComplete();
                        }
                    }
                }), MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT);
            } else {
                deregisterAndMarkComplete();
            }
        }
    }

    private void deregisterAndMarkComplete() {
        clusterService.removeListener(RemedialAllocationSettingService.this);
        remediationComplete.getAndSet(true);
    }

    private Tuple<ClusterState, Void> updateCluster(ClusterState originalState) {
        ClusterState.Builder clusterStateBuilder = null;
        // This looks like a scary set of nested loops, but it's not. Failure indices cannot be shared between projects or data streams.
        for (Map.Entry<ProjectId, ProjectMetadata> projectEntry : originalState.metadata().projects().entrySet()) {
            ProjectMetadata.Builder projectMetadataBuilder = null;
            for (DataStream dataStreams : projectEntry.getValue().dataStreams().values()) {
                for (Index failureIndex : dataStreams.getFailureIndices()) {
                    IndexMetadata failureIndexMetadata = projectEntry.getValue().index(failureIndex);
                    Settings failureIndexSettings = failureIndexMetadata.getSettings();
                    if (ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.exists(failureIndexSettings) == false) {
                        if (projectMetadataBuilder == null) {
                            projectMetadataBuilder = ProjectMetadata.builder(projectEntry.getValue());
                        }
                        // Remediate failure index setting
                        projectMetadataBuilder.put(addStatelessExistingShardsAllocatorSetting(failureIndexMetadata), false);
                    }
                }
            }
            if (projectMetadataBuilder != null) {
                if (clusterStateBuilder == null) {
                    clusterStateBuilder = ClusterState.builder(originalState);
                }
                clusterStateBuilder.putProjectMetadata(projectMetadataBuilder);
            }
        }
        if (clusterStateBuilder != null) {
            return new Tuple<>(clusterStateBuilder.build(), null);
        } else {
            return new Tuple<>(originalState, null);
        }
    }

    static IndexMetadata addStatelessExistingShardsAllocatorSetting(IndexMetadata original) {
        return IndexMetadata.builder(original)
            .settings(
                Settings.builder()
                    .put(original.getSettings())
                    .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), Stateless.NAME)
            )
            .build();
    }

    public class RemediateAllocationSettingClusterStateExecutor extends SimpleBatchedExecutor<RemedialAllocationSettingTask, Void> {
        @Override
        public Tuple<ClusterState, Void> executeTask(RemedialAllocationSettingTask task, ClusterState clusterState) throws Exception {
            return updateCluster(clusterState);
        }

        @Override
        public void taskSucceeded(RemedialAllocationSettingTask task, Void result) {
            task.listener().onResponse(result);
        }
    }

    public record RemedialAllocationSettingTask(ActionListener<Void> listener) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }
}
