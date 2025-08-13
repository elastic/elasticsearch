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

package co.elastic.elasticsearch.stateless.multiproject;

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

@FixForMultiProject(
    description = "This service should use per-project object store. It currently uses cluster object store "
        + "because per-project object stores are currently created/deleted along with ProjectMetadata which "
        + "is too early for this service on the deletion path, i.e. the object store is already closed and "
        + "removed when the lease needs to be updated. Once project settings and secrets get moved outside of "
        + "ProjectMetadata, we should fix the ordering issue and use per-project object store here. When "
        + "per-project object store is in use, we can also rename the lease from project-xxx_lease to be "
        + "just project_lease. See also ES-11934 and ES-11206"
)
public class ProjectLifeCycleService implements ClusterStateListener {
    private static final Logger LOGGER = LogManager.getLogger(ProjectLifeCycleService.class);

    private final ClusterService cluserterService;
    private final ObjectStoreService objectStoreService;
    private final ThreadPool threadPool;
    private final Client client;
    private final MasterServiceTaskQueue<DeleteProjectTask> deleteProjectMetadataQueue;
    // The cluster's UUID used for updating a project lease
    private String clusterUuid;
    private byte[] clusterUuidBytes;
    private final Executor leaseActionExecutor;

    // A set to track the projects that are currently going through deletion steps.
    private final Set<ProjectId> runningDeletions = ConcurrentCollections.newConcurrentSet();

    public ProjectLifeCycleService(
        ClusterService clusterService,
        ObjectStoreService objectStoreService,
        ThreadPool threadPool,
        Client client
    ) {
        this.cluserterService = clusterService;
        this.objectStoreService = objectStoreService;
        this.threadPool = threadPool;
        this.client = client;
        this.leaseActionExecutor = threadPool.executor(ThreadPool.Names.SNAPSHOT_META);
        this.deleteProjectMetadataQueue = clusterService.createTaskQueue("delete-project", Priority.NORMAL, new DeleteProjectExecutor());
    }

    @FixForMultiProject(description = "Integrate lease updates in the file-based creation/deletion path after ES-11454 in ES-11206")
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false || event.state().metadata().clusterUUIDCommitted() == false) {
            return;
        }
        final var uuid = event.state().metadata().clusterUUID();
        assert uuid.equals(Metadata.UNKNOWN_CLUSTER_UUID) == false;
        if (clusterUuid == null) {
            setClusterUuid(uuid);
        }
        assert clusterUuidBytes != null;
        for (var projectId : event.state().metadata().projects().keySet()) {
            if (event.previousState().metadata().hasProject(projectId) == false) {
                // TODO: Current project creation is not properly orchestrated so that a project will be created even when it
                // is marked for deletion. This becomes an issue when master fails over during project deletion. The new master
                // immediately starts processing file-based settings and attempts to create the project again which races the
                // project deletion process. This in turn leads to race between acquiring and releasing the project lease.
                // Therefore we workaround this by not acquiring the project lease if it is marked for deletion. This will no
                // longer be necessary once we have ES-11206 that create project in a more orchestrated fashion.
                final var projectStateRegistry = ProjectStateRegistry.get(event.state());
                if (projectStateRegistry.isProjectMarkedForDeletion(projectId) || projectStateRegistry.hasProject(projectId) == false) {
                    continue;
                }
                ActionListener<Boolean> loggingListener = ActionListener.wrap(success -> {
                    if (success) {
                        LOGGER.info("acquired lease for project [{}]", projectId);
                    } else {
                        LOGGER.info("could not acquire lease for project [{}]", projectId);
                    }
                }, e -> LOGGER.info(Strings.format("error acquiring lease for project [%s]", projectId), e));
                threadPool.generic().execute(() -> acquireProjectLease(projectId, loggingListener));

            }
        }
        maybeDeleteProjects(event);
    }

    // Package private for testing
    Set<ProjectId> getRunningDeletions() {
        return Collections.unmodifiableSet(runningDeletions);
    }

    private void maybeDeleteProjects(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        final Set<ProjectId> currentProjectsMarkedForDeletion = ProjectStateRegistry.get(state).getProjectsMarkedForDeletion();

        // If the master fails over, the new master may need to release leases for projects that were marked for deletion
        // a while back, i.e. the project exists in both current and previous projectsMarkedForDeletion. So we kick off
        // the process of releasing leases for a project if it is (1) still found in the metadata and (2) not currently
        // releasing its lease.
        for (ProjectId projectId : currentProjectsMarkedForDeletion) {
            if (state.metadata().hasProject(projectId) == false || runningDeletions.contains(projectId)) {
                continue;
            }
            runningDeletions.add(projectId);
            SubscribableListener
                // Flush the project
                .<BroadcastResponse>newForked(flushListener -> {
                    LOGGER.info("flushing project [{}] before deletion", projectId);
                    new FlushProject(projectId, flushListener).run();
                })
                // Release the project lease
                .<Boolean>andThen((releaseListener, response) -> {
                    assert response.getFailedShards() == 0;
                    final var releaseProjectLease = new ReleaseProjectLease(projectId, releaseListener.delegateFailure((l, success) -> {
                        if (success) {
                            LOGGER.info("released lease for project [{}]", projectId);
                        } else {
                            // This may happen if the master fails over while the lease is being released. The old master
                            // may release the lease while the new master sees it already released. This is safe since
                            // a project cannot be un-deleted once marked for deletion and the old master should fail to
                            // delete the project metadata which will be completed by the new master.
                            LOGGER.error("could not release lease for project [{}]", projectId);
                            // When `success` is false, the project release is released by either this node or another node.
                            // It's safe to proceed to delete the project metadata. If this node is an old master, it will
                            // fail at the next step.
                        }
                        l.onResponse(success);
                    }));
                    releaseProjectLease.run();
                })
                // Remove the project metadata from the cluster state
                .<Void>andThen((metadataDeletionListener, success) -> submitDeleteProjectTask(projectId, metadataDeletionListener))
                // Log result and remove the project from the list of running deletions
                .addListener(
                    ActionListener.runAfter(
                        ActionListener.wrap(ignore -> LOGGER.info("removed project [{}] from the cluster", projectId), e -> {
                            // Receiving exception here means either releaseProjectLease stopped retry or cluster state update failed
                            // due to master change. This is OK because the new master will take over. If master fails over again back to
                            // this node after the exception. It is also OK because we remove the runningLease entry and this
                            // node will retry again after it changes back to the master.
                            LOGGER.error(
                                Strings.format(
                                    "error when deleting project metadata [%s], local node is master: [%s]",
                                    projectId,
                                    cluserterService.state().nodes().isLocalNodeElectedMaster()
                                ),
                                e
                            );
                        }),
                        () -> {
                            // Remove the running release regardless. In most cases, the project metadata should be deleted.
                            // In edge cases such as master fail-over, the new master shall take over to complete the job.
                            runningDeletions.remove(projectId);
                        }
                    )
                );
        }
    }

    // Visible for testing
    void setClusterUuid(String clusterUuid) {
        this.clusterUuid = clusterUuid;
        this.clusterUuidBytes = Base64.getUrlDecoder().decode(clusterUuid);
    }

    /**
     * Tries to update the project lease file for the given project to assign it to this cluster. If the listener is completed
     * exceptionally, there is no guarantee whether the lease is updated or not.
     *
     * @param projectId the project to assign to this cluster
     * @param listener For an unassigned lease, true if successful, false if unsuccessful (e.g. another cluster updates the
     *                 lease first) or the lease is assigned already. Otherwise, the listener is completed with an exception.
     */
    // Visible for testing
    void acquireProjectLease(ProjectId projectId, ActionListener<Boolean> listener) {
        SubscribableListener
            // Read the current lease
            .<Optional<ProjectLease>>newForked(l -> readLease(projectId, l))
            // assign to the current cluster if unassigned
            .<Boolean>andThen(leaseActionExecutor, threadPool.getThreadContext(), (l, projectLease) -> {
                var currentLease = projectLease.orElse(ProjectLease.EMPTY_LEASE);
                if (Arrays.equals(currentLease.clusterUuid(), clusterUuidBytes)) {
                    LOGGER.debug("project [{}]'s lease is already assigned to this cluster", projectId);
                    l.onResponse(true);
                    return;
                }
                if (currentLease.isAssigned()) {
                    LOGGER.debug(
                        "cannot acquire lease for project [{}] as the project is already assigned to cluster [{}].",
                        projectId,
                        currentLease.clusterUuidAsString()
                    );
                    l.onResponse(false);
                    return;
                }
                var newLease = new ProjectLease(ProjectLease.CURRENT_FORMAT_VERSION, currentLease.leaseVersion() + 1, clusterUuidBytes);
                LOGGER.debug(
                    "updating lease to assign project [{}] to this cluster (current lease: {}, proposed lease: {})",
                    projectId,
                    currentLease,
                    newLease
                );
                getProjectLeaseContainer().compareAndSetRegister(
                    OperationPurpose.CLUSTER_STATE,
                    getProjectLeaseBlobName(projectId),
                    currentLease.asBytes(),
                    newLease.asBytes(),
                    l
                );
            })
            .addListener(listener);
    }

    private class ReleaseProjectLease extends RetryableAction<Boolean> {

        private final ProjectId projectId;

        private ReleaseProjectLease(ProjectId projectId, ActionListener<Boolean> listener) {
            super(
                LOGGER,
                threadPool,
                TimeValue.timeValueMillis(5),
                TimeValue.timeValueSeconds(5),
                TimeValue.timeValueMillis(Long.MAX_VALUE),
                listener,
                leaseActionExecutor
            );
            this.projectId = projectId;
        }

        @Override
        public void tryAction(ActionListener<Boolean> listener) {
            releaseProjectLease(projectId, listener);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            final ClusterState state = cluserterService.state();
            return state.nodes().isLocalNodeElectedMaster() && state.metadata().hasProject(projectId);
        }
    }

    private class FlushProject extends RetryableAction<BroadcastResponse> {
        private final ProjectId projectId;

        private FlushProject(ProjectId projectId, ActionListener<BroadcastResponse> listener) {
            super(
                LOGGER,
                threadPool,
                TimeValue.timeValueMillis(5),
                TimeValue.timeValueSeconds(5),
                TimeValue.timeValueMillis(Long.MAX_VALUE),
                listener,
                threadPool.executor(ThreadPool.Names.FLUSH)
            );
            this.projectId = projectId;
        }

        @Override
        public void tryAction(ActionListener<BroadcastResponse> listener) {
            client.projectClient(projectId).admin().indices().prepareFlush().execute(listener.delegateFailure((l, response) -> {
                if (response.getFailedShards() > 0) {
                    l.onFailure(
                        new ElasticsearchException(
                            "flush failed for project [" + projectId + "] with " + response.getFailedShards() + " failed shards"
                        )
                    );
                } else {
                    l.onResponse(response);
                }
            }));
        }

        @Override
        public boolean shouldRetry(Exception e) {
            final ClusterState state = cluserterService.state();
            return state.nodes().isLocalNodeElectedMaster() && state.metadata().hasProject(projectId);
        }
    }

    /**
     * Tries to update the project lease file for the given project to unassign it from this cluster by setting a {@code NIL_UUID} as the
     * currently owning cluster. If the listener is completed exceptionally, there is no guarantee whether the lease is updated or not.
     *
     * @param projectId the project to unassign from this cluster
     * @param listener For a lease that is assigned to this cluster, true if successfully updated, false if the lease is unassigned or
     *                 this cluster is not the owner of this project. Otherwise, the listener is completed with an exception.
     */
    // Visible for testing
    void releaseProjectLease(ProjectId projectId, ActionListener<Boolean> listener) {
        LOGGER.info("releasing lease for project [{}]", projectId);
        SubscribableListener
            // Read the current lease
            .<Optional<ProjectLease>>newForked(l -> readLease(projectId, l))
            // Unassign if assigned to the current cluster
            .<Boolean>andThen(leaseActionExecutor, threadPool.getThreadContext(), (l, projectLease) -> {
                var currentLease = projectLease.orElse(ProjectLease.EMPTY_LEASE);
                if (currentLease.isAssigned() == false) {
                    LOGGER.debug("skipped releasing lease for project [{}] as it is already unassigned", projectId);
                    l.onResponse(false);
                    return;
                }
                if (Arrays.equals(currentLease.clusterUuid(), clusterUuidBytes) == false) {
                    LOGGER.debug("cannot release lease for project [{}] since this cluster is not the owner", projectId);
                    l.onResponse(false);
                    return;
                }
                var newLease = new ProjectLease(
                    ProjectLease.CURRENT_FORMAT_VERSION,
                    currentLease.leaseVersion() + 1,
                    ProjectLease.NIL_UUID
                );
                LOGGER.debug(
                    "updating lease to unassign project [{}] from this cluster (current lease: {}, proposed lease: {})",
                    projectId,
                    currentLease,
                    newLease
                );
                getProjectLeaseContainer().compareAndSetRegister(
                    OperationPurpose.CLUSTER_STATE,
                    getProjectLeaseBlobName(projectId),
                    currentLease.asBytes(),
                    newLease.asBytes(),
                    l.delegateFailure((l0, success) -> {
                        if (success) {
                            l0.onResponse(success);
                        } else {
                            // Will trigger a retry
                            l0.onFailure(
                                new ConcurrentModificationException("project lease for [" + projectId + "] was concurrently modified")
                            );
                        }
                    })
                );
            })
            .addListener(listener);
    }

    private void readLease(ProjectId projectId, ActionListener<Optional<ProjectLease>> listener) {
        leaseActionExecutor.execute(
            ActionRunnable.wrap(
                listener,
                l -> getProjectLeaseContainer().getRegister(
                    OperationPurpose.CLUSTER_STATE,
                    getProjectLeaseBlobName(projectId),
                    l.map(optionalBytesReference -> {
                        if (optionalBytesReference.isPresent() == false) {
                            return Optional.empty();
                        }
                        return Optional.of(ProjectLease.fromBytes(optionalBytesReference.bytesReference()));
                    })
                )
            )
        );
    }

    private void submitDeleteProjectTask(ProjectId projectId, ActionListener<Void> listener) {
        deleteProjectMetadataQueue.submitTask(
            "delete-project " + projectId,
            new DeleteProjectTask(projectId, listener),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT
        );
    }

    public static String getProjectLeaseBlobName(ProjectId projectId) {
        return "project-" + projectId.id() + "_lease";
    }

    private BlobContainer getProjectLeaseContainer() {
        return objectStoreService.getClusterRootContainer();
    }

    record DeleteProjectTask(ProjectId projectId, ActionListener<Void> listener) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private static class DeleteProjectExecutor implements ClusterStateTaskExecutor<DeleteProjectTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<DeleteProjectTask> batchExecutionContext) throws Exception {
            var metadataBuilder = Metadata.builder(batchExecutionContext.initialState().metadata());
            var routingTableBuilder = GlobalRoutingTable.builder(batchExecutionContext.initialState().globalRoutingTable());
            var clusterBlocksBuilder = ClusterBlocks.builder(batchExecutionContext.initialState().blocks());
            for (TaskContext<DeleteProjectTask> taskContext : batchExecutionContext.taskContexts()) {
                try {
                    ProjectId projectId = taskContext.getTask().projectId();
                    metadataBuilder.removeProject(projectId);
                    routingTableBuilder.removeProject(projectId);
                    clusterBlocksBuilder.removeProject(projectId);
                    taskContext.success(() -> taskContext.getTask().listener.onResponse(null));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return ClusterState.builder(batchExecutionContext.initialState())
                .metadata(metadataBuilder.build())
                .routingTable(routingTableBuilder.build())
                .blocks(clusterBlocksBuilder.build())
                .build();
        }
    }
}
