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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.Executor;

public class ProjectLifeCycleService implements ClusterStateListener {
    private static final Logger LOGGER = LogManager.getLogger(ProjectLifeCycleService.class);

    private final ObjectStoreService objectStoreService;
    private final ThreadPool threadPool;
    // The cluster's UUID used for updating a project lease
    private String clusterUuid;
    private byte[] clusterUuidBytes;
    private final Executor executor;

    public ProjectLifeCycleService(ObjectStoreService objectStoreService, ThreadPool threadPool) {
        this.objectStoreService = objectStoreService;
        this.threadPool = threadPool;
        this.executor = threadPool.executor(ThreadPool.Names.SNAPSHOT_META);
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
                ActionListener<Boolean> loggingListener = ActionListener.wrap(success -> {
                    if (success) {
                        LOGGER.info("acquired lease for project [{}]", projectId);
                    } else {
                        LOGGER.info("could not acquire lease for project [{}]", projectId);
                    }
                }, e -> LOGGER.info("error acquiring lease for project [{}]", projectId, e));
                threadPool.generic().execute(() -> acquireProjectLease(projectId, loggingListener));

            }
        }
        for (var projectId : event.previousState().metadata().projects().keySet()) {
            if (event.state().metadata().hasProject(projectId) == false) {
                ActionListener<Boolean> loggingListener = ActionListener.wrap(success -> {
                    if (success) {
                        LOGGER.info("released lease for project [{}]", projectId);
                    } else {
                        // todo: should this even happen?
                        LOGGER.error("could not release lease for project [{}]", projectId);
                    }
                }, e -> LOGGER.error("error releasing lease for project [{}]", projectId, e));
                threadPool.generic().execute(() -> releaseProjectLease(projectId, loggingListener));
            }
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
            .<Boolean>andThen(executor, threadPool.getThreadContext(), (l, projectLease) -> {
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
        SubscribableListener
            // Read the current lease
            .<Optional<ProjectLease>>newForked(l -> readLease(projectId, l))
            // Unassign if assigned to the current cluster
            .<Boolean>andThen(executor, threadPool.getThreadContext(), (l, projectLease) -> {
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
                    l
                );
            })
            .addListener(listener);
    }

    private void readLease(ProjectId projectId, ActionListener<Optional<ProjectLease>> listener) {
        executor.execute(
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

    @FixForMultiProject(description = "after ES-11041, this can be just project_lease")
    public static String getProjectLeaseBlobName(ProjectId projectId) {
        return "project-" + projectId.id() + "_lease";
    }

    @FixForMultiProject(description = "after ES-11041, this should return the project-specific container")
    private BlobContainer getProjectLeaseContainer() {
        return objectStoreService.getClusterRootContainer();
    }
}
