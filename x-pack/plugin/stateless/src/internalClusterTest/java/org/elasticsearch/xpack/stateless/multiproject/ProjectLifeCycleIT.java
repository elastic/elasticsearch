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

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryPlugin;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryStrategy;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class ProjectLifeCycleIT extends AbstractStatelessIntegTestCase {

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), StatelessMockRepositoryPlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    // TODO: The putProject and removeProject methods should be moved to AbstractStatelessIntegTestCase, see also ES-12376
    @Override
    protected void putProject(ProjectId projectId) throws Exception {
        super.putProject(projectId);
        // TODO: Wait for the lease to be claimed. This is needed until we have ES-11206
        final var blobContainer = getCurrentMasterObjectStoreService().getClusterRootContainer();
        final var clusterUuid = internalCluster().clusterService().state().metadata().clusterUUID();
        final var projectLeaseBlobName = "project-" + projectId.id() + "_lease";
        assertBusy(() -> {
            assertTrue(blobContainer.blobExists(OperationPurpose.CLUSTER_STATE, projectLeaseBlobName));
            final ProjectLease projectLease = readLease(projectLeaseBlobName);
            assertTrue(projectLease.isAssigned());
            assertEquals(projectLease.clusterUuidAsString(), clusterUuid);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void removeProject(ProjectId projectId) throws Exception {
        logger.info("--> removing project [{}]", projectId);
        final var watchedFile = getFileSettingsWatchedFile();
        final var newVersion = nextReservedStateVersion();

        // Mark the project for deletion
        // Project secrets file
        final var projectSecretsPath = watchedFile.resolveSibling("project-" + projectId + ".secrets.json");
        {
            final var map = XContentHelper.convertToMap(JSON.xContent(), Files.readString(projectSecretsPath), false);
            final var metadata = (Map<String, Object>) map.get("metadata");
            metadata.put("version", Strings.format("%s", newVersion));
            writeAndMoveContentAtomically(
                XContentHelper.convertToJson(XContentTestUtils.convertToXContent(map, JSON), false, XContentType.JSON),
                projectSecretsPath
            );
        }
        // Project settings file
        final var projectSettingsPath = watchedFile.resolveSibling("project-" + projectId + ".json");
        {
            final var map = XContentHelper.convertToMap(JSON.xContent(), Files.readString(projectSettingsPath), false);
            final var state = (Map<String, Object>) map.get("state");
            state.put("marked_for_deletion", true); // mark for deletion
            final var metadata = (Map<String, Object>) map.get("metadata");
            metadata.put("version", Strings.format("%s", newVersion));
            writeAndMoveContentAtomically(
                XContentHelper.convertToJson(XContentTestUtils.convertToXContent(map, JSON), false, XContentType.JSON),
                projectSettingsPath
            );
        }

        // Wait for lease to be released and project metadata to be removed
        final var blobContainer = getCurrentMasterObjectStoreService().getClusterRootContainer();
        final var projectLeaseBlobName = "project-" + projectId.id() + "_lease";
        assertBusy(() -> {
            assertTrue(blobContainer.blobExists(OperationPurpose.CLUSTER_STATE, projectLeaseBlobName));
            final ProjectLease projectLease = readLease(projectLeaseBlobName);
            assertFalse(projectLease.isAssigned()); // Lease is released
            assertArrayEquals(ProjectLease.NIL_UUID, projectLease.clusterUuid());
            assertFalse(internalCluster().getInstance(ClusterService.class).state().metadata().hasProject(projectId));
        });

        Files.deleteIfExists(projectSecretsPath);
        Files.deleteIfExists(projectSettingsPath);
        // TODO: Manually clean up the project registry. Otherwise the same project cannot be recreated due to the marked_for_deletion flag.
        // This is temporary because the clean-up should happen automatically when the project config files are removed. See also ES-12411
        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.submitUnbatchedStateUpdateTask("delete-project-from-state-registry", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState)
                    .putCustom(
                        ProjectStateRegistry.TYPE,
                        ProjectStateRegistry.builder(ProjectStateRegistry.get(currentState)).removeProject(projectId).build()
                    )
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e, "fail to delete project from state registry");
            }
        });

        awaitClusterState(state -> ProjectStateRegistry.get(state).hasProject(projectId) == false);
    }

    public void testLease() throws Exception {
        startMasterAndIndexNode();
        startMasterAndIndexNode();
        ensureStableCluster(2);
        var projectId = randomUniqueProjectId();
        final String projectLeaseBlobName = "project-" + projectId.id() + "_lease";
        assertThat(getLeaseBlobs().size(), equalTo(0));

        // acquire a non-existing lease
        putProject(projectId);
        AtomicInteger nextLeaseVersion = new AtomicInteger(1);
        {
            final var projectLease = readLease(projectLeaseBlobName);
            assertThat(projectLease.leaseVersion(), equalTo((long) nextLeaseVersion.get()));
        }
        nextLeaseVersion.incrementAndGet();

        // release
        removeProject(projectId);
        {
            final var projectLease = readLease(projectLeaseBlobName);
            assertThat(projectLease.leaseVersion(), equalTo((long) nextLeaseVersion.get()));
        }
        nextLeaseVersion.incrementAndGet();

        // acquire an unassigned lease
        putProject(projectId);
        {
            final var projectLease = readLease(projectLeaseBlobName);
            assertThat(projectLease.leaseVersion(), equalTo((long) nextLeaseVersion.get()));
        }
        nextLeaseVersion.incrementAndGet();

        // Clean up
        removeProject(projectId);

    }

    public void testReleasingLeaseRetries() throws Exception {
        final String masterAndIndexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final var projectId = randomUniqueProjectId();
        final String projectLeaseBlobName = "project-" + projectId.id() + "_lease";
        putProject(projectId);

        final var projectLifeCycleService = internalCluster().getCurrentMasterNodeInstance(ProjectLifeCycleService.class);
        assertThat(projectLifeCycleService.getRunningReleases(), anEmptyMap());

        // Simulate failures for releasing the lease
        final var getRegisterBarrier = new CyclicBarrier(2);
        final var getRegisterErrorCount = new AtomicInteger(between(1, 4));
        final var compareAndSetRegisterErrorCount = new AtomicInteger(between(1, 4));
        setNodeRepositoryStrategy(masterAndIndexNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerGetRegister(
                Runnable originalRunnable,
                OperationPurpose purpose,
                String key,
                ActionListener<OptionalBytesReference> listener
            ) {
                if (key.equals(projectLeaseBlobName) == false) {
                    originalRunnable.run();
                    return;
                }
                final int previousCount = getRegisterErrorCount.getAndDecrement();
                if (previousCount > 0) {
                    listener.onFailure(new IOException("Simulated failure for getRegister"));
                    return;
                }
                // Block once for the initial getRegister, not the retry triggered by compareAndSetRegister failure
                if (previousCount == 0) {
                    safeAwait(getRegisterBarrier);
                    safeAwait(getRegisterBarrier);
                }
                originalRunnable.run();
            }

            @Override
            public void blobContainerCompareAndSetRegister(
                Runnable originalRunnable,
                OperationPurpose purpose,
                String key,
                BytesReference expected,
                BytesReference updated,
                ActionListener<Boolean> listener
            ) {
                if (key.equals(projectLeaseBlobName) == false) {
                    originalRunnable.run();
                    return;
                }
                if (compareAndSetRegisterErrorCount.getAndDecrement() > 0) {
                    listener.onFailure(new IOException("Simulated failure for compareAndSetRegister"));
                    return;
                }
                originalRunnable.run();
            }
        });

        final var thread = new Thread(() -> {
            try {
                removeProject(projectId);
            } catch (Exception e) {
                fail(e, "Failed to remove project");
            }
        }, "TEST-remove-project-thread");
        thread.start();

        safeAwait(getRegisterBarrier);
        assertThat(projectLifeCycleService.getRunningReleases().keySet(), contains(projectId));
        safeAwait(getRegisterBarrier);

        thread.join(30_000);
        assertThat(thread.isAlive(), equalTo(false));

        assertThat(projectLifeCycleService.getRunningReleases(), anEmptyMap());
        assertThat(getRegisterErrorCount.get(), lessThan(0));
        assertThat(compareAndSetRegisterErrorCount.get(), lessThan(0));
    }

    public void testReleasingLeaseWorksDuringMasterFailOver() throws Exception {
        final String masterAndIndexNode1 = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final var projectId = randomUniqueProjectId();
        final String projectLeaseBlobName = "project-" + projectId.id() + "_lease";
        putProject(projectId);

        startMasterAndIndexNode();
        ensureStableCluster(3);

        final var projectLifeCycleService1 = internalCluster().getCurrentMasterNodeInstance(ProjectLifeCycleService.class);

        // Simulate failures for releasing the lease
        final var releaseStartedLatch = new CountDownLatch(1);
        final boolean failGetRegister = randomBoolean();
        setNodeRepositoryStrategy(masterAndIndexNode1, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerGetRegister(
                Runnable originalRunnable,
                OperationPurpose purpose,
                String key,
                ActionListener<OptionalBytesReference> listener
            ) {
                if (key.equals(projectLeaseBlobName) == false || failGetRegister == false) {
                    originalRunnable.run();
                    return;
                }
                releaseStartedLatch.countDown();
                listener.onFailure(new IOException("Simulated failure for getRegister"));
            }

            @Override
            public void blobContainerCompareAndSetRegister(
                Runnable originalRunnable,
                OperationPurpose purpose,
                String key,
                BytesReference expected,
                BytesReference updated,
                ActionListener<Boolean> listener
            ) {
                if (key.equals(projectLeaseBlobName) == false || failGetRegister) {
                    originalRunnable.run();
                    return;
                }
                releaseStartedLatch.countDown();
                listener.onFailure(new IOException("Simulated failure for compareAndSetRegister"));
            }
        });

        final var thread = new Thread(() -> {
            try {
                removeProject(projectId);
            } catch (Exception e) {
                fail(e, "Failed to remove project");
            }
        }, "TEST-remove-project-thread");
        thread.start();

        safeAwait(releaseStartedLatch);
        assertThat(projectLifeCycleService1.getRunningReleases().keySet(), contains(projectId));

        final var shutdownMaster = masterNodeAbdicatesForGracefulShutdown();
        assertThat(shutdownMaster, equalTo(masterAndIndexNode1));

        // New master should take over to release the project lease
        thread.join(30_000);
        assertThat(thread.isAlive(), equalTo(false));
        assertBusy(() -> assertThat(projectLifeCycleService1.getRunningReleases(), anEmptyMap()));
    }

    private List<BlobMetadata> getLeaseBlobs() throws IOException {
        var leaseBlobs = getCurrentMasterObjectStoreService().getClusterRootContainer().listBlobs(OperationPurpose.CLUSTER_STATE);
        return leaseBlobs.values()
            .stream()
            .filter(e -> e.name().startsWith("project") && e.name().endsWith("lease"))
            .collect(Collectors.toList());
    }

    private ProjectLease readLease(String blobName) throws IOException {
        try (var in = getCurrentMasterObjectStoreService().getClusterRootContainer().readBlob(OperationPurpose.CLUSTER_STATE, blobName)) {
            return ProjectLease.fromBytes(new BytesArray(in.readAllBytes()));
        }
    }
}
