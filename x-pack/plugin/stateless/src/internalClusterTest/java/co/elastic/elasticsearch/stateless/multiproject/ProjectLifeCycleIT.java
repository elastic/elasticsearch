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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

@LuceneTestCase.SuppressFileSystems("*")
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

    public void testLease() throws Exception {
        startMasterAndIndexNode();
        startMasterAndIndexNode();
        ensureStableCluster(2);
        var projectId = randomUniqueProjectId();
        final String projectLeaseBlobName = ProjectLease.leaseBlobName(projectId);
        assertThat(getLeaseBlobs().size(), equalTo(0));

        // acquire a non-existing lease
        putProject(projectId);
        AtomicInteger nextLeaseVersion = new AtomicInteger(1);
        {
            final var projectLease = readProjectLease(projectLeaseBlobName);
            assertThat(projectLease.leaseVersion(), equalTo((long) nextLeaseVersion.get()));
        }
        nextLeaseVersion.incrementAndGet();

        // release
        removeProject(projectId);
        {
            final var projectLease = readProjectLease(projectLeaseBlobName);
            assertThat(projectLease.leaseVersion(), equalTo((long) nextLeaseVersion.get()));
        }
        nextLeaseVersion.incrementAndGet();

        // acquire an unassigned lease
        putProject(projectId);
        {
            final var projectLease = readProjectLease(projectLeaseBlobName);
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
        final String projectLeaseBlobName = ProjectLease.leaseBlobName(projectId);
        putProject(projectId);

        final var projectLifeCycleService = internalCluster().getCurrentMasterNodeInstance(ProjectLifeCycleService.class);
        assertThat(projectLifeCycleService.getRunningDeletions().isEmpty(), is(true));

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
        assertThat(projectLifeCycleService.getRunningDeletions(), contains(projectId));
        safeAwait(getRegisterBarrier);

        thread.join(30_000);
        assertThat(thread.isAlive(), equalTo(false));

        assertThat(projectLifeCycleService.getRunningDeletions().isEmpty(), is(true));
        assertThat(getRegisterErrorCount.get(), lessThan(0));
        assertThat(compareAndSetRegisterErrorCount.get(), lessThan(0));
    }

    public void testReleasingLeaseWorksDuringMasterFailOver() throws Exception {
        final String masterAndIndexNode1 = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final var projectId = randomUniqueProjectId();
        final String projectLeaseBlobName = ProjectLease.leaseBlobName(projectId);
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
        assertThat(projectLifeCycleService1.getRunningDeletions(), contains(projectId));

        final var shutdownMaster = masterNodeAbdicatesForGracefulShutdown();
        assertThat(shutdownMaster, equalTo(masterAndIndexNode1));

        // New master should take over to release the project lease
        thread.join(30_000);
        assertThat(thread.isAlive(), equalTo(false));
        assertBusy(() -> assertThat(projectLifeCycleService1.getRunningDeletions().isEmpty(), is(true)));
    }

    private List<BlobMetadata> getLeaseBlobs() throws IOException {
        var leaseBlobs = getCurrentMasterObjectStoreService().getClusterRootContainer().listBlobs(OperationPurpose.CLUSTER_STATE);
        return leaseBlobs.values()
            .stream()
            .filter(e -> e.name().startsWith("project") && e.name().endsWith("lease"))
            .collect(Collectors.toList());
    }
}
