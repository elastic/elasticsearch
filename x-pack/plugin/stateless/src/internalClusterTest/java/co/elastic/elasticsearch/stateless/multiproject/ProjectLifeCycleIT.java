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

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class ProjectLifeCycleIT extends AbstractStatelessIntegTestCase {

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    public void testLease() throws Exception {
        startMasterAndIndexNode();
        startMasterAndIndexNode();
        ensureStableCluster(2);
        String clusterUuid = internalCluster().clusterService().state().metadata().clusterUUID();
        var projectId = randomUniqueProjectId();
        assertThat(getLeaseBlobs().size(), equalTo(0));

        // acquire a non-existing lease
        putProject(projectId);
        AtomicInteger nextLeaseVersion = new AtomicInteger(1);
        assertBusy(() -> {
            var leases = getLeaseBlobs();
            assertThat(leases.size(), equalTo(1));
            var projectLeaseBlob = leases.getFirst();
            assertThat(projectLeaseBlob.length(), equalTo((long) ProjectLease.LEASE_SIZE_IN_BYTES));
            var projectLease = readLease(projectLeaseBlob.name());
            assertTrue(projectLease.isAssigned());
            assertEquals(projectLease.clusterUuidAsString(), clusterUuid);
            assertThat(projectLease.leaseVersion(), equalTo((long) nextLeaseVersion.get()));
        });
        nextLeaseVersion.incrementAndGet();

        // release
        removeProject(projectId);
        assertBusy(() -> {
            var leases = getLeaseBlobs();
            assertThat(leases.size(), equalTo(1));
            var projectLeaseBlob = leases.getFirst();
            assertThat(projectLeaseBlob.length(), equalTo((long) ProjectLease.LEASE_SIZE_IN_BYTES));
            var projectLease = readLease(projectLeaseBlob.name());
            assertFalse(projectLease.isAssigned());
            assertArrayEquals(ProjectLease.NIL_UUID, projectLease.clusterUuid());
            assertThat(projectLease.leaseVersion(), equalTo((long) nextLeaseVersion.get()));
        });
        nextLeaseVersion.incrementAndGet();

        // acquire an unassigned lease
        putProject(projectId);
        assertBusy(() -> {
            var leases = getLeaseBlobs();
            assertThat(leases.size(), equalTo(1));
            var projectLeaseBlob = leases.getFirst();
            assertThat(projectLeaseBlob.length(), equalTo((long) ProjectLease.LEASE_SIZE_IN_BYTES));
            var projectLease = readLease(projectLeaseBlob.name());
            assertTrue(projectLease.isAssigned());
            assertEquals(projectLease.clusterUuidAsString(), clusterUuid);
            assertThat(projectLease.leaseVersion(), equalTo((long) nextLeaseVersion.get()));
        });
        nextLeaseVersion.incrementAndGet();

        // Clean up
        removeProject(projectId);

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
