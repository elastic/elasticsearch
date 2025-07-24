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

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class StatelessLeaseIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(ShutdownPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    public void testLeaseFormatUpdateAndStatelessClusterConsistencyCheck() throws Exception {
        String node1 = startMasterAndIndexNode();
        String node2 = startMasterAndIndexNode();
        ensureStableCluster(2);
        var projectId = randomUniqueProjectId();
        try {
            putProject(projectId);
        } catch (Exception e) {
            fail(e, "failed to create project [%s]", projectId);
        }
        // move back the lease to the legacy format
        final var lease1 = readLease(getObjectStoreService(node1));
        assertThat(lease1.formatVersion(), equalTo(StatelessLease.V1_FORMAT_VERSION));
        assertThat(lease1.nodeLeftGeneration(), equalTo(0L));
        assertThat(lease1.projectsUnderDeletedGeneration(), equalTo(0L));
        getObjectStoreService(node1).getClusterStateBlobContainer()
            .writeBlob(
                OperationPurpose.CLUSTER_STATE,
                StatelessElectionStrategy.LEASE_BLOB,
                new StatelessLease(StatelessLease.LEGACY_FORMAT_VERSION, lease1.currentTerm(), lease1.nodeLeftGeneration(), 0).asBytes(),
                false
            );
        final var lease2 = readLease(getObjectStoreService(node1));
        assertThat(lease2.formatVersion(), equalTo(StatelessLease.LEGACY_FORMAT_VERSION));
        // Cluster consistency check is version agnostic
        var master1 = internalCluster().getMasterName();
        var consistencyService = internalCluster().getInstance(
            StatelessClusterConsistencyService.class,
            master1.equals(node1) ? node2 : node1
        );
        safeAwait(
            SubscribableListener.<Void>newForked(l -> consistencyService.ensureClusterStateConsistentWithRootBlob(l, TEST_REQUEST_TIMEOUT))
        );
        // changing master updates the lease but doesn't change the format
        var l = client().execute(
            PutShutdownNodeAction.INSTANCE,
            new PutShutdownNodeAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                getNodeId(internalCluster().getMasterName()),
                SingleNodeShutdownMetadata.Type.SIGTERM,
                "Shutdown for test",
                null,
                null,
                TimeValue.timeValueMinutes(randomIntBetween(1, 5))
            )
        );
        var response = l.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(response.isAcknowledged(), equalTo(true));
        assertBusy(() -> { assertThat(internalCluster().getMasterName(), not(equalTo(master1))); });
        final var lease3 = readLease(getObjectStoreService(node1));
        assertThat(lease3.formatVersion(), equalTo(StatelessLease.LEGACY_FORMAT_VERSION));
        assertThat(lease3.currentTerm(), equalTo(lease2.currentTerm() + 1));
        assertThat(lease3.nodeLeftGeneration(), equalTo(lease2.nodeLeftGeneration()));
        assertThat(lease3.projectsUnderDeletedGeneration(), equalTo(lease2.projectsUnderDeletedGeneration()));
        // Cluster consistency check is version agnostic
        safeAwait(
            SubscribableListener.<Void>newForked(
                l2 -> consistencyService.ensureClusterStateConsistentWithRootBlob(l2, TEST_REQUEST_TIMEOUT)
            )
        );
        // Changes to node gen (or project deletion) updates the lease and changes the format
        var removeNode = randomBoolean();
        if (removeNode) {
            internalCluster().stopNode(master1);
            ensureStableCluster(1);
        } else {
            markProjectsForDeletion(Set.of(projectId));
        }
        final var lease4 = readLease(getObjectStoreService(internalCluster().getMasterName()));
        assertThat(lease4.formatVersion(), equalTo(StatelessLease.V1_FORMAT_VERSION));
        assertThat(lease4.currentTerm(), equalTo(lease3.currentTerm()));
        if (removeNode) {
            assertThat(lease4.nodeLeftGeneration(), equalTo(lease3.nodeLeftGeneration() + 1));
            assertThat(lease4.projectsUnderDeletedGeneration(), equalTo(lease3.projectsUnderDeletedGeneration()));
        } else {
            assertThat(lease4.nodeLeftGeneration(), equalTo(lease3.nodeLeftGeneration()));
            assertThat(lease4.projectsUnderDeletedGeneration(), equalTo(lease3.projectsUnderDeletedGeneration() + 1));
        }
    }

    public void testProjectDeletionGenIsWrittenInRootBlob() throws Exception {
        String indexNode1 = startMasterAndIndexNode();
        String indexNode2 = startMasterAndIndexNode();
        ensureStableCluster(2);

        final var projectIds = randomSet(1, 3, ESTestCase::randomUniqueProjectId);
        for (var projectId : projectIds) {
            try {
                putProject(projectId);
            } catch (Exception e) {
                fail(e, "failed to create project [%s]", projectId);
            }
        }

        ObjectStoreService objectStoreService = getObjectStoreService(indexNode1);
        var lease1 = readLease(objectStoreService);
        assertThat(lease1.formatVersion(), equalTo(StatelessLease.V1_FORMAT_VERSION));
        markProjectsForDeletion(randomNonEmptySubsetOf(projectIds));
        var lease2 = readLease(objectStoreService);
        assertThat(lease2.projectsUnderDeletedGeneration(), equalTo(lease1.projectsUnderDeletedGeneration() + 1));
        assertThat(lease2.nodeLeftGeneration(), equalTo(lease1.nodeLeftGeneration()));
        assertThat(lease2.currentTerm(), equalTo(lease1.currentTerm()));
        assertThat(lease2.formatVersion(), equalTo(StatelessLease.V1_FORMAT_VERSION));
        final StatelessClusterConsistencyService consistencyService = internalCluster().getInstance(
            StatelessClusterConsistencyService.class,
            randomFrom(indexNode1, indexNode2)
        );
        safeAwait(
            SubscribableListener.<Void>newForked(l -> consistencyService.ensureClusterStateConsistentWithRootBlob(l, TEST_REQUEST_TIMEOUT))
        );
    }

    public static StatelessLease readLease(ObjectStoreService objectStoreService) throws IOException {
        try (
            var inputStream = objectStoreService.getClusterStateBlobContainer()
                .readBlob(OperationPurpose.CLUSTER_STATE, StatelessElectionStrategy.LEASE_BLOB)
        ) {
            var leaseOptional = StatelessLease.fromBytes(OptionalBytesReference.of(new BytesArray(inputStream.readAllBytes())));
            assertThat(leaseOptional.isPresent(), equalTo(true));
            return leaseOptional.get();
        }
    }

    // TODO: do this via updating file-based settings once https://elasticco.atlassian.net/browse/ES-12375 is done
    private void markProjectsForDeletion(Collection<ProjectId> projects) {
        final var projectsMarkedForDeletion = new CountDownLatch(1);
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    var builder = ProjectStateRegistry.builder(currentState);
                    for (ProjectId projectId : projects) {
                        builder.markProjectForDeletion(projectId);
                    }
                    return ClusterState.builder(currentState).putCustom(ProjectStateRegistry.TYPE, builder.build()).build();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Unexpected exception: " + e);
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    projectsMarkedForDeletion.countDown();
                }
            });
        safeAwait(projectsMarkedForDeletion);
    }
}
