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

package co.elastic.elasticsearch.stateless.objectstore;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.coordination.ApplyCommitRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryClusterStateDelayListeners;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@TestIssueLogging(
    issueUrl = "https://github.com/elastic/elasticsearch-serverless/issues/4161",
    value = "co.elastic.elasticsearch.serverless.multiproject.MultiProjectFileSettingsService:DEBUG"
)
@LuceneTestCase.SuppressFileSystems(value = { "ExtrasFS" })
public class FsMultiProjectObjectStoreIT extends FsObjectStoreTests {

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    protected Settings projectSettings(ProjectId projectId) {
        return Settings.builder()
            .put("stateless.object_store.type", "fs")
            .put("stateless.object_store.bucket", "project_" + projectId)
            .put("stateless.object_store.base_path", "base_path")
            .build();
    }

    @Override
    protected Settings projectSecrets(ProjectId projectId) {
        return Settings.EMPTY;
    }

    @Override
    protected Settings repositorySettings(ProjectId projectId) {
        return Settings.builder().put(super.repositorySettings()).put("location", "backup_" + projectId.id()).build();
    }

    @Override
    protected void assertBackupRepositorySettings(RepositoryMetadata repositoryMetadata, ProjectId projectId) {
        assertThat(repositoryMetadata.settings().get("location"), equalTo("backup_" + projectId.id()));
    }

    @FixForMultiProject(
        description = "move it to IndexRecoveryIT or better place once https://elasticco.atlassian.net/browse/ES-12053 is done"
    )
    public void testProjectIsCorrectlyResolvedForNewShard() throws Exception {
        // In this test, the RecoveryClusterStateDelayListeners helps delaying or erroring the entire commit cluster state request on
        // the index node. In this test, it is how we reject the 1st cluster state update where the index is initially created as
        // unassigned so that the index node sees a 2nd combined cluster state update where index is both created and assigned.
        // Once it reaches this state, the test has a chance to fail without the fix. But the chance is still not high enough because
        // the 2nd cluster state can apply fast enough (on applier thread) for the index to be visible before the recovery happens
        // (on generic thread). This is not something RecoveryClusterStateDelayListeners can help because, as explained earlier,
        // it works on the entirety of the request, not delaying it halfway through its processing. To increase the chances for the
        // test to fail without the fix, we delay the application of the 2nd cluster state by waiting for the shard to pass the
        // RECOVERING state, which means its blob store is correctly resolved.
        startMasterAndIndexNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        final ProjectId projectId = randomUniqueProjectId();
        putProject(projectId);

        final var indexName = "index";
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final ClusterService indexNodeClusterService = internalCluster().clusterService(indexNode);
        final var versionToDelay = new AtomicLong(-1);
        indexNodeClusterService.addLowPriorityApplier(event -> {
            if (versionToDelay.get() == event.state().version()) {
                // Wait until the shard has passed the recovering state which means its blob store is correctly resolved
                waitUntil(() -> {
                    final var index = StreamSupport.stream(indicesService.spliterator(), false)
                        .filter(indexService -> indexService.index().getName().equals(indexName))
                        .findFirst()
                        .orElse(null);
                    if (index == null) {
                        return false;
                    }
                    if (index.hasShard(0) == false) {
                        return false;
                    }
                    final IndexShardState state = index.getShard(0).state();
                    return state.id() > IndexShardState.RECOVERING.id();
                });
            }
        });

        final Client projectClient = client().projectClient(projectId);
        final long initialClusterStateVersion = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state().version();

        try (var recoveryClusterStateDelayListeners = new RecoveryClusterStateDelayListeners(initialClusterStateVersion)) {
            MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
            indexTransportService.addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
                assertThat(request, instanceOf(ApplyCommitRequest.class));
                recoveryClusterStateDelayListeners.getClusterStateDelayListener(((ApplyCommitRequest) request).getVersion())
                    .addListener(ActionListener.wrap(ignored -> handler.messageReceived(request, channel, task), channel::sendResponse));
            });
            recoveryClusterStateDelayListeners.addCleanup(indexTransportService::clearInboundRules);

            final var searchNodeClusterService = internalCluster().getInstance(ClusterService.class, searchNode);
            final var indexCreated = new AtomicBoolean();
            final ClusterStateListener clusterStateListener = event -> {
                final var indexNodeProceedListener = recoveryClusterStateDelayListeners.getClusterStateDelayListener(
                    event.state().version()
                );
                final var indexRoutingTable = event.state().routingTable(projectId).index(indexName);
                assertNotNull(indexRoutingTable);
                final var indexShardRoutingTable = indexRoutingTable.shard(0);

                if (indexShardRoutingTable.primaryShard().assignedToNode() == false && indexCreated.compareAndSet(false, true)) {
                    // this is the cluster state update which creates the index, so fail the application in order to increase the chances of
                    // missing the index in the cluster state when the shard is recovered from the empty store
                    // We also delay the next cluster state application on the index node so that the recovery process has a better chance
                    // to not see the index in the cluster state
                    versionToDelay.set(event.state().version() + 1);
                    indexNodeProceedListener.onFailure(new RuntimeException("Unable to process cluster state update"));
                } else {
                    // this is some other cluster state update, so we must let it proceed now
                    indexNodeProceedListener.onResponse(null);
                }
            };
            searchNodeClusterService.addListener(clusterStateListener);
            recoveryClusterStateDelayListeners.addCleanup(() -> searchNodeClusterService.removeListener(clusterStateListener));

            projectClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    indexSettings(1, 1).put("index.routing.allocation.include._name", indexNode + "," + searchNode)
                        .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                )
                .get(TEST_REQUEST_TIMEOUT);

            // Enure green with the project client
            final var healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, indexName).masterNodeTimeout(TEST_REQUEST_TIMEOUT)
                .timeout(TEST_REQUEST_TIMEOUT)
                .waitForStatus(ClusterHealthStatus.GREEN)
                .waitForEvents(Priority.LANGUID)
                .waitForNoRelocatingShards(true)
                .waitForNoInitializingShards(true)
                .waitForNodes(Integer.toString(cluster().size()));
            final var clusterHealthResponse = projectClient.admin().cluster().health(healthRequest).get();
            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            assertThat(clusterHealthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        } finally {
            removeProject(projectId);
        }
    }
}
