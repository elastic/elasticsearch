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
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.index.MergePolicyConfig.INDEX_MERGE_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@LuceneTestCase.SuppressFileSystems("*")
public class ProjectSealingIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(InternalSettingsPlugin.class, StatelessMockRepositoryPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testFlushProject() throws Exception {
        startMasterAndIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        startMasterAndIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        startSearchNode();
        ensureStableCluster(3);

        var projects = randomSet(2, 4, ESTestCase::randomUniqueProjectId);
        logger.info("--> creating projects {}", projects);
        for (ProjectId project : projects) {
            putProject(project);
        }
        logger.info("--> creating indices...");
        Map<ProjectId, Collection<String>> indicesPerProject = new HashMap<>();
        var indices = randomSet(1, 4, ESTestCase::randomIdentifier);
        for (ProjectId project : projects) {
            final var projectIndices = randomNonEmptySubsetOf(indices);
            indicesPerProject.put(project, projectIndices);
            for (String indexName : projectIndices) {
                var indexSettings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                    .put(INDEX_MERGE_ENABLED, false)
                    .build();
                createIndex(project, indexName, indexSettings);
            }
        }
        ensureGreen();

        final Set<ShardId> allShards = internalCluster().clusterService()
            .state()
            .globalRoutingTable()
            .routingTables()
            .values()
            .stream()
            .flatMap(RoutingTable::allShards)
            .filter(ShardRouting::isPromotableToPrimary)
            .map(ShardRouting::shardId)
            .collect(Collectors.toSet());
        var shardSegmentGenerationsBeforeFlush = allShards.stream()
            .collect(Collectors.toMap(Function.identity(), this::getSegmentGeneration));

        Map<ProjectId, Set<ShardId>> shardsWithWritesPerProject = new HashMap<>();
        for (ProjectId project : projects) {
            var shards = new HashSet<ShardId>();
            shardsWithWritesPerProject.put(project, shards);
            for (String indexName : indicesPerProject.get(project)) {
                var response = indexDocs(project, indexName, randomIntBetween(10, 20));
                shards.addAll(Arrays.stream(response.getItems()).map(i -> i.getResponse().getShardId()).toList());
            }
        }

        final var flushedProject = randomFrom(projects);
        final var metadata = internalCluster().clusterService().state().metadata();
        final int flushedProjectTotalShardCount = (int) allShards.stream()
            .filter(s -> metadata.projectFor(s.getIndex()).id().equals(flushedProject))
            .count();
        logger.info("--> flushing project [{}]", flushedProject);
        logger.info("--> Shards with writes per project: {}", shardsWithWritesPerProject);
        var flushResp = client().projectClient(flushedProject).admin().indices().prepareFlush().execute().actionGet();
        assertThat(flushResp.getTotalShards(), greaterThanOrEqualTo(shardsWithWritesPerProject.get(flushedProject).size()));
        assertThat(flushResp.getTotalShards(), equalTo(flushedProjectTotalShardCount));
        assertThat("expected no shard flush failure", flushResp.getFailedShards(), equalTo(0));
        assertThat(flushResp.getSuccessfulShards(), greaterThanOrEqualTo(shardsWithWritesPerProject.get(flushedProject).size()));
        assertThat(flushResp.getSuccessfulShards(), equalTo(flushedProjectTotalShardCount));

        for (ShardId shardId : allShards) {
            final var projectId = internalCluster().clusterService().state().metadata().projectFor(shardId.getIndex()).id();
            final long currentGeneration = getSegmentGeneration(shardId);
            final long previousGeneration = shardSegmentGenerationsBeforeFlush.get(shardId);
            if (projectId.equals(flushedProject) && shardsWithWritesPerProject.get(flushedProject).contains(shardId)) {
                assertThat(currentGeneration, equalTo(previousGeneration + 1));
            } else {
                assertThat(currentGeneration, equalTo(previousGeneration));
            }
        }

        for (ProjectId project : projects) {
            removeProject(project);
        }
    }

    public void testProjectDeletionFlushes() throws Exception {
        final var indexNode1 = startMasterAndIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        startMasterAndIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        startSearchNode();
        ensureStableCluster(3);

        var projects = randomSet(2, 4, ESTestCase::randomUniqueProjectId);
        for (ProjectId project : projects) {
            putProject(project);
        }
        Map<ProjectId, Collection<String>> indicesPerProject = new HashMap<>();
        var indices = randomSet(1, 4, ESTestCase::randomIdentifier);
        for (ProjectId project : projects) {
            final var projectIndices = randomNonEmptySubsetOf(indices);
            indicesPerProject.put(project, projectIndices);
            for (String indexName : projectIndices) {
                var indexSettings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                    .put(INDEX_MERGE_ENABLED, false)
                    .build();
                createIndex(project, indexName, indexSettings);
            }
        }
        ensureGreen();

        final Set<ShardId> allShards = internalCluster().clusterService()
            .state()
            .globalRoutingTable()
            .routingTables()
            .values()
            .stream()
            .flatMap(RoutingTable::allShards)
            .filter(ShardRouting::isPromotableToPrimary)
            .map(ShardRouting::shardId)
            .collect(Collectors.toSet());
        var shardSegmentGenerationsBeforeFlush = allShards.stream()
            .collect(Collectors.toMap(Function.identity(), this::getSegmentGeneration));

        for (ProjectId project : projects) {
            for (String indexName : indicesPerProject.get(project)) {
                indexDocs(project, indexName, randomIntBetween(10, 20));
            }
        }

        final var deletingProject = randomFrom(projects);
        // TODO: randomly also fail flushes
        // Block on the project lease CAS for the project that is being deleted to assert that the flush happened
        final var projectLeaseReleaseStarted = new CountDownLatch(1);
        final var continueProjectLeaseRelease = new CountDownLatch(1);
        setNodeRepositoryStrategy(indexNode1, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerCompareAndSetRegister(
                Runnable originalRunnable,
                OperationPurpose purpose,
                String key,
                BytesReference expected,
                BytesReference updated,
                ActionListener<Boolean> listener
            ) {
                if (key.equals(ProjectLease.leaseBlobName(deletingProject))) {
                    logger.info("--> Received CAS for [{}]", key);
                    projectLeaseReleaseStarted.countDown();
                    safeAwait(continueProjectLeaseRelease);
                }
                originalRunnable.run();
            }
        });
        markProjectForDeletion(deletingProject);
        safeAwait(projectLeaseReleaseStarted);

        assertBusy(() -> {
            for (ShardId shardId : allShards) {
                final var projectId = internalCluster().clusterService().state().metadata().projectFor(shardId.getIndex()).id();
                final long currentGeneration = getSegmentGeneration(shardId);
                final long previousGeneration = shardSegmentGenerationsBeforeFlush.get(shardId);
                if (projectId.equals(deletingProject)) {
                    assertThat(currentGeneration, equalTo(previousGeneration + 1));
                } else {
                    assertThat(currentGeneration, equalTo(previousGeneration));
                }
            }
        });

        continueProjectLeaseRelease.countDown();
        for (ProjectId project : projects) {
            if (project.equals(deletingProject)) {
                ensureProjectRemovedAndCleanUp(project);
            } else {
                removeProject(project);
            }
        }
    }

    private long getSegmentGeneration(ShardId shardId) {
        final var indexShard = findIndexShard(shardId.getIndex(), shardId.getId());
        return indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
    }

    private void createIndex(ProjectId projectId, String indexName, Settings indexSettings) {
        ElasticsearchAssertions.assertAcked(
            client().projectClient(projectId).admin().indices().prepareCreate(indexName).setSettings(indexSettings)
        );
    }

    private BulkResponse indexDocs(ProjectId projectId, String indexName, int numDocs) {
        var client = client().projectClient(projectId);
        var bulkRequest = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            var indexRequest = client.prepareIndex(indexName);
            bulkRequest.add(indexRequest.setSource(Map.of("field", randomUnicodeOfCodepointLengthBetween(1, 25))));
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);
        return bulkResponse;
    }
}
