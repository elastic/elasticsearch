/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.admin.cluster.RestClusterStateAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.TestCustomMetadata;
import org.elasticsearch.test.disruption.BusyMasterServiceDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class DedicatedClusterSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    public static class TestCustomMetadataPlugin extends Plugin {

        private final List<NamedWriteableRegistry.Entry> namedWritables = new ArrayList<>();
        private final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();

        public TestCustomMetadataPlugin() {
            registerBuiltinWritables();
        }

        private <T extends Metadata.Custom> void registerMetadataCustom(String name, Writeable.Reader<T> reader,
                                                                        Writeable.Reader<NamedDiff> diffReader,
                                                                        CheckedFunction<XContentParser, T, IOException> parser) {
            namedWritables.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, name, reader));
            namedWritables.add(new NamedWriteableRegistry.Entry(NamedDiff.class, name, diffReader));
            namedXContents.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(name), parser));
        }

        private void registerBuiltinWritables() {
            registerMetadataCustom(SnapshottableMetadata.TYPE, SnapshottableMetadata::readFrom,
                SnapshottableMetadata::readDiffFrom, SnapshottableMetadata::fromXContent);
            registerMetadataCustom(NonSnapshottableMetadata.TYPE, NonSnapshottableMetadata::readFrom,
                NonSnapshottableMetadata::readDiffFrom, NonSnapshottableMetadata::fromXContent);
            registerMetadataCustom(SnapshottableGatewayMetadata.TYPE, SnapshottableGatewayMetadata::readFrom,
                SnapshottableGatewayMetadata::readDiffFrom, SnapshottableGatewayMetadata::fromXContent);
            registerMetadataCustom(NonSnapshottableGatewayMetadata.TYPE, NonSnapshottableGatewayMetadata::readFrom,
                NonSnapshottableGatewayMetadata::readDiffFrom, NonSnapshottableGatewayMetadata::fromXContent);
            registerMetadataCustom(SnapshotableGatewayNoApiMetadata.TYPE, SnapshotableGatewayNoApiMetadata::readFrom,
                NonSnapshottableGatewayMetadata::readDiffFrom, SnapshotableGatewayNoApiMetadata::fromXContent);
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return namedWritables;
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return namedXContents;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, TestCustomMetadataPlugin.class, BrokenSettingPlugin.class);
    }

    public static class BrokenSettingPlugin extends Plugin {
        private static boolean breakSetting = false;
        private static final IllegalArgumentException EXCEPTION =  new IllegalArgumentException("this setting goes boom");

        static void breakSetting(boolean breakSetting) {
            BrokenSettingPlugin.breakSetting = breakSetting;
        }

        static final Setting<String> BROKEN_SETTING = new Setting<>("setting.broken", "default", s->s,
                s-> {
                    if ((s.equals("default") == false && breakSetting)) {
                        throw EXCEPTION;
                    }
                },
                Setting.Property.NodeScope, Setting.Property.Dynamic);

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(BROKEN_SETTING);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/37485")
    public void testExceptionWhenRestoringPersistentSettings() {
        logger.info("--> start 2 nodes");
        internalCluster().startNodes(2);

        Client client = client();
        Consumer<String> setSettingValue = value -> {
            client.admin().cluster().prepareUpdateSettings().setPersistentSettings(
                    Settings.builder()
                            .put(BrokenSettingPlugin.BROKEN_SETTING.getKey(), value))
                    .execute().actionGet();
        };

        Consumer<String> assertSettingValue = value -> {
            assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                            .getMetadata().persistentSettings().get(BrokenSettingPlugin.BROKEN_SETTING.getKey()),
                    equalTo(value));
        };

        logger.info("--> set test persistent setting");
        setSettingValue.accept("new value");
        assertSettingValue.accept("new value");

        createRepository("test-repo", "fs", randomRepoPath());

        logger.info("--> start snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet()
                .getSnapshots("test-repo").get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> change the test persistent setting and break it");
        setSettingValue.accept("new value 2");
        assertSettingValue.accept("new value 2");
        BrokenSettingPlugin.breakSetting(true);

        logger.info("--> restore snapshot");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true)
                .setWaitForCompletion(true).execute().actionGet();

        } catch (IllegalArgumentException ex) {
            assertEquals(BrokenSettingPlugin.EXCEPTION.getMessage(), ex.getMessage());
        }

        assertSettingValue.accept("new value 2");
    }

    public void testRestoreCustomMetadata() throws Exception {
        Path tempDir = randomRepoPath();

        logger.info("--> start node");
        internalCluster().startNode();
        Client client = client();
        createIndex("test-idx");
        logger.info("--> add custom persistent metadata");
        updateClusterState(currentState -> {
            ClusterState.Builder builder = ClusterState.builder(currentState);
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            metadataBuilder.putCustom(SnapshottableMetadata.TYPE, new SnapshottableMetadata("before_snapshot_s"));
            metadataBuilder.putCustom(NonSnapshottableMetadata.TYPE, new NonSnapshottableMetadata("before_snapshot_ns"));
            metadataBuilder.putCustom(SnapshottableGatewayMetadata.TYPE, new SnapshottableGatewayMetadata("before_snapshot_s_gw"));
            metadataBuilder.putCustom(NonSnapshottableGatewayMetadata.TYPE, new NonSnapshottableGatewayMetadata("before_snapshot_ns_gw"));
            metadataBuilder.putCustom(SnapshotableGatewayNoApiMetadata.TYPE,
                new SnapshotableGatewayNoApiMetadata("before_snapshot_s_gw_noapi"));
            builder.metadata(metadataBuilder);
            return builder.build();
        });

        createRepository("test-repo", "fs", tempDir);

        logger.info("--> start snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().successfulShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet()
                .getSnapshots("test-repo").get(0).state(),
            equalTo(SnapshotState.SUCCESS));

        logger.info("--> change custom persistent metadata");
        updateClusterState(currentState -> {
            ClusterState.Builder builder = ClusterState.builder(currentState);
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            if (randomBoolean()) {
                metadataBuilder.putCustom(SnapshottableMetadata.TYPE, new SnapshottableMetadata("after_snapshot_s"));
            } else {
                metadataBuilder.removeCustom(SnapshottableMetadata.TYPE);
            }
            metadataBuilder.putCustom(NonSnapshottableMetadata.TYPE, new NonSnapshottableMetadata("after_snapshot_ns"));
            if (randomBoolean()) {
                metadataBuilder.putCustom(SnapshottableGatewayMetadata.TYPE, new SnapshottableGatewayMetadata("after_snapshot_s_gw"));
            } else {
                metadataBuilder.removeCustom(SnapshottableGatewayMetadata.TYPE);
            }
            metadataBuilder.putCustom(NonSnapshottableGatewayMetadata.TYPE, new NonSnapshottableGatewayMetadata("after_snapshot_ns_gw"));
            metadataBuilder.removeCustom(SnapshotableGatewayNoApiMetadata.TYPE);
            builder.metadata(metadataBuilder);
            return builder.build();
        });

        logger.info("--> delete repository");
        assertAcked(client.admin().cluster().prepareDeleteRepository("test-repo"));

        createRepository("test-repo-2", "fs", tempDir);

        logger.info("--> restore snapshot");
        client.admin().cluster().prepareRestoreSnapshot("test-repo-2", "test-snap").setRestoreGlobalState(true).setIndices("-*")
            .setWaitForCompletion(true).execute().actionGet();

        logger.info("--> make sure old repository wasn't restored");
        assertRequestBuilderThrows(client.admin().cluster().prepareGetRepositories("test-repo"), RepositoryMissingException.class);
        assertThat(client.admin().cluster().prepareGetRepositories("test-repo-2").get().repositories().size(), equalTo(1));

        logger.info("--> check that custom persistent metadata was restored");
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);
        Metadata metadata = clusterState.getMetadata();
        assertThat(((SnapshottableMetadata) metadata.custom(SnapshottableMetadata.TYPE)).getData(), equalTo("before_snapshot_s"));
        assertThat(((NonSnapshottableMetadata) metadata.custom(NonSnapshottableMetadata.TYPE)).getData(), equalTo("after_snapshot_ns"));
        assertThat(((SnapshottableGatewayMetadata) metadata.custom(SnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw"));
        assertThat(((NonSnapshottableGatewayMetadata) metadata.custom(NonSnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("after_snapshot_ns_gw"));

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        logger.info("--> check that gateway-persistent custom metadata survived full cluster restart");
        clusterState = client().admin().cluster().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);
        metadata = clusterState.getMetadata();
        assertThat(metadata.custom(SnapshottableMetadata.TYPE), nullValue());
        assertThat(metadata.custom(NonSnapshottableMetadata.TYPE), nullValue());
        assertThat(((SnapshottableGatewayMetadata) metadata.custom(SnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw"));
        assertThat(((NonSnapshottableGatewayMetadata) metadata.custom(NonSnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("after_snapshot_ns_gw"));
        // Shouldn't be returned as part of API response
        assertThat(metadata.custom(SnapshotableGatewayNoApiMetadata.TYPE), nullValue());
        // But should still be in state
        metadata = internalCluster().getInstance(ClusterService.class).state().metadata();
        assertThat(((SnapshotableGatewayNoApiMetadata) metadata.custom(SnapshotableGatewayNoApiMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw_noapi"));
    }

    private void updateClusterState(final ClusterStateUpdater updater) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return updater.execute(currentState);
            }

            @Override
            public void onFailure(String source, @Nullable Exception e) {
                countDownLatch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
    }

    private interface ClusterStateUpdater {
        ClusterState execute(ClusterState currentState) throws Exception;
    }

    public void testSnapshotDuringNodeShutdown() throws Exception {
        logger.info("--> start 2 nodes");
        Client client = client();

        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_shards", 2)
                                                                   .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> creating repository");
        AcknowledgedResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", randomRepoPath())
                                .put("random", randomAlphaOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                                .setWaitForCompletion(false)
                                .setIndices("test-idx")
                                .get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], shutting it down", blockedNode);
        unblockNode("test-repo", blockedNode);

        logger.info("--> stopping node [{}]", blockedNode);
        stopNode(blockedNode);
        logger.info("--> waiting for completion");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(60));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");
    }

    public void testSnapshotWithStuckNode() throws Exception {
        logger.info("--> start 2 nodes");
        ArrayList<String> nodes = new ArrayList<>();
        nodes.add(internalCluster().startNode());
        nodes.add(internalCluster().startNode());
        Client client = client();

        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_shards", 2)
                                                                   .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> creating repository");
        Path repo = randomRepoPath();
        AcknowledgedResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", repo)
                                .put("random", randomAlphaOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");
        // Remove it from the list of available nodes
        nodes.remove(blockedNode);

        assertFileCount(repo, 0);
        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                                .setWaitForCompletion(false)
                                .setIndices("test-idx")
                                .get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], aborting snapshot", blockedNode);

        ActionFuture<AcknowledgedResponse> deleteSnapshotResponseFuture = internalCluster().client(nodes.get(0))
            .admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap").execute();
        // Make sure that abort makes some progress
        Thread.sleep(100);
        unblockNode("test-repo", blockedNode);
        logger.info("--> stopping node [{}]", blockedNode);
        stopNode(blockedNode);
        try {
            AcknowledgedResponse deleteSnapshotResponse = deleteSnapshotResponseFuture.actionGet();
            assertThat(deleteSnapshotResponse.isAcknowledged(), equalTo(true));
        } catch (SnapshotMissingException ex) {
            // When master node is closed during this test, it sometime manages to delete the snapshot files before
            // completely stopping. In this case the retried delete snapshot operation on the new master can fail
            // with SnapshotMissingException
        }

        logger.info("--> making sure that snapshot no longer exists");
        expectThrows(SnapshotMissingException.class,
            () -> client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap")
                .execute().actionGet().getSnapshots("test-repo"));

        logger.info("--> Go through a loop of creating and deleting a snapshot to trigger repository cleanup");
        client().admin().cluster().prepareCleanupRepository("test-repo").get();

        // Expect two files to remain in the repository:
        //   (1) index-(N+1)
        //   (2) index-latest
        assertFileCount(repo, 2);
        logger.info("--> done");
    }

    public void testRestoreIndexWithMissingShards() throws Exception {
        disableRepoConsistencyCheck("This test leaves behind a purposely broken repository");
        logger.info("--> start 2 nodes");
        internalCluster().startNode();
        internalCluster().startNode();
        cluster().wipeIndices("_all");

        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx-some", 2, Settings.builder().put("number_of_shards", 6)
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data into test-idx-some");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx-some", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareSearch("test-idx-some").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> shutdown one of the nodes");
        internalCluster().stopRandomDataNode();
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForNodes("<2")
                .execute().actionGet().isTimedOut(),
            equalTo(false));

        logger.info("--> create an index that will have all allocated shards");
        assertAcked(prepareCreate("test-idx-all", 1, Settings.builder().put("number_of_shards", 6)
                .put("number_of_replicas", 0)));
        ensureGreen("test-idx-all");

        logger.info("--> create an index that will be closed");
        assertAcked(prepareCreate("test-idx-closed", 1, Settings.builder().put("number_of_shards", 4)
                                                                          .put("number_of_replicas", 0)));
        logger.info("--> indexing some data into test-idx-all");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx-all", Integer.toString(i), "foo", "bar" + i);
            indexDoc("test-idx-closed", Integer.toString(i), "foo", "bar" + i);
        }
        refresh("test-idx-closed", "test-idx-all"); // don't refresh test-idx-some it will take 30 sec until it times out...
        assertThat(client().prepareSearch("test-idx-all").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));
        assertThat(client().prepareSearch("test-idx-closed").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));
        assertAcked(client().admin().indices().prepareClose("test-idx-closed"));

        logger.info("--> create an index that will have no allocated shards");
        assertAcked(prepareCreate("test-idx-none", 1, Settings.builder().put("number_of_shards", 6)
                .put("index.routing.allocation.include.tag", "nowhere")
                .put("number_of_replicas", 0)).setWaitForActiveShards(ActiveShardCount.NONE).get());
        assertTrue(indexExists("test-idx-none"));

        createRepository("test-repo", "fs", randomRepoPath());

        logger.info("--> start snapshot with default settings without a closed index - should fail");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.FAILED));
        assertThat(createSnapshotResponse.getSnapshotInfo().reason(), containsString("Indices don't have primary shards"));

        if (randomBoolean()) {
            logger.info("checking snapshot completion using status");
            client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2")
                    .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                    .setWaitForCompletion(false).setPartial(true).execute().actionGet();
            assertBusy(() -> {
                SnapshotsStatusResponse snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus("test-repo")
                    .setSnapshots("test-snap-2").get();
                List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
                assertEquals(snapshotStatuses.size(), 1);
                logger.trace("current snapshot status [{}]", snapshotStatuses.get(0));
                assertTrue(snapshotStatuses.get(0).getState().completed());
            }, 1, TimeUnit.MINUTES);
            SnapshotsStatusResponse snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus("test-repo")
                .setSnapshots("test-snap-2").get();
            List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
            assertThat(snapshotStatuses.size(), equalTo(1));
            SnapshotStatus snapshotStatus = snapshotStatuses.get(0);
            logger.info("State: [{}], Reason: [{}]",
                createSnapshotResponse.getSnapshotInfo().state(), createSnapshotResponse.getSnapshotInfo().reason());
            assertThat(snapshotStatus.getShardsStats().getTotalShards(), equalTo(22));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), lessThan(16));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), greaterThan(10));

            // There is slight delay between snapshot being marked as completed in the cluster state and on the file system
            // After it was marked as completed in the cluster state - we need to check if it's completed on the file system as well
            assertBusy(() -> {
                GetSnapshotsResponse response = client().admin().cluster().prepareGetSnapshots("test-repo")
                    .setSnapshots("test-snap-2").get();
                assertThat(response.getSnapshots("test-repo").size(), equalTo(1));
                SnapshotInfo snapshotInfo = response.getSnapshots("test-repo").get(0);
                assertTrue(snapshotInfo.state().completed());
                assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
            }, 1, TimeUnit.MINUTES);
        } else {
            logger.info("checking snapshot completion using wait_for_completion flag");
            createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2")
                    .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                    .setWaitForCompletion(true).setPartial(true).execute().actionGet();
            logger.info("State: [{}], Reason: [{}]",
                createSnapshotResponse.getSnapshotInfo().state(), createSnapshotResponse.getSnapshotInfo().reason());
            assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(22));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), lessThan(16));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(10));
            assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-2").execute().actionGet()
                    .getSnapshots("test-repo").get(0).state(),
                equalTo(SnapshotState.PARTIAL));
        }

        assertAcked(client().admin().indices().prepareClose("test-idx-all"));

        logger.info("--> restore incomplete snapshot - should fail");
        assertFutureThrows(client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2").setRestoreGlobalState(false)
                .setWaitForCompletion(true).execute(),
            SnapshotRestoreException.class);

        logger.info("--> restore snapshot for the index that was snapshotted completely");
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false).setIndices("test-idx-all").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        assertThat(client().prepareSearch("test-idx-all").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> restore snapshot for the partial index");
        cluster().wipeIndices("test-idx-some");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2")
                .setRestoreGlobalState(false).setIndices("test-idx-some").setPartial(true).setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), allOf(greaterThan(0), lessThan(6)));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), greaterThan(0));

        assertThat(client().prepareSearch("test-idx-some").setSize(0).get().getHits().getTotalHits().value, allOf(greaterThan(0L),
            lessThan(100L)));

        logger.info("--> restore snapshot for the index that didn't have any shards snapshotted successfully");
        cluster().wipeIndices("test-idx-none");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2")
                .setRestoreGlobalState(false).setIndices("test-idx-none").setPartial(true).setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(6));

        assertThat(client().prepareSearch("test-idx-some").setSize(0).get().getHits().getTotalHits().value, allOf(greaterThan(0L),
            lessThan(100L)));

        logger.info("--> restore snapshot for the closed index that was snapshotted completely");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false).setIndices("test-idx-closed").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(4));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(4));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        assertThat(client().prepareSearch("test-idx-closed").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));
    }

    public void testRestoreIndexWithShardsMissingInLocalGateway() throws Exception {
        logger.info("--> start 2 nodes");
        Settings nodeSettings = Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
                .build();

        internalCluster().startNodes(2, nodeSettings);
        cluster().wipeIndices("_all");

        createRepository("test-repo", "fs", randomRepoPath());

        int numberOfShards = 6;
        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_shards", numberOfShards)
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data into test-idx");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> start snapshot");
        assertThat(client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setIndices("test-idx")
                .setWaitForCompletion(true).get().getSnapshotInfo().state(),
            equalTo(SnapshotState.SUCCESS));

        logger.info("--> close the index");
        assertAcked(client().admin().indices().prepareClose("test-idx"));

        logger.info("--> shutdown one of the nodes that should make half of the shards unavailable");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForNodes("2")
                .execute().actionGet().isTimedOut(),
            equalTo(false));

        logger.info("--> restore index snapshot");
        assertThat(client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-1").setRestoreGlobalState(false)
                .setWaitForCompletion(true).get().getRestoreInfo().successfulShards(),
            equalTo(6));

        ensureGreen("test-idx");
        assertThat(client().prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        IntSet reusedShards = new IntHashSet();
        List<RecoveryState> recoveryStates = client().admin().indices().prepareRecoveries("test-idx").get()
            .shardRecoveryStates().get("test-idx");
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getIndex().reusedBytes() > 0) {
                reusedShards.add(recoveryState.getShardId().getId());
            }
        }
        logger.info("--> check that at least half of the shards had some reuse: [{}]", reusedShards);
        assertThat(reusedShards.size(), greaterThanOrEqualTo(numberOfShards / 2));
    }

    public void testRegistrationFailure() {
        disableRepoConsistencyCheck("This test does not create any data in the repository");
        logger.info("--> start first node");
        internalCluster().startNode();
        logger.info("--> start second node");
        // Make sure the first node is elected as master
        internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false));
        // Register mock repositories
        for (int i = 0; i < 5; i++) {
            client().admin().cluster().preparePutRepository("test-repo" + i)
                    .setType("mock").setSettings(Settings.builder()
                    .put("location", randomRepoPath())).setVerify(false).get();
        }
        logger.info("--> make sure that properly setup repository can be registered on all nodes");
        client().admin().cluster().preparePutRepository("test-repo-0")
                .setType("fs").setSettings(Settings.builder()
                .put("location", randomRepoPath())).get();

    }

    public void testThatSensitiveRepositorySettingsAreNotExposed() throws Exception {
        disableRepoConsistencyCheck("This test does not create any data in the repository");
        Settings nodeSettings = Settings.EMPTY;
        logger.info("--> start two nodes");
        internalCluster().startNodes(2, nodeSettings);
        // Register mock repositories
        client().admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put(MockRepository.Plugin.USERNAME_SETTING.getKey(), "notsecretusername")
                        .put(MockRepository.Plugin.PASSWORD_SETTING.getKey(), "verysecretpassword")
        ).get();

        NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);
        RestGetRepositoriesAction getRepoAction = new RestGetRepositoriesAction(internalCluster().getInstance(SettingsFilter.class));
        RestRequest getRepoRequest = new FakeRestRequest();
        getRepoRequest.params().put("repository", "test-repo");
        final CountDownLatch getRepoLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> getRepoError = new AtomicReference<>();
        getRepoAction.handleRequest(getRepoRequest, new AbstractRestChannel(getRepoRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    assertThat(response.content().utf8ToString(), containsString("notsecretusername"));
                    assertThat(response.content().utf8ToString(), not(containsString("verysecretpassword")));
                } catch (AssertionError ex) {
                    getRepoError.set(ex);
                }
                getRepoLatch.countDown();
            }
        }, nodeClient);
        assertTrue(getRepoLatch.await(1, TimeUnit.SECONDS));
        if (getRepoError.get() != null) {
            throw getRepoError.get();
        }

        RestClusterStateAction clusterStateAction = new RestClusterStateAction(internalCluster().getInstance(SettingsFilter.class));
        RestRequest clusterStateRequest = new FakeRestRequest();
        final CountDownLatch clusterStateLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> clusterStateError = new AtomicReference<>();
        clusterStateAction.handleRequest(clusterStateRequest, new AbstractRestChannel(clusterStateRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    assertThat(response.content().utf8ToString(), containsString("notsecretusername"));
                    assertThat(response.content().utf8ToString(), not(containsString("verysecretpassword")));
                } catch (AssertionError ex) {
                    clusterStateError.set(ex);
                }
                clusterStateLatch.countDown();
            }
        }, nodeClient);
        assertTrue(clusterStateLatch.await(1, TimeUnit.SECONDS));
        if (clusterStateError.get() != null) {
            throw clusterStateError.get();
        }
    }

    public void testMasterShutdownDuringSnapshot() throws Exception {
        logger.info("-->  starting two master nodes and two data nodes");
        internalCluster().startMasterOnlyNodes(2);
        internalCluster().startDataOnlyNodes(2);

        final Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        assertAcked(prepareCreate("test-idx", 0, Settings.builder().put("number_of_shards", between(1, 20))
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx").setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();

        final int numberOfShards = getNumShards("test-idx").numPrimaries;
        logger.info("number of shards: {}", numberOfShards);

        dataNodeClient().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false)
            .setIndices("test-idx").get();

        logger.info("--> stopping master node");
        internalCluster().stopCurrentMasterNode();

        logger.info("--> wait until the snapshot is done");

        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster().prepareGetSnapshots("test-repo")
                .setSnapshots("test-snap").get();
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots("test-repo").get(0);
            assertTrue(snapshotInfo.state().completed());
        }, 1, TimeUnit.MINUTES);

        logger.info("--> verify that snapshot was succesful");

        GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster().prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap").get();
        SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots("test-repo").get(0);
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testMasterAndDataShutdownDuringSnapshot() throws Exception {
        logger.info("-->  starting three master nodes and two data nodes");
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNodes(2);

        final Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
            .setType("mock").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        assertAcked(prepareCreate("test-idx", 0, Settings.builder().put("number_of_shards", between(1, 20))
            .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx").setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();

        final int numberOfShards = getNumShards("test-idx").numPrimaries;
        logger.info("number of shards: {}", numberOfShards);

        final String masterNode = blockMasterFromFinalizingSnapshotOnSnapFile("test-repo");
        final String dataNode = blockNodeWithIndex("test-repo", "test-idx");

        dataNodeClient().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false)
            .setIndices("test-idx").get();

        logger.info("--> stopping data node {}", dataNode);
        stopNode(dataNode);
        logger.info("--> stopping master node {} ", masterNode);
        internalCluster().stopCurrentMasterNode();

        logger.info("--> wait until the snapshot is done");

        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster().prepareGetSnapshots("test-repo")
                .setSnapshots("test-snap").get();
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots("test-repo").get(0);
            assertTrue(snapshotInfo.state().completed());
        }, 1, TimeUnit.MINUTES);

        logger.info("--> verify that snapshot was partial");

        GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster().prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap").get();
        SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots("test-repo").get(0);
        assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
        assertNotEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertThat(snapshotInfo.failedShards(), greaterThan(0));
        for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
            assertNotNull(failure.reason());
        }
    }

    public void testMasterShutdownDuringFailedSnapshot() throws Exception {
        logger.info("-->  starting two master nodes and two data nodes");
        internalCluster().startMasterOnlyNodes(2);
        internalCluster().startDataOnlyNodes(2);

        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
            .setType("mock").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        assertAcked(prepareCreate("test-idx", 0, Settings.builder()
            .put("number_of_shards", 6).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("-->  indexing some data");
        final int numdocs = randomIntBetween(50, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx").setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();

        logger.info("-->  stopping random data node, which should cause shards to go missing");
        internalCluster().stopRandomDataNode();
        assertBusy(() ->
            assertEquals(ClusterHealthStatus.RED, client().admin().cluster().prepareHealth().get().getStatus()),
            30, TimeUnit.SECONDS);

        final String masterNode = blockMasterFromFinalizingSnapshotOnIndexFile("test-repo");

        logger.info("-->  snapshot");
        client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in on " + masterNode);
        waitForBlock(masterNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("-->  stopping master node");
        internalCluster().stopCurrentMasterNode();

        logger.info("-->  wait until the snapshot is done");
        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster()
                .prepareGetSnapshots("test-repo").setSnapshots("test-snap").setIgnoreUnavailable(true).get();
            assertEquals(1, snapshotsStatusResponse.getSnapshots("test-repo").size());
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots("test-repo").get(0);
            assertTrue(snapshotInfo.state().completed());
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            SnapshotsInProgress snapshotsInProgress = clusterState.custom(SnapshotsInProgress.TYPE);
            assertEquals(0, snapshotsInProgress.entries().size());
        }, 30, TimeUnit.SECONDS);

        logger.info("-->  verify that snapshot failed");
        GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster()
            .prepareGetSnapshots("test-repo").setSnapshots("test-snap").get();
        SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots("test-repo").get(0);
        assertEquals(SnapshotState.FAILED, snapshotInfo.state());
    }

    /**
     * Tests that a shrunken index (created via the shrink APIs) and subsequently snapshotted
     * can be restored when the node the shrunken index was created on is no longer part of
     * the cluster.
     */
    public void testRestoreShrinkIndex() throws Exception {
        logger.info("-->  starting a master node and a data node");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final Client client = client();
        final String repo = "test-repo";
        final String snapshot = "test-snap";
        final String sourceIdx = "test-idx";
        final String shrunkIdx = "test-idx-shrunk";

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository(repo).setType("fs")
            .setSettings(Settings.builder().put("location", randomRepoPath())
                             .put("compress", randomBoolean())));

        assertAcked(prepareCreate(sourceIdx, 0, Settings.builder()
            .put("number_of_shards", between(2, 10)).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomIntBetween(10, 100)];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(sourceIdx).setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();

        logger.info("--> shrink the index");
        assertAcked(client.admin().indices().prepareUpdateSettings(sourceIdx)
            .setSettings(Settings.builder().put("index.blocks.write", true)).get());
        assertAcked(client.admin().indices().prepareResizeIndex(sourceIdx, shrunkIdx).get());

        logger.info("--> snapshot the shrunk index");
        CreateSnapshotResponse createResponse = client.admin().cluster()
            .prepareCreateSnapshot(repo, snapshot)
            .setWaitForCompletion(true).setIndices(shrunkIdx).get();
        assertEquals(SnapshotState.SUCCESS, createResponse.getSnapshotInfo().state());

        logger.info("--> delete index and stop the data node");
        assertAcked(client.admin().indices().prepareDelete(sourceIdx).get());
        assertAcked(client.admin().indices().prepareDelete(shrunkIdx).get());
        internalCluster().stopRandomDataNode();
        client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForNodes("1");

        logger.info("--> start a new data node");
        final Settings dataSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLength(5))
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()) // to get a new node id
            .build();
        internalCluster().startDataOnlyNode(dataSettings);
        client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForNodes("2");

        logger.info("--> restore the shrunk index and ensure all shards are allocated");
        RestoreSnapshotResponse restoreResponse = client().admin().cluster()
            .prepareRestoreSnapshot(repo, snapshot).setWaitForCompletion(true)
            .setIndices(shrunkIdx).get();
        assertEquals(restoreResponse.getRestoreInfo().totalShards(),
            restoreResponse.getRestoreInfo().successfulShards());
        ensureYellow();
    }

    public void testSnapshotWithDateMath() {
        final String repo = "repo";
        final AdminClient admin = client().admin();

        final IndexNameExpressionResolver nameExpressionResolver = new IndexNameExpressionResolver();
        final String snapshotName = "<snapshot-{now/d}>";

        logger.info("-->  creating repository");
        assertAcked(admin.cluster().preparePutRepository(repo).setType("fs")
            .setSettings(Settings.builder().put("location", randomRepoPath())
                .put("compress", randomBoolean())));

        final String expression1 = nameExpressionResolver.resolveDateMathExpression(snapshotName);
        logger.info("-->  creating date math snapshot");
        CreateSnapshotResponse snapshotResponse =
            admin.cluster().prepareCreateSnapshot(repo, snapshotName)
                .setIncludeGlobalState(true)
                .setWaitForCompletion(true)
                .get();
        assertThat(snapshotResponse.status(), equalTo(RestStatus.OK));
        // snapshot could be taken before or after a day rollover
        final String expression2 = nameExpressionResolver.resolveDateMathExpression(snapshotName);

        SnapshotsStatusResponse response = admin.cluster().prepareSnapshotStatus(repo)
            .setSnapshots(Sets.newHashSet(expression1, expression2).toArray(Strings.EMPTY_ARRAY))
            .setIgnoreUnavailable(true)
            .get();
        List<SnapshotStatus> snapshots = response.getSnapshots();
        assertThat(snapshots, hasSize(1));
        assertThat(snapshots.get(0).getState().completed(), equalTo(true));
    }

    public void testSnapshotTotalAndIncrementalSizes() throws IOException {
        Client client = client();
        final String indexName = "test-blocks-1";
        final String repositoryName = "repo-" + indexName;
        final String snapshot0 = "snapshot-0";
        final String snapshot1 = "snapshot-1";

        createIndex(indexName);

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client.prepareIndex(indexName).setSource("test", "init").execute().actionGet();
        }

        final Path repoPath = randomRepoPath();
        createRepository(repositoryName, "fs", repoPath);

        logger.info("--> create a snapshot");
        client.admin().cluster().prepareCreateSnapshot(repositoryName, snapshot0)
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .get();

        SnapshotsStatusResponse response = client.admin().cluster().prepareSnapshotStatus(repositoryName)
            .setSnapshots(snapshot0)
            .get();

        List<SnapshotStatus> snapshots = response.getSnapshots();

        List<Path> snapshot0Files = scanSnapshotFolder(repoPath);
        assertThat(snapshots, hasSize(1));

        final int snapshot0FileCount = snapshot0Files.size();
        final long snapshot0FileSize = calculateTotalFilesSize(snapshot0Files);

        SnapshotStats stats = snapshots.get(0).getStats();

        assertThat(stats.getTotalFileCount(), greaterThanOrEqualTo(snapshot0FileCount));
        assertThat(stats.getTotalSize(), greaterThanOrEqualTo(snapshot0FileSize));

        assertThat(stats.getIncrementalFileCount(), equalTo(stats.getTotalFileCount()));
        assertThat(stats.getIncrementalSize(), equalTo(stats.getTotalSize()));

        assertThat(stats.getIncrementalFileCount(), equalTo(stats.getProcessedFileCount()));
        assertThat(stats.getIncrementalSize(), equalTo(stats.getProcessedSize()));

        // add few docs - less than initially
        docs = between(1, 5);
        for (int i = 0; i < docs; i++) {
            client.prepareIndex(indexName).setSource("test", "test" + i).execute().actionGet();
        }

        // create another snapshot
        // total size has to grow and has to be equal to files on fs
        assertThat(client.admin().cluster()
                .prepareCreateSnapshot(repositoryName, snapshot1)
                .setWaitForCompletion(true).get().status(),
            equalTo(RestStatus.OK));

        //  drop 1st one to avoid miscalculation as snapshot reuses some files of prev snapshot
        assertTrue(client.admin().cluster()
            .prepareDeleteSnapshot(repositoryName, snapshot0)
            .get().isAcknowledged());

        response = client.admin().cluster().prepareSnapshotStatus(repositoryName)
            .setSnapshots(snapshot1)
            .get();

        final List<Path> snapshot1Files = scanSnapshotFolder(repoPath);

        final int snapshot1FileCount = snapshot1Files.size();
        final long snapshot1FileSize = calculateTotalFilesSize(snapshot1Files);

        snapshots = response.getSnapshots();

        SnapshotStats anotherStats = snapshots.get(0).getStats();

        ArrayList<Path> snapshotFilesDiff = new ArrayList<>(snapshot1Files);
        snapshotFilesDiff.removeAll(snapshot0Files);

        assertThat(anotherStats.getIncrementalFileCount(), greaterThanOrEqualTo(snapshotFilesDiff.size()));
        assertThat(anotherStats.getIncrementalSize(), greaterThanOrEqualTo(calculateTotalFilesSize(snapshotFilesDiff)));

        assertThat(anotherStats.getIncrementalFileCount(), equalTo(anotherStats.getProcessedFileCount()));
        assertThat(anotherStats.getIncrementalSize(), equalTo(anotherStats.getProcessedSize()));

        assertThat(stats.getTotalSize(), lessThan(anotherStats.getTotalSize()));
        assertThat(stats.getTotalFileCount(), lessThan(anotherStats.getTotalFileCount()));

        assertThat(anotherStats.getTotalFileCount(), greaterThanOrEqualTo(snapshot1FileCount));
        assertThat(anotherStats.getTotalSize(), greaterThanOrEqualTo(snapshot1FileSize));
    }

    public void testDataNodeRestartWithBusyMasterDuringSnapshot() throws Exception {
        logger.info("-->  starting a master node and two data nodes");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
            .setType("mock").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        assertAcked(prepareCreate("test-idx", 0, Settings.builder()
            .put("number_of_shards", 5).put("number_of_replicas", 0)));
        ensureGreen();
        logger.info("-->  indexing some data");
        final int numdocs = randomIntBetween(50, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx").setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();
        final String dataNode = blockNodeWithIndex("test-repo", "test-idx");
        logger.info("-->  snapshot");
        ServiceDisruptionScheme disruption = new BusyMasterServiceDisruption(random(), Priority.HIGH);
        setDisruptionScheme(disruption);
        client(internalCluster().getMasterName()).admin().cluster()
            .prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();
        disruption.startDisrupting();
        logger.info("-->  restarting data node, which should cause primary shards to be failed");
        internalCluster().restartNode(dataNode, InternalTestCluster.EMPTY_CALLBACK);

        logger.info("-->  wait for shard snapshots to show as failed");
        assertBusy(() -> assertThat(
            client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-snap").get().getSnapshots()
                .get(0).getShardsStats().getFailedShards(), greaterThanOrEqualTo(1)), 60L, TimeUnit.SECONDS);

        unblockNode("test-repo", dataNode);
        disruption.stopDisrupting();
        // check that snapshot completes
        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster()
                .prepareGetSnapshots("test-repo").setSnapshots("test-snap").setIgnoreUnavailable(true).get();
            assertEquals(1, snapshotsStatusResponse.getSnapshots("test-repo").size());
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots("test-repo").get(0);
            assertTrue(snapshotInfo.state().toString(), snapshotInfo.state().completed());
        }, 60L, TimeUnit.SECONDS);
    }

    public void testDataNodeRestartAfterShardSnapshotFailure() throws Exception {
        logger.info("-->  starting a master node and two data nodes");
        internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
            .setType("mock").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        assertAcked(prepareCreate("test-idx", 0, Settings.builder()
            .put("number_of_shards", 2).put("number_of_replicas", 0)));
        ensureGreen();
        logger.info("-->  indexing some data");
        final int numdocs = randomIntBetween(50, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx").setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();
        blockAllDataNodes("test-repo");
        logger.info("-->  snapshot");
        client(internalCluster().getMasterName()).admin().cluster()
            .prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();
        logger.info("-->  restarting first data node, which should cause the primary shard on it to be failed");
        internalCluster().restartNode(dataNodes.get(0), InternalTestCluster.EMPTY_CALLBACK);

        logger.info("-->  wait for shard snapshot of first primary to show as failed");
        assertBusy(() -> assertThat(
            client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-snap").get().getSnapshots()
                .get(0).getShardsStats().getFailedShards(), is(1)), 60L, TimeUnit.SECONDS);

        logger.info("-->  restarting second data node, which should cause the primary shard on it to be failed");
        internalCluster().restartNode(dataNodes.get(1), InternalTestCluster.EMPTY_CALLBACK);

        // check that snapshot completes with both failed shards being accounted for in the snapshot result
        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster()
                .prepareGetSnapshots("test-repo").setSnapshots("test-snap").setIgnoreUnavailable(true).get();
            assertEquals(1, snapshotsStatusResponse.getSnapshots("test-repo").size());
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots("test-repo").get(0);
            assertTrue(snapshotInfo.state().toString(), snapshotInfo.state().completed());
            assertThat(snapshotInfo.totalShards(), is(2));
            assertThat(snapshotInfo.shardFailures(), hasSize(2));
        }, 60L, TimeUnit.SECONDS);
    }

    public void testRetentionLeasesClearedOnRestore() throws Exception {
        final String repoName = "test-repo-retention-leases";
        assertAcked(client().admin().cluster().preparePutRepository(repoName)
            .setType("fs")
            .setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())));

        final String indexName = "index-retention-leases";
        final int shardCount = randomIntBetween(1, 5);
        assertAcked(client().admin().indices().prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .get());
        final ShardId shardId = new ShardId(resolveIndex(indexName), randomIntBetween(0, shardCount - 1));

        final int snapshotDocCount = iterations(10, 1000);
        logger.debug("--> indexing {} docs into {}", snapshotDocCount, indexName);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[snapshotDocCount];
        for (int i = 0; i < snapshotDocCount; i++) {
            indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", "value");
        }
        indexRandom(true, indexRequestBuilders);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), snapshotDocCount);

        final String leaseId = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        logger.debug("--> adding retention lease with id {} to {}", leaseId, shardId);
        client().execute(RetentionLeaseActions.Add.INSTANCE, new RetentionLeaseActions.AddRequest(
            shardId, leaseId, RETAIN_ALL, "test")).actionGet();

        final ShardStats shardStats = Arrays.stream(client().admin().indices().prepareStats(indexName).get().getShards())
            .filter(s -> s.getShardRouting().shardId().equals(shardId)).findFirst().get();
        final RetentionLeases retentionLeases = shardStats.getRetentionLeaseStats().retentionLeases();
        assertTrue(shardStats + ": " + retentionLeases, retentionLeases.contains(leaseId));

        final String snapshotName = "snapshot-retention-leases";
        logger.debug("-->  create snapshot {}:{}", repoName, snapshotName);
        CreateSnapshotResponse createResponse = client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true).setIndices(indexName).get();
        assertThat(createResponse.getSnapshotInfo().successfulShards(), equalTo(shardCount));
        assertThat(createResponse.getSnapshotInfo().failedShards(), equalTo(0));

        if (randomBoolean()) {
            final int extraDocCount = iterations(10, 1000);
            logger.debug("--> indexing {} extra docs into {}", extraDocCount, indexName);
            indexRequestBuilders = new IndexRequestBuilder[extraDocCount];
            for (int i = 0; i < extraDocCount; i++) {
                indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", "value");
            }
            indexRandom(true, indexRequestBuilders);
        }

        // Wait for green so the close does not fail in the edge case of coinciding with a shard recovery that hasn't fully synced yet
        ensureGreen();
        logger.debug("-->  close index {}", indexName);
        assertAcked(client().admin().indices().prepareClose(indexName));

        logger.debug("--> restore index {} from snapshot", indexName);
        RestoreSnapshotResponse restoreResponse = client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true).get();
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(shardCount));
        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));

        ensureGreen();
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), snapshotDocCount);

        final RetentionLeases restoredRetentionLeases = Arrays.stream(client().admin().indices().prepareStats(indexName).get()
            .getShards()).filter(s -> s.getShardRouting().shardId().equals(shardId)).findFirst().get()
            .getRetentionLeaseStats().retentionLeases();
        assertFalse(restoredRetentionLeases.toString() + " has no " + leaseId, restoredRetentionLeases.contains(leaseId));
    }

    private long calculateTotalFilesSize(List<Path> files) {
        return files.stream().mapToLong(f -> {
            try {
                return Files.size(f);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).sum();
    }

    private List<Path> scanSnapshotFolder(Path repoPath) throws IOException {
        List<Path> files = new ArrayList<>();
        Files.walkFileTree(repoPath, new SimpleFileVisitor<Path>(){
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getFileName().toString().startsWith("__")){
                        files.add(file);
                    }
                    return super.visitFile(file, attrs);
                }
            }
        );
        return files;
    }

    public static class SnapshottableMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable";

        public SnapshottableMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshottableMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshottableMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static SnapshottableMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshottableMetadata::new, parser);
        }


        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_AND_SNAPSHOT;
        }
    }

    public static class NonSnapshottableMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_non_snapshottable";

        public NonSnapshottableMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static NonSnapshottableMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(NonSnapshottableMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static NonSnapshottableMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(NonSnapshottableMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_ONLY;
        }
    }

    public static class SnapshottableGatewayMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable_gateway";

        public SnapshottableGatewayMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshottableGatewayMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshottableGatewayMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static SnapshottableGatewayMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshottableGatewayMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.SNAPSHOT, Metadata.XContentContext.GATEWAY);
        }
    }

    public static class NonSnapshottableGatewayMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_non_snapshottable_gateway";

        public NonSnapshottableGatewayMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static NonSnapshottableGatewayMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(NonSnapshottableGatewayMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static NonSnapshottableGatewayMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(NonSnapshottableGatewayMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_AND_GATEWAY;
        }

    }

    public static class SnapshotableGatewayNoApiMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable_gateway_no_api";

        public SnapshotableGatewayNoApiMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshotableGatewayNoApiMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshotableGatewayNoApiMetadata::new, in);
        }

        public static SnapshotableGatewayNoApiMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshotableGatewayNoApiMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }
    }
}
