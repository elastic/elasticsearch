/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.write.file.TestParquetBytes;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * The loss of a node and its disk with staged durability: every acknowledged row is in the staged files, and the shard that is created
 * again with an empty store (a human allocates an empty primary and accepts the loss of the store) adopts them, so that no
 * acknowledged row is lost.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetFlushIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ParquetNodeLossIT extends ESIntegTestCase {

    private static final String INDEX = "parquet-node-loss";
    private static final String REPOSITORY = "lake";

    private static Path repoRoot;

    @BeforeClass
    public static void createRepoRoot() {
        repoRoot = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Environment.PATH_REPO_SETTING.getKey(), repoRoot)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetWriteEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // the base class searches every shard, and this engine cannot be searched
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    private BulkResponse bulk(int from, int to) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = from; i < to; i++) {
            bulk.add(new IndexRequest(INDEX).source("count", i));
        }
        return bulk.get();
    }

    private IndexShard shard() {
        return internalCluster().getDataNodeInstance(IndicesService.class).indexServiceSafe(resolveIndex(INDEX)).getShard(0);
    }

    /** The table of the index for a history of its shard, as a reader outside the engine opens it: from the repository directory. */
    private ShardTable table(String uuid, String history) throws IOException {
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        return ShardTable.open(
            new FsBlobStore(4096, repoRoot.resolve(REPOSITORY), true),
            new TableLocation(root + "/" + INDEX + "/" + uuid, BlobPath.EMPTY.add(INDEX).add(uuid)),
            0,
            INDEX,
            uuid,
            history,
            null
        );
    }

    /** The values of {@code count} in the data files that the table lists for the history, each once. */
    private TreeSet<Long> listedRows(String uuid, String history) throws IOException {
        final List<Path> files = new ArrayList<>();
        for (ShardTable.UploadedFile file : table(uuid, history).uploadedFiles()) {
            files.add(org.elasticsearch.core.PathUtils.get(java.net.URI.create(file.location())));
        }
        final TreeSet<Long> counts = new TreeSet<>();
        for (Path file : files) {
            for (var row : TestParquetBytes.read(Files.readAllBytes(file)).rows()) {
                assertTrue("count appears once", counts.add(row.getLong("count", 0)));
            }
        }
        return counts;
    }

    private int stagedFileCount(String uuid) throws IOException {
        final Path staging = repoRoot.resolve(REPOSITORY).resolve(INDEX).resolve(uuid).resolve("staging");
        if (Files.exists(staging) == false) {
            return 0;
        }
        try (Stream<Path> files = Files.walk(staging)) {
            return (int) files.filter(f -> f.getFileName().toString().endsWith(".parquet")).count();
        }
    }

    public void testTheRowsOfALostStoreAreAdoptedByTheShardThatReplacesIt() throws Exception {
        // the node that loses its disk must not be the master, which keeps the cluster state
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.parquet.repository", REPOSITORY)
                        .put("index.parquet.durability", "staged")
                        // nothing flushes: every acknowledged row is only in the local files and in the staged files
                        .put("index.parquet.flush_threshold_age", "6h")
                        .put("index.parquet.compaction.enabled", false)
                        .put("index.parquet.compaction.interval", "1s")
                )
                .setMapping("count", "type=long")
        );
        ensureGreen(INDEX);
        final String uuid = resolveIndex(INDEX).getUUID();
        final String oldHistory = shard().getHistoryUUID();
        for (int from = 0; from < 3_000; from += 1_000) {
            final BulkResponse response = bulk(from, from + 1_000);
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
        assertThat(stagedFileCount(uuid), equalTo(3));
        assertThat("nothing is in the table yet", table(uuid, oldHistory).uploadedFiles(), hasSize(0));

        // the node and its disk are lost: the shard comes back with no store at all
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });
        awaitClusterState(state -> state.getRoutingTable().index(INDEX).allPrimaryShardsUnassigned());

        // a human accepts the loss of the store, and the shard starts again with a new history
        final String node = clusterService().state().nodes().getDataNodes().values().iterator().next().getName();
        ClusterRerouteUtils.reroute(client(), new AllocateEmptyPrimaryAllocationCommand(INDEX, 0, node, true));
        ensureGreen(INDEX);
        final String newHistory = shard().getHistoryUUID();
        assertThat(newHistory, not(equalTo(oldHistory)));

        // the new primary adopts the staged files of the old history: every acknowledged row is in the table, exactly once
        assertBusy(() -> {
            final TreeSet<Long> rows = listedRows(uuid, oldHistory);
            assertThat(rows.size(), equalTo(3_000));
            assertThat(rows.first(), equalTo(0L));
            assertThat(rows.last(), equalTo(2_999L));
            assertThat("the staged copies are deleted", stagedFileCount(uuid), equalTo(0));
        });

        // the shard works again: new rows go to the new history, and the old rows stay where they are
        final BulkResponse after = bulk(3_000, 3_100);
        assertFalse(after.buildFailureMessage(), after.hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertThat(listedRows(uuid, newHistory).size(), equalTo(100));
        assertThat(listedRows(uuid, oldHistory).size(), equalTo(3_000));
    }
}
