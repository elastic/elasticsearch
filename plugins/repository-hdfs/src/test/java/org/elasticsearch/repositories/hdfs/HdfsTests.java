/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.fixtures.hdfs.HdfsClientThreadLeakFilter;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ThreadLeakFilters(filters = { HdfsClientThreadLeakFilter.class })
public class HdfsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(HdfsPlugin.class);
    }

    public void testSimpleWorkflow() {
        Client client = client();

        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(
                    Settings.builder()
                        .put("uri", "hdfs:///")
                        .put("conf.fs.AbstractFileSystem.hdfs.impl", TestingFs.class.getName())
                        .put("path", "foo")
                        .put("chunk_size", randomIntBetween(100, 1000) + "k")
                        .put("compress", randomBoolean())
                )
        );

        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            prepareIndex("test-idx-1").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            prepareIndex("test-idx-2").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            prepareIndex("test-idx-3").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        assertThat(count(client, "test-idx-2"), equalTo(100L));
        assertThat(count(client, "test-idx-3"), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            client.admin()
                .cluster()
                .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "test-repo")
                .setSnapshots("test-snap")
                .get()
                .getSnapshots()
                .get(0)
                .state(),
            equalTo(SnapshotState.SUCCESS)
        );

        logger.info("--> delete some data");
        for (int i = 0; i < 50; i++) {
            client.prepareDelete("test-idx-1", Integer.toString(i)).get();
        }
        for (int i = 50; i < 100; i++) {
            client.prepareDelete("test-idx-2", Integer.toString(i)).get();
        }
        for (int i = 0; i < 100; i += 2) {
            client.prepareDelete("test-idx-3", Integer.toString(i)).get();
        }
        client().admin().indices().prepareRefresh().get();
        assertThat(count(client, "test-idx-1"), equalTo(50L));
        assertThat(count(client, "test-idx-2"), equalTo(50L));
        assertThat(count(client, "test-idx-3"), equalTo(50L));

        logger.info("--> close indices");
        client.admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        assertThat(count(client, "test-idx-2"), equalTo(100L));
        assertThat(count(client, "test-idx-3"), equalTo(50L));

        // Test restore after index deletion
        logger.info("--> delete indices");
        client().admin().indices().prepareDelete("test-idx-1", "test-idx-2").get();
        logger.info("--> restore one index after deletion");
        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-2")
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        ClusterState clusterState = client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        assertThat(clusterState.getMetadata().getProject().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetadata().getProject().hasIndex("test-idx-2"), equalTo(false));
        final BlobStoreRepository repo = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository("test-repo");
        BlobStoreTestUtil.assertConsistency(repo);
    }

    public void testMissingUri() {
        try {
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(Settings.EMPTY)
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("No 'uri' defined for hdfs"));
        }
    }

    public void testEmptyUri() {
        try {
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(Settings.builder().put("uri", "/path").build())
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage(), e.getCause().getMessage().contains("Invalid scheme [null] specified in uri [/path]"));
        }
    }

    public void testNonHdfsUri() {
        try {
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(Settings.builder().put("uri", "file:///").build())
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("Invalid scheme [file] specified in uri [file:///]"));
        }
    }

    public void testPathSpecifiedInHdfs() {
        try {
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(Settings.builder().put("uri", "hdfs:///some/path").build())
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("Use 'path' option to specify a path [/some/path]"));
        }
    }

    public void testMissingPath() {
        try {
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(Settings.builder().put("uri", "hdfs:///").build())
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("No 'path' defined for hdfs"));
        }
    }

    public void testReplicationFactorBelowOne() {
        try {
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(Settings.builder().put("uri", "hdfs:///").put("replication_factor", "0").put("path", "foo").build())
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof RepositoryException);
            assertTrue(e.getCause().getMessage().contains("Value of replication_factor [0] must be >= 1"));
        }
    }

    public void testReplicationFactorOverMaxShort() {
        try {
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(Settings.builder().put("uri", "hdfs:///").put("replication_factor", "32768").put("path", "foo").build())
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof RepositoryException);
            assertTrue(e.getCause().getMessage().contains("Value of replication_factor [32768] must be <= 32767"));
        }
    }

    public void testReplicationFactorBelowReplicationMin() {
        try {
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(
                    Settings.builder()
                        .put("uri", "hdfs:///")
                        .put("replication_factor", "4")
                        .put("path", "foo")
                        .put("conf.dfs.replication.min", "5")
                        .build()
                )
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof RepositoryException);
            assertTrue(e.getCause().getMessage().contains("Value of replication_factor [4] must be >= dfs.replication.min [5]"));
        }
    }

    public void testReplicationFactorOverReplicationMax() {
        try {
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("hdfs")
                .setSettings(
                    Settings.builder()
                        .put("uri", "hdfs:///")
                        .put("replication_factor", "600")
                        .put("path", "foo")
                        .put("conf.dfs.replication.max", "512")
                        .build()
                )
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof RepositoryException);
            assertTrue(e.getCause().getMessage().contains("Value of replication_factor [600] must be <= dfs.replication.max [512]"));
        }
    }

    private long count(Client client, String index) {
        return SearchResponseUtils.getTotalHitsValue(client.prepareSearch(index).setSize(0));
    }
}
