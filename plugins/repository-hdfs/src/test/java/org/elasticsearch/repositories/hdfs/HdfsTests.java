/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;

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

        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository("test-repo")
            .setType("hdfs")
            .setSettings(
                Settings.builder()
                    .put("uri", "hdfs:///")
                    .put("conf.fs.AbstractFileSystem.hdfs.impl", TestingFs.class.getName())
                    .put("path", "foo")
                    .put("chunk_size", randomIntBetween(100, 1000) + "k")
                    .put("compress", randomBoolean())
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-2").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-3").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        assertThat(count(client, "test-idx-2"), equalTo(100L));
        assertThat(count(client, "test-idx-3"), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(),
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
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
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
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-2")
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetadata().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetadata().hasIndex("test-idx-2"), equalTo(false));
        final BlobStoreRepository repo = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository("test-repo");
        BlobStoreTestUtil.assertConsistency(repo);
    }

    public void testMissingUri() {
        try {
            client().admin().cluster().preparePutRepository("test-repo").setType("hdfs").setSettings(Settings.EMPTY).get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("No 'uri' defined for hdfs"));
        }
    }

    public void testEmptyUri() {
        try {
            client().admin()
                .cluster()
                .preparePutRepository("test-repo")
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
            client().admin()
                .cluster()
                .preparePutRepository("test-repo")
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
            client().admin()
                .cluster()
                .preparePutRepository("test-repo")
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
            client().admin()
                .cluster()
                .preparePutRepository("test-repo")
                .setType("hdfs")
                .setSettings(Settings.builder().put("uri", "hdfs:///").build())
                .get();
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("No 'path' defined for hdfs"));
        }
    }

    private long count(Client client, String index) {
        return client.prepareSearch(index).setSize(0).get().getHits().getTotalHits().value;
    }
}
