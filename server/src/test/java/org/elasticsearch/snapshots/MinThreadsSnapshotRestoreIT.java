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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.mockstore.MockRepository;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for snapshot/restore that require at least 2 threads available
 * in the thread pool (for example, tests that use the mock repository that
 * block on master).
 */
public class MinThreadsSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                   .put("thread_pool.snapshot.core", 2)
                   .put("thread_pool.snapshot.max", 2)
                   .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MockRepository.Plugin.class);
    }

    public void testConcurrentSnapshotDeletionsNotAllowed() throws Exception {
        logger.info("--> creating repository");
        final String repo = "test-repo";
        assertAcked(client().admin().cluster().preparePutRepository(repo).setType("mock").setSettings(
            Settings.builder()
                .put("location", randomRepoPath())
                .put("random", randomAlphaOfLength(10))
                .put("wait_after_unblock", 200)).get());

        logger.info("--> snapshot twice");
        final String index = "test-idx1";
        assertAcked(prepareCreate(index, 1, Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        for (int i = 0; i < 10; i++) {
            index(index, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        final String snapshot1 = "test-snap1";
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot1).setWaitForCompletion(true).get();
        final String index2 = "test-idx2";
        assertAcked(prepareCreate(index2, 1, Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        for (int i = 0; i < 10; i++) {
            index(index2, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        final String snapshot2 = "test-snap2";
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot2).setWaitForCompletion(true).get();

        String blockedNode = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, blockedNode).repository(repo)).blockOnDataFiles(true);
        logger.info("--> start deletion of first snapshot");
        ActionFuture<AcknowledgedResponse> future =
            client().admin().cluster().prepareDeleteSnapshot(repo, snapshot2).execute();
        logger.info("--> waiting for block to kick in on node [{}]", blockedNode);
        waitForBlock(blockedNode, repo, TimeValue.timeValueSeconds(10));

        logger.info("--> try deleting the second snapshot, should fail because the first deletion is in progress");
        try {
            client().admin().cluster().prepareDeleteSnapshot(repo, snapshot1).get();
            fail("should not be able to delete snapshots concurrently");
        } catch (ConcurrentSnapshotExecutionException e) {
            assertThat(e.getMessage(), containsString("cannot delete - another snapshot is currently being deleted"));
        }

        logger.info("--> unblocking blocked node [{}]", blockedNode);
        unblockNode(repo, blockedNode);

        logger.info("--> wait until first snapshot is finished");
        assertAcked(future.actionGet());

        logger.info("--> delete second snapshot, which should now work");
        client().admin().cluster().prepareDeleteSnapshot(repo, snapshot1).get();
        assertTrue(client().admin().cluster().prepareGetSnapshots(repo).setSnapshots("_all").get().getSnapshots(repo).isEmpty());
    }

    public void testSnapshottingWithInProgressDeletionNotAllowed() throws Exception {
        logger.info("--> creating repository");
        final String repo = "test-repo";
        assertAcked(client().admin().cluster().preparePutRepository(repo).setType("mock").setSettings(
            Settings.builder()
                .put("location", randomRepoPath())
                .put("random", randomAlphaOfLength(10))
                .put("wait_after_unblock", 200)).get());

        logger.info("--> snapshot");
        final String index = "test-idx";
        assertAcked(prepareCreate(index, 1, Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        for (int i = 0; i < 10; i++) {
            index(index, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        final String snapshot1 = "test-snap1";
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot1).setWaitForCompletion(true).get();

        String blockedNode = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, blockedNode).repository(repo)).blockOnDataFiles(true);
        logger.info("--> start deletion of snapshot");
        ActionFuture<AcknowledgedResponse> future = client().admin().cluster().prepareDeleteSnapshot(repo, snapshot1).execute();
        logger.info("--> waiting for block to kick in on node [{}]", blockedNode);
        waitForBlock(blockedNode, repo, TimeValue.timeValueSeconds(10));

        logger.info("--> try creating a second snapshot, should fail because the deletion is in progress");
        final String snapshot2 = "test-snap2";
        try {
            client().admin().cluster().prepareCreateSnapshot(repo, snapshot2).setWaitForCompletion(true).get();
            fail("should not be able to create a snapshot while another is being deleted");
        } catch (ConcurrentSnapshotExecutionException e) {
            assertThat(e.getMessage(), containsString("cannot snapshot while a snapshot deletion is in-progress"));
        }

        logger.info("--> unblocking blocked node [{}]", blockedNode);
        unblockNode(repo, blockedNode);

        logger.info("--> wait until snapshot deletion is finished");
        assertAcked(future.actionGet());

        logger.info("--> creating second snapshot, which should now work");
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot2).setWaitForCompletion(true).get();
        assertEquals(1, client().admin().cluster().prepareGetSnapshots(repo).setSnapshots("_all").get().getSnapshots(repo).size());
    }

    public void testRestoreWithInProgressDeletionsNotAllowed() throws Exception {
        logger.info("--> creating repository");
        final String repo = "test-repo";
        assertAcked(client().admin().cluster().preparePutRepository(repo).setType("mock").setSettings(
            Settings.builder()
                .put("location", randomRepoPath())
                .put("random", randomAlphaOfLength(10))
                .put("wait_after_unblock", 200)).get());

        logger.info("--> snapshot");
        final String index = "test-idx";
        assertAcked(prepareCreate(index, 1, Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        for (int i = 0; i < 10; i++) {
            index(index, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        final String snapshot1 = "test-snap1";
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot1).setWaitForCompletion(true).get();
        final String index2 = "test-idx2";
        assertAcked(prepareCreate(index2, 1, Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        for (int i = 0; i < 10; i++) {
            index(index2, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        final String snapshot2 = "test-snap2";
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot2).setWaitForCompletion(true).get();
        client().admin().indices().prepareClose(index, index2).get();

        String blockedNode = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, blockedNode).repository(repo)).blockOnDataFiles(true);
        logger.info("--> start deletion of snapshot");
        ActionFuture<AcknowledgedResponse> future = client().admin().cluster().prepareDeleteSnapshot(repo, snapshot2).execute();
        logger.info("--> waiting for block to kick in on node [{}]", blockedNode);
        waitForBlock(blockedNode, repo, TimeValue.timeValueSeconds(10));

        logger.info("--> try restoring the other snapshot, should fail because the deletion is in progress");
        try {
            client().admin().cluster().prepareRestoreSnapshot(repo, snapshot1).setWaitForCompletion(true).get();
            fail("should not be able to restore a snapshot while another is being deleted");
        } catch (ConcurrentSnapshotExecutionException e) {
            assertThat(e.getMessage(), containsString("cannot restore a snapshot while a snapshot deletion is in-progress"));
        }

        logger.info("--> unblocking blocked node [{}]", blockedNode);
        unblockNode(repo, blockedNode);

        logger.info("--> wait until snapshot deletion is finished");
        assertAcked(future.actionGet());

        logger.info("--> restoring snapshot, which should now work");
        client().admin().cluster().prepareRestoreSnapshot(repo, snapshot1).setWaitForCompletion(true).get();
        assertEquals(1, client().admin().cluster().prepareGetSnapshots(repo).setSnapshots("_all").get().getSnapshots(repo).size());
    }
}
