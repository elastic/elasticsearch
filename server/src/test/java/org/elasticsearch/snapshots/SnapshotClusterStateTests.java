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

import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.cluster.AbstractIndicesClusterStateServiceTestCase;
import org.elasticsearch.indices.cluster.ClusterStateChanges;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

public class SnapshotClusterStateTests extends AbstractIndicesClusterStateServiceTestCase {

    private ThreadPool threadPool;
    private ClusterStateChanges cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testRepository() {
        ClusterState state = ClusterStateCreationUtils.state(3, new String[]{"test-index"}, randomIntBetween(1, 5));
        state = cluster.putRepository(
            state,
            new PutRepositoryRequest("test-repo").type("fs").verify(false)
                .settings(
                    "{\"location\": \"" + Environment.PATH_REPO_SETTING.get(ClusterStateChanges.SETTINGS).get(0) + "\"}",
                    XContentType.JSON
                )
        );
        state = cluster.createSnapshot(state, new CreateSnapshotRequest("test-repo", "test-snap"));
        assertNotNull(state.custom(SnapshotsInProgress.TYPE));
        SnapshotsInProgress.Entry firstEntry = ((SnapshotsInProgress) state.custom(SnapshotsInProgress.TYPE)).entries().get(0);
        assertEquals("test-snap", firstEntry.snapshot().getSnapshotId().getName());
        assertEquals(SnapshotsInProgress.State.INIT, firstEntry.state());
        state = cluster.beginSnapshot(state, firstEntry, false);
        SnapshotsInProgress.Entry secondEntry = ((SnapshotsInProgress) state.custom(SnapshotsInProgress.TYPE)).entries().get(0);
        assertEquals(Strings.toString(state, true, true), SnapshotsInProgress.State.STARTED, secondEntry.state());
        final ClusterState finalState = state;
        RuntimeException ex =
            expectThrows(RuntimeException.class, () -> cluster.deleteIndices(finalState, new DeleteIndexRequest("test-index")));
        assertEquals(
            "Cannot delete indices that are being snapshotted: [[test-index]]. " +
                "Try again after snapshot finishes or cancel the currently running snapshot.",
            ex.getCause().getMessage()
        );
        state = cluster.deleteSnapshot(state, new DeleteSnapshotRequest("test-repo", "test-snap"));
        assertNotNull(state.custom(SnapshotsInProgress.TYPE));
        // TODO: This should be 0, but we're currently leaving one aborted entry behind
        assertEquals(1, ((SnapshotsInProgress) state.custom(SnapshotsInProgress.TYPE)).entries().size());
        // TODO: This should not throw, but currently does because the deleted snapshot is not properly cleared from the state
        final ClusterState beforeDeleteState = state;
        expectThrows(
            RuntimeException.class,
            () -> cluster.deleteRepository(beforeDeleteState, new DeleteRepositoryRequest("test-repo"))
        );
    }
}
