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
package org.elasticsearch.repositories;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public abstract class AbstractThirdPartyRepositoryTestCase extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .setSecureSettings(credentials())
            .build();
    }

    protected abstract SecureSettings credentials();

    protected abstract void createRepository(String repoName);


    public void testCreateSnapshot() {
        createRepository("test-repo");

        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-2", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-3", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();

        final String snapshotName = "test-snap-" + System.currentTimeMillis();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", snapshotName)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client().admin()
                .cluster()
                .prepareGetSnapshots("test-repo")
                .setSnapshots(snapshotName)
                .get()
                .getSnapshots()
                .get(0)
                .state(),
            equalTo(SnapshotState.SUCCESS));

        assertTrue(client().admin()
                .cluster()
                .prepareDeleteSnapshot("test-repo", snapshotName)
                .get()
                .isAcknowledged());

    }
}
