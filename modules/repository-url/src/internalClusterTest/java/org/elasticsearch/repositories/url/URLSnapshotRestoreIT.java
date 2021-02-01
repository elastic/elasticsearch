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

package org.elasticsearch.repositories.url;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.plugin.repository.url.URLRepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class URLSnapshotRestoreIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(URLRepositoryPlugin.class);
    }

    public void testUrlRepository() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path repositoryLocation = randomRepoPath();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
            .setType(FsRepository.TYPE).setSettings(Settings.builder()
                .put(FsRepository.LOCATION_SETTING.getKey(), repositoryLocation)
                .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client
            .admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        int actualTotalShards = createSnapshotResponse.getSnapshotInfo().totalShards();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(actualTotalShards));

        SnapshotState state = client
            .admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap")
            .get()
            .getSnapshots("test-repo")
            .get(0)
            .state();
        assertThat(state, equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> create read-only URL repository");
        assertAcked(client.admin().cluster().preparePutRepository("url-repo")
            .setType(URLRepository.TYPE).setSettings(Settings.builder()
                .put(URLRepository.URL_SETTING.getKey(), repositoryLocation.toUri().toURL().toString())
                .put("list_directories", randomBoolean())));
        logger.info("--> restore index after deletion");
        RestoreSnapshotResponse restoreSnapshotResponse = client
            .admin()
            .cluster()
            .prepareRestoreSnapshot("url-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> list available shapshots");
        GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("url-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots("url-repo"), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots("url-repo").size(), equalTo(1));

        logger.info("--> delete snapshot");
        AcknowledgedResponse deleteSnapshotResponse = client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap").get();
        assertAcked(deleteSnapshotResponse);

        logger.info("--> list available shapshot again, no snapshots should be returned");
        getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("url-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots("url-repo"), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots("url-repo").size(), equalTo(0));
    }
}
