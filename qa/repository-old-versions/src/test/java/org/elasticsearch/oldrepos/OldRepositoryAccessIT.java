/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldrepos;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class OldRepositoryAccessIT extends ESRestTestCase {
    @Override
    protected Map<String, List<Map<?, ?>>> wipeSnapshots() {
        return Collections.emptyMap();
    }

    public void testOldRepoAccess() throws IOException {
        String repoLocation = System.getProperty("tests.repo.location");
        Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));

        int oldEsPort = Integer.parseInt(System.getProperty("tests.es.port"));
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(adminClient().getNodes().toArray(new Node[0])));
             RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {
            try {
                Request createIndex = new Request("PUT", "/test");
                int numberOfShards = randomIntBetween(1, 3);
                createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\": " + numberOfShards + "}}");
                oldEs.performRequest(createIndex);

                for (int i = 0; i < 5; i++) {
                    Request doc = new Request("PUT", "/test/doc/testdoc" + i);
                    doc.addParameter("refresh", "true");
                    doc.setJsonEntity("{\"test\":\"test" + i + "\", \"val\":" + i + "}");
                    oldEs.performRequest(doc);
                }

                // register repo on old ES and take snapshot
                Request createRepoRequest = new Request("PUT", "/_snapshot/testrepo");
                createRepoRequest.setJsonEntity("{\"type\":\"fs\",\"settings\":{\"location\":\"" + repoLocation + "\"}}");
                oldEs.performRequest(createRepoRequest);

                Request createSnapshotRequest = new Request("PUT", "/_snapshot/testrepo/snap1");
                createSnapshotRequest.addParameter("wait_for_completion", "true");
                createSnapshotRequest.setJsonEntity("{\"indices\":\"test\"}");
                oldEs.performRequest(createSnapshotRequest);

                // register repo on new ES
                ElasticsearchAssertions.assertAcked(client.snapshot().createRepository(
                    new PutRepositoryRequest("testrepo").type("fs").settings(
                        Settings.builder().put("location", repoLocation).build()), RequestOptions.DEFAULT));

                // list snapshots on new ES
                List<SnapshotInfo> snapshotInfos =
                    client.snapshot().get(new GetSnapshotsRequest("testrepo").snapshots(new String[] {"_all"}),
                    RequestOptions.DEFAULT).getSnapshots();
                assertThat(snapshotInfos, hasSize(1));
                SnapshotInfo snapshotInfo = snapshotInfos.get(0);
                assertEquals("snap1", snapshotInfo.snapshotId().getName());
                assertEquals("testrepo", snapshotInfo.repository());
                assertEquals(Arrays.asList("test"), snapshotInfo.indices());
                assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
                assertEquals(numberOfShards, snapshotInfo.successfulShards());
                assertEquals(numberOfShards, snapshotInfo.totalShards());
                assertEquals(0, snapshotInfo.failedShards());
                assertEquals(oldVersion, snapshotInfo.version());

                // list specific snapshot on new ES
                snapshotInfos =
                    client.snapshot().get(new GetSnapshotsRequest("testrepo").snapshots(new String[] {"snap1"}),
                        RequestOptions.DEFAULT).getSnapshots();
                assertThat(snapshotInfos, hasSize(1));
                snapshotInfo = snapshotInfos.get(0);
                assertEquals("snap1", snapshotInfo.snapshotId().getName());
                assertEquals("testrepo", snapshotInfo.repository());
                assertEquals(Arrays.asList("test"), snapshotInfo.indices());
                assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
                assertEquals(numberOfShards, snapshotInfo.successfulShards());
                assertEquals(numberOfShards, snapshotInfo.totalShards());
                assertEquals(0, snapshotInfo.failedShards());
                assertEquals(oldVersion, snapshotInfo.version());


                // list advanced snapshot info on new ES
                SnapshotsStatusResponse snapshotsStatusResponse = client.snapshot().status(
                    new SnapshotsStatusRequest("testrepo").snapshots(new String[]{"snap1"}),
                    RequestOptions.DEFAULT);
                assertThat(snapshotsStatusResponse.getSnapshots(), hasSize(1));
                SnapshotStatus snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
                assertEquals("snap1", snapshotStatus.getSnapshot().getSnapshotId().getName());
                assertEquals("testrepo", snapshotStatus.getSnapshot().getRepository());
                assertEquals(Sets.newHashSet("test"), snapshotStatus.getIndices().keySet());
                assertEquals(SnapshotsInProgress.State.SUCCESS, snapshotStatus.getState());
                assertEquals(numberOfShards, snapshotStatus.getShardsStats().getDoneShards());
                assertEquals(numberOfShards, snapshotStatus.getShardsStats().getTotalShards());
                assertEquals(0, snapshotStatus.getShardsStats().getFailedShards());
                assertThat(snapshotStatus.getStats().getTotalSize(), greaterThan(0L));
                assertThat(snapshotStatus.getStats().getTotalFileCount(), greaterThan(0));
            } finally {
                oldEs.performRequest(new Request("DELETE", "/test"));
            }
        }
    }

}
