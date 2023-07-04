/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Tests that verify that a snapshot repository is not getting corrupted and continues to function properly when accessed by multiple
 * clusters of different versions. Concretely this test suite is simulating the following scenario:
 * <ul>
 *     <li>Start and run against a cluster in an old version: {@link TestStep#STEP1_OLD_CLUSTER}</li>
 *     <li>Start and run against a cluster running the current version: {@link TestStep#STEP2_NEW_CLUSTER}</li>
 *     <li>Run against the old version cluster from the first step: {@link TestStep#STEP3_OLD_CLUSTER}</li>
 *     <li>Run against the current version cluster from the second step: {@link TestStep#STEP4_NEW_CLUSTER}</li>
 * </ul>
 */
@SuppressWarnings("removal")
@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/94459")
public class MultiVersionRepositoryAccessIT extends ESRestTestCase {

    private enum TestStep {
        STEP1_OLD_CLUSTER("step1"),
        STEP2_NEW_CLUSTER("step2"),
        STEP3_OLD_CLUSTER("step3"),
        STEP4_NEW_CLUSTER("step4");

        private final String name;

        TestStep(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        public static TestStep parse(String value) {
            return switch (value) {
                case "step1" -> STEP1_OLD_CLUSTER;
                case "step2" -> STEP2_NEW_CLUSTER;
                case "step3" -> STEP3_OLD_CLUSTER;
                case "step4" -> STEP4_NEW_CLUSTER;
                default -> throw new AssertionError("unknown test step: " + value);
            };
        }
    }

    private static final TestStep TEST_STEP = TestStep.parse(System.getProperty("tests.rest.suite"));

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    public void testCreateAndRestoreSnapshot() throws IOException {
        final String repoName = getTestName();
        try {
            final int shards = 3;
            final String index = "test-index";
            createIndex(index, shards);
            createRepository(repoName, false, true);
            createSnapshot(repoName, "snapshot-" + TEST_STEP, index);
            final String snapshotToDeleteName = "snapshot-to-delete";
            // Create a snapshot and delete it right away again to test the impact of each version's cleanup functionality that is run
            // as part of the snapshot delete
            createSnapshot(repoName, snapshotToDeleteName, index);
            final List<Map<String, Object>> snapshotsIncludingToDelete = listSnapshots(repoName);
            // Every step creates one snapshot and we have to add one more for the temporary snapshot
            assertThat(snapshotsIncludingToDelete, hasSize(TEST_STEP.ordinal() + 1 + 1));
            assertThat(
                snapshotsIncludingToDelete.stream().map(sn -> (String) sn.get("snapshot")).collect(Collectors.toList()),
                hasItem(snapshotToDeleteName)
            );
            deleteSnapshot(repoName, snapshotToDeleteName);
            final List<Map<String, Object>> snapshots = listSnapshots(repoName);
            assertThat(snapshots, hasSize(TEST_STEP.ordinal() + 1));
            switch (TEST_STEP) {
                case STEP2_NEW_CLUSTER, STEP4_NEW_CLUSTER -> assertSnapshotStatusSuccessful(
                    repoName,
                    snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new)
                );
                case STEP1_OLD_CLUSTER -> assertSnapshotStatusSuccessful(repoName, "snapshot-" + TEST_STEP);
                case STEP3_OLD_CLUSTER -> assertSnapshotStatusSuccessful(
                    repoName,
                    "snapshot-" + TEST_STEP,
                    "snapshot-" + TestStep.STEP3_OLD_CLUSTER
                );
            }
            if (TEST_STEP == TestStep.STEP3_OLD_CLUSTER) {
                ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards, index);
            } else if (TEST_STEP == TestStep.STEP4_NEW_CLUSTER) {
                for (TestStep value : TestStep.values()) {
                    ensureSnapshotRestoreWorks(repoName, "snapshot-" + value, shards, index);
                }
            }
        } finally {
            deleteRepository(repoName);
        }
    }

    public void testReadOnlyRepo() throws IOException {
        final String repoName = getTestName();
        final int shards = 3;
        final boolean readOnly = TEST_STEP.ordinal() > 1; // only restore from read-only repo in steps 3 and 4
        createRepository(repoName, readOnly, true);
        final String index = "test-index";
        if (readOnly == false) {
            createIndex(index, shards);
            createSnapshot(repoName, "snapshot-" + TEST_STEP, index);
        }
        final List<Map<String, Object>> snapshots = listSnapshots(repoName);
        switch (TEST_STEP) {
            case STEP1_OLD_CLUSTER -> assertThat(snapshots, hasSize(1));
            case STEP2_NEW_CLUSTER, STEP4_NEW_CLUSTER, STEP3_OLD_CLUSTER -> assertThat(snapshots, hasSize(2));
        }
        if (TEST_STEP == TestStep.STEP1_OLD_CLUSTER || TEST_STEP == TestStep.STEP3_OLD_CLUSTER) {
            assertSnapshotStatusSuccessful(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER);
        } else {
            assertSnapshotStatusSuccessful(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, "snapshot-" + TestStep.STEP2_NEW_CLUSTER);
        }
        if (TEST_STEP == TestStep.STEP3_OLD_CLUSTER) {
            ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards, index);
        } else if (TEST_STEP == TestStep.STEP4_NEW_CLUSTER) {
            ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards, index);
            ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP2_NEW_CLUSTER, shards, index);
        }
    }

    private static final List<Class<? extends Exception>> EXPECTED_BWC_EXCEPTIONS = List.of(
        ResponseException.class,
        ElasticsearchStatusException.class
    );

    public void testUpgradeMovesRepoToNewMetaVersion() throws IOException {
        final String repoName = getTestName();
        try {
            final int shards = 3;
            final String index = "test-index";
            createIndex(index, shards);
            final IndexVersion minNodeVersion = minimumIndexVersion();
            // 7.12.0+ will try to load RepositoryData during repo creation if verify is true, which is impossible in case of version
            // incompatibility in the downgrade test step. We verify that it is impossible here and then create the repo using verify=false
            // to check behavior on other operations below.
            final boolean verify = TEST_STEP != TestStep.STEP3_OLD_CLUSTER
                || SnapshotsService.includesUUIDs(minNodeVersion)
                || minNodeVersion.before(IndexVersion.V_7_12_0);
            if (verify == false) {
                expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> createRepository(repoName, false, true));
            }
            createRepository(repoName, false, verify);
            // only create some snapshots in the first two steps
            if (TEST_STEP == TestStep.STEP1_OLD_CLUSTER || TEST_STEP == TestStep.STEP2_NEW_CLUSTER) {
                createSnapshot(repoName, "snapshot-" + TEST_STEP, index);
                final List<Map<String, Object>> snapshots = listSnapshots(repoName);
                // Every step creates one snapshot
                assertThat(snapshots, hasSize(TEST_STEP.ordinal() + 1));
                assertSnapshotStatusSuccessful(repoName, snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new));
                if (TEST_STEP == TestStep.STEP1_OLD_CLUSTER) {
                    ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards, index);
                } else {
                    deleteSnapshot(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER);
                    ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP2_NEW_CLUSTER, shards, index);
                    createSnapshot(repoName, "snapshot-1", index);
                    ensureSnapshotRestoreWorks(repoName, "snapshot-1", shards, index);
                    deleteSnapshot(repoName, "snapshot-" + TestStep.STEP2_NEW_CLUSTER);
                    createSnapshot(repoName, "snapshot-2", index);
                    ensureSnapshotRestoreWorks(repoName, "snapshot-2", shards, index);
                }
            } else {
                if (SnapshotsService.includesUUIDs(minNodeVersion) == false) {
                    assertThat(TEST_STEP, is(TestStep.STEP3_OLD_CLUSTER));
                    expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> listSnapshots(repoName));
                    expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> deleteSnapshot(repoName, "snapshot-1"));
                    expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> deleteSnapshot(repoName, "snapshot-2"));
                    expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> createSnapshot(repoName, "snapshot-impossible", index));
                } else {
                    assertThat(listSnapshots(repoName), hasSize(2));
                    if (TEST_STEP == TestStep.STEP4_NEW_CLUSTER) {
                        ensureSnapshotRestoreWorks(repoName, "snapshot-1", shards, index);
                        ensureSnapshotRestoreWorks(repoName, "snapshot-2", shards, index);
                    }
                }
            }
        } finally {
            deleteRepository(repoName);
        }
    }

    private static void assertSnapshotStatusSuccessful(String repoName, String... snapshots) throws IOException {
        Request statusReq = new Request("GET", "/_snapshot/" + repoName + "/" + String.join(",", snapshots) + "/_status");
        ObjectPath statusResp = ObjectPath.createFromResponse(client().performRequest(statusReq));
        for (int i = 0; i < snapshots.length; i++) {
            assertThat(statusResp.evaluate("snapshots." + i + ".shards_stats.failed"), equalTo(0));
        }
    }

    private void deleteSnapshot(String repoName, String name) throws IOException {
        assertAcknowledged(client().performRequest(new Request("DELETE", "/_snapshot/" + repoName + "/" + name)));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> listSnapshots(String repoName) throws IOException {
        try (
            InputStream entity = client().performRequest(new Request("GET", "/_snapshot/" + repoName + "/_all")).getEntity().getContent();
            XContentParser parser = createParser(JsonXContent.jsonXContent, entity)
        ) {
            return (List<Map<String, Object>>) parser.map().get("snapshots");
        }
    }

    private static void ensureSnapshotRestoreWorks(String repoName, String name, int shards, String index) throws IOException {
        wipeAllIndices();
        Request restoreReq = new Request("POST", "/_snapshot/" + repoName + "/" + name + "/_restore");
        restoreReq.setJsonEntity("{\"indices\": \"" + index + "\"}");
        restoreReq.addParameter("wait_for_completion", "true");
        ObjectPath restoreResp = ObjectPath.createFromResponse(client().performRequest(restoreReq));
        assertThat(restoreResp.evaluate("snapshot.shards.failed"), equalTo(0));
        assertThat(restoreResp.evaluate("snapshot.shards.successful"), equalTo(shards));
    }

    private static void createRepository(String repoName, boolean readOnly, boolean verify) throws IOException {
        Request repoReq = new Request("PUT", "/_snapshot/" + repoName);
        repoReq.setJsonEntity(
            Strings.toString(
                new PutRepositoryRequest().type("fs")
                    .verify(verify)
                    .settings(Settings.builder().put("location", repoName).put("readonly", readOnly).build())
            )
        );
        assertAcknowledged(client().performRequest(repoReq));
    }

    private static void createSnapshot(String repoName, String name, String index) throws IOException {
        final Request createSnapshotRequest = new Request("PUT", "/_snapshot/" + repoName + "/" + name);
        createSnapshotRequest.addParameter("wait_for_completion", "true");
        createSnapshotRequest.setJsonEntity("{ \"indices\" : \"" + index + "\"}");
        final Response response = client().performRequest(createSnapshotRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(HttpURLConnection.HTTP_OK));
    }

    private void createIndex(String name, int shards) throws IOException {
        final Request putIndexRequest = new Request("PUT", "/" + name);
        putIndexRequest.setJsonEntity(Strings.format("""
            {
                "settings" : {
                    "index" : {
                        "number_of_shards" : %s,
                        "number_of_replicas" : 0
                    }
                }
            }""", shards));
        final Response response = client().performRequest(putIndexRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(HttpURLConnection.HTTP_OK));
    }
}
