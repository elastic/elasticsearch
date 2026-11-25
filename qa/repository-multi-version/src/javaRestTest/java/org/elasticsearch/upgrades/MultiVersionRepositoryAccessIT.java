/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.snapshots.SnapshotsServiceUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
 * <p>
 * Rather than splitting the four steps across separate Gradle tasks and JVMs (as the legacy {@code testClusters} setup did), the steps are
 * modelled as ordered test parameters. Two {@link ElasticsearchCluster} rules — one on the old version and one on the current version —
 * share the same {@code path.repo} directory and run for the duration of the suite. {@link TestCaseOrdering} guarantees that every test
 * method runs against {@link TestStep#STEP1_OLD_CLUSTER} first, then {@link TestStep#STEP2_NEW_CLUSTER}, and so on, so the repository state
 * accumulated by earlier steps is observed by later ones.
 */
@TestCaseOrdering(MultiVersionRepositoryAccessIT.TestStepOrdering.class)
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

        /**
         * Whether this step runs against the old-version cluster (steps 1 and 3) as opposed to the current-version cluster (steps 2 and 4).
         */
        boolean runsAgainstOldCluster() {
            return this == STEP1_OLD_CLUSTER || this == STEP3_OLD_CLUSTER;
        }
    }

    /**
     * Orders test execution so that all methods run against {@link TestStep#STEP1_OLD_CLUSTER} before any run against
     * {@link TestStep#STEP2_NEW_CLUSTER}, and so on. This preserves the sequential old/new/old/new repository access the legacy
     * multi-task setup relied on.
     */
    public static class TestStepOrdering implements Comparator<TestMethodAndParams> {
        @Override
        public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
            return Integer.compare(getOrdinal(o1), getOrdinal(o2));
        }

        private static int getOrdinal(TestMethodAndParams t) {
            return ((TestStep) t.getInstanceArguments().get(0)).ordinal();
        }
    }

    private static final String OLD_CLUSTER_VERSION = System.getProperty("tests.old_cluster_version");

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster oldCluster = buildCluster(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion());

    private static final ElasticsearchCluster newCluster = buildCluster(Version.CURRENT.toString(), false);

    private static ElasticsearchCluster buildCluster(String version, boolean detachedVersion) {
        var cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(version, detachedVersion)
            .nodes(2)
            .setting("path.repo", () -> repoDirectory.getRoot().getPath())
            .setting("xpack.security.enabled", "false");
        // 8.10.x versions contain a bogus assertion that trips when reading repositories touched by newer versions
        // see https://github.com/elastic/elasticsearch/issues/98454 for details
        if (version.equals("8.10.0") || version.equals("8.10.1") || version.equals("8.10.2") || version.equals("8.10.3")) {
            cluster.jvmArg("-da");
        }
        return cluster.build();
    }

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(oldCluster).around(newCluster);

    /**
     * Tracks which cluster the shared REST client is currently pointing at, so we only tear down and re-create the client when a step
     * needs the other cluster.
     */
    private static Boolean clientUsesOldCluster = null;

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(TestStep.values()).map(s -> new Object[] { s }).collect(Collectors.toList());
    }

    private final TestStep testStep;

    public MultiVersionRepositoryAccessIT(@Name("step") TestStep testStep) {
        this.testStep = testStep;
    }

    @Override
    protected String getTestRestCluster() {
        return testStep.runsAgainstOldCluster() ? oldCluster.getHttpAddresses() : newCluster.getHttpAddresses();
    }

    /**
     * Repoints the shared REST client at the cluster required by the current step. {@link ESRestTestCase#initClient()} runs first (as a
     * superclass {@code @Before}) and is a no-op once the client is initialized, so here we explicitly close and re-create the client
     * whenever the step switches between the old and new cluster.
     */
    @Before
    public void selectClusterForStep() throws IOException {
        boolean needsOldCluster = testStep.runsAgainstOldCluster();
        if (clientUsesOldCluster != null && clientUsesOldCluster != needsOldCluster) {
            closeClients();
            initClient();
        }
        clientUsesOldCluster = needsOldCluster;
    }

    /**
     * The repository name must be stable across all four steps so that a repository created in one step is observed by later steps.
     * {@link #getTestName()} includes the parameterized step suffix (e.g. {@code "testReadOnlyRepo {step=step1}"}), so strip it.
     */
    private String getRepoName() {
        return getTestName().split(" ")[0];
    }

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
        final String repoName = getRepoName();
        try {
            final int shards = 3;
            final String index = "test-index";
            createIndex(index, shards);
            createRepository(repoName, false, true);
            createSnapshot(repoName, "snapshot-" + testStep, index);
            final String snapshotToDeleteName = "snapshot-to-delete";
            // Create a snapshot and delete it right away again to test the impact of each version's cleanup functionality that is run
            // as part of the snapshot delete
            createSnapshot(repoName, snapshotToDeleteName, index);
            final List<Map<String, Object>> snapshotsIncludingToDelete = listSnapshots(repoName);
            // Every step creates one snapshot and we have to add one more for the temporary snapshot
            assertThat(snapshotsIncludingToDelete, hasSize(testStep.ordinal() + 1 + 1));
            assertThat(
                snapshotsIncludingToDelete.stream().map(sn -> (String) sn.get("snapshot")).collect(Collectors.toList()),
                hasItem(snapshotToDeleteName)
            );
            deleteSnapshot(repoName, snapshotToDeleteName);
            final List<Map<String, Object>> snapshots = listSnapshots(repoName);
            assertThat(snapshots, hasSize(testStep.ordinal() + 1));
            switch (testStep) {
                case STEP2_NEW_CLUSTER, STEP4_NEW_CLUSTER -> assertSnapshotStatusSuccessful(
                    repoName,
                    snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new)
                );
                case STEP1_OLD_CLUSTER -> assertSnapshotStatusSuccessful(repoName, "snapshot-" + testStep);
                case STEP3_OLD_CLUSTER -> assertSnapshotStatusSuccessful(
                    repoName,
                    "snapshot-" + testStep,
                    "snapshot-" + TestStep.STEP3_OLD_CLUSTER
                );
            }
            if (testStep == TestStep.STEP3_OLD_CLUSTER) {
                ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards, index);
            } else if (testStep == TestStep.STEP4_NEW_CLUSTER) {
                for (TestStep value : TestStep.values()) {
                    ensureSnapshotRestoreWorks(repoName, "snapshot-" + value, shards, index);
                }
            }
        } finally {
            deleteRepository(repoName);
        }
    }

    public void testReadOnlyRepo() throws IOException {
        final String repoName = getRepoName();
        final int shards = 3;
        final boolean readOnly = testStep.ordinal() > 1; // only restore from read-only repo in steps 3 and 4
        createRepository(repoName, readOnly, true);
        final String index = "test-index";
        if (readOnly == false) {
            createIndex(index, shards);
            createSnapshot(repoName, "snapshot-" + testStep, index);
        }
        final List<Map<String, Object>> snapshots = listSnapshots(repoName);
        switch (testStep) {
            case STEP1_OLD_CLUSTER -> assertThat(snapshots, hasSize(1));
            case STEP2_NEW_CLUSTER, STEP4_NEW_CLUSTER, STEP3_OLD_CLUSTER -> assertThat(snapshots, hasSize(2));
        }
        if (testStep == TestStep.STEP1_OLD_CLUSTER || testStep == TestStep.STEP3_OLD_CLUSTER) {
            assertSnapshotStatusSuccessful(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER);
        } else {
            assertSnapshotStatusSuccessful(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, "snapshot-" + TestStep.STEP2_NEW_CLUSTER);
        }
        if (testStep == TestStep.STEP3_OLD_CLUSTER) {
            ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards, index);
        } else if (testStep == TestStep.STEP4_NEW_CLUSTER) {
            ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards, index);
            ensureSnapshotRestoreWorks(repoName, "snapshot-" + TestStep.STEP2_NEW_CLUSTER, shards, index);
        }
    }

    private static final List<Class<? extends Exception>> EXPECTED_BWC_EXCEPTIONS = List.of(
        ResponseException.class,
        ElasticsearchStatusException.class
    );

    public void testUpgradeMovesRepoToNewMetaVersion() throws IOException {
        final String repoName = getRepoName();
        try {
            final int shards = 3;
            final String index = "test-index";
            createIndex(index, shards);
            final IndexVersion minNodeVersion = minimumIndexVersion();
            // 7.12.0+ will try to load RepositoryData during repo creation if verify is true, which is impossible in case of version
            // incompatibility in the downgrade test step. We verify that it is impossible here and then create the repo using verify=false
            // to check behavior on other operations below.
            final boolean verify = testStep != TestStep.STEP3_OLD_CLUSTER
                || SnapshotsServiceUtils.includesUUIDs(minNodeVersion)
                || minNodeVersion.before(IndexVersions.V_7_12_0);
            if (verify == false) {
                expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> createRepository(repoName, false, true));
            }
            createRepository(repoName, false, verify);
            // only create some snapshots in the first two steps
            if (testStep == TestStep.STEP1_OLD_CLUSTER || testStep == TestStep.STEP2_NEW_CLUSTER) {
                createSnapshot(repoName, "snapshot-" + testStep, index);
                final List<Map<String, Object>> snapshots = listSnapshots(repoName);
                // Every step creates one snapshot
                assertThat(snapshots, hasSize(testStep.ordinal() + 1));
                assertSnapshotStatusSuccessful(repoName, snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new));
                if (testStep == TestStep.STEP1_OLD_CLUSTER) {
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
                if (SnapshotsServiceUtils.includesUUIDs(minNodeVersion) == false) {
                    assertThat(testStep, is(TestStep.STEP3_OLD_CLUSTER));
                    expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> listSnapshots(repoName));
                    expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> deleteSnapshot(repoName, "snapshot-1"));
                    expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> deleteSnapshot(repoName, "snapshot-2"));
                    expectThrowsAnyOf(EXPECTED_BWC_EXCEPTIONS, () -> createSnapshot(repoName, "snapshot-impossible", index));
                } else {
                    assertThat(listSnapshots(repoName), hasSize(2));
                    if (testStep == TestStep.STEP4_NEW_CLUSTER) {
                        ensureSnapshotRestoreWorks(repoName, "snapshot-1", shards, index);
                        ensureSnapshotRestoreWorks(repoName, "snapshot-2", shards, index);
                    }
                }
            }
        } finally {
            deleteRepository(repoName);
        }
    }

    public void testSnapshotCreatedInOldVersionCanBeDeletedInNew() throws IOException {
        final String repoName = getRepoName();
        try {
            final int shards = 3;
            final String index = "test-index";
            createIndex(index, shards);
            final IndexVersion minNodeVersion = minimumIndexVersion();
            // 7.12.0+ will try to load RepositoryData during repo creation if verify is true, which is impossible in case of version
            // incompatibility in the downgrade test step.
            final boolean verify = testStep != TestStep.STEP3_OLD_CLUSTER
                || SnapshotsServiceUtils.includesUUIDs(minNodeVersion)
                || minNodeVersion.before(IndexVersions.V_7_12_0);
            createRepository(repoName, false, verify);

            // Create snapshots in the first step
            if (testStep == TestStep.STEP1_OLD_CLUSTER) {
                int numberOfSnapshots = randomIntBetween(5, 10);
                for (int i = 0; i < numberOfSnapshots; i++) {
                    createSnapshot(repoName, "snapshot-" + i, index);
                }
                final List<Map<String, Object>> snapshots = listSnapshots(repoName);
                assertSnapshotStatusSuccessful(repoName, snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new));
            } else if (testStep == TestStep.STEP2_NEW_CLUSTER) {
                final List<Map<String, Object>> snapshots = listSnapshots(repoName);
                List<String> snapshotNames = new ArrayList<>(snapshots.stream().map(sn -> (String) sn.get("snapshot")).toList());

                // Delete a single snapshot
                deleteSnapshot(repoName, snapshotNames.removeFirst());

                // Delete a bulk number of snapshots, avoiding the case where we delete all snapshots since this invokes
                // cleanup code and bulk snapshot deletion logic which is tested in testUpgradeMovesRepoToNewMetaVersion
                final List<String> snapshotsToDeleteInBulk = randomSubsetOf(randomIntBetween(1, snapshotNames.size() - 1), snapshotNames);
                deleteSnapshots(repoName, snapshotsToDeleteInBulk);
                snapshotNames.removeAll(snapshotsToDeleteInBulk);

                // Delete the rest of the snapshots (will invoke bulk snapshot deletion logic)
                deleteSnapshots(repoName, snapshotNames);
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

    private void deleteSnapshots(String repoName, List<String> names) throws IOException {
        assertAcknowledged(
            client().performRequest(new Request("DELETE", "/_snapshot/" + repoName + "/" + Strings.collectionToCommaDelimitedString(names)))
        );
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
                new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).type("fs")
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
