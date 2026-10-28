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

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.fixtures.hdfs.HdfsClientThreadLeakFilter;
import org.elasticsearch.test.fixtures.hdfs.HdfsFixture;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;

@ThreadLeakFilters(filters = { HdfsClientThreadLeakFilter.class, TestContainersThreadFilter.class })
abstract class AbstractRepositoryHdfsRestIT extends ESRestTestCase {

    abstract HdfsFixture hdfsFixture();

    public void testBadUrl() throws IOException {
        final var ex = assertThrows(
            ResponseException.class,
            () -> registerHdfsRepository("file://does-not-matter", randomRepoName(), randomPath(), randomBoolean(), randomBoolean())
        );
        assertEquals("expect bad URL response, got: " + ex.getResponse(), 500, ex.getResponse().getStatusLine().getStatusCode());
    }

    public void testCreateGetDeleteRepository() throws IOException {
        final var repoName = randomRepoName();
        final var path = randomPath();
        for (int i = 0; i < between(2, 5); i++) {
            registerHdfsRepository(hdfsUri0(), repoName, path, randomBoolean(), randomBoolean());
            final var repo = getRepository(repoName);
            assertEquals("hdfs", repo.get(repoName + ".type"));
            assertEquals(hdfsUri0(), repo.get(repoName + ".settings.uri"));
            assertEquals(path, repo.get(repoName + ".settings.path"));
            deleteRepository(repoName);
            assertRepositoryNotFound(repoName);
        }
    }

    public void testCreateAndVerifyRepository() throws IOException {
        registerHdfsRepository(hdfsUri0(), randomRepoName(), randomPath(), true, false);
    }

    public void testCreateListDeleteSnapshots() throws IOException {
        final var repoName = randomRepoName();
        final var path = randomPath();
        registerHdfsRepository(hdfsUri0(), repoName, path, false, false);

        final var indexName = randomIndexName();
        final var primaries = between(1, 3);
        createIndex(indexName, indexSettings(primaries, between(0, 2)).build());

        final var snapshotNames = randomList(1, 10, ESRestTestCase::randomIdentifier);
        for (var snapshotName : snapshotNames) {
            final var createSnapshot = createSnapshot(repoName, snapshotName, true);
            assertEquals(snapshotName, createSnapshot.get("snapshot.snapshot"));
            assertEquals("SUCCESS", createSnapshot.get("snapshot.state"));
            assertEquals(Integer.valueOf(primaries), createSnapshot.get("snapshot.shards.successful"));
            assertEquals(Integer.valueOf(0), createSnapshot.get("snapshot.shards.failed"));
        }

        // unregister and re-register the repo to confirm the snapshots persist
        final var maybeReregisterRepo = randomBoolean();
        if (maybeReregisterRepo) {
            deleteRepository(repoName);
            registerHdfsRepository(hdfsUri0(), repoName, path, false, false);
        }

        assertEquals(
            "list API must return all snapshots",
            snapshotNames.stream().sorted().toList(),
            listAllSnapshotNames(repoName).stream().sorted().toList()
        );

        final var remainingSnapshots = new ArrayList<>(snapshotNames);
        Collections.sort(remainingSnapshots);
        while (remainingSnapshots.isEmpty() == false) {
            deleteSnapshot(repoName, remainingSnapshots.removeLast(), false);
            assertEquals(remainingSnapshots, listAllSnapshotNames(repoName).stream().sorted().toList());
        }

        deleteRepository(repoName);
    }

    private List<String> listAllSnapshotNames(String repoName) throws IOException {
        return listAllSnapshots(repoName).<List<Map<String, ?>>>get("snapshots")
            .stream()
            .map(info -> (String) info.get("snapshot"))
            .toList();
    }

    public void testCreateReadOnlyRepo() throws IOException {
        final var repoName = randomRepoName();
        final var path = hdfsFixture().getExistingReadonlyRepoPath();
        registerHdfsRepository(hdfsUri0(), repoName, path, false, true);
        final var snapshots = listAllSnapshotNames(repoName);
        assertEquals("repository must contain exactly 1 snapshot", 1, snapshots.size());

        final var createSnapshotError = assertThrows(ResponseException.class, () -> createSnapshot(repoName, randomIdentifier(), true));
        assertEquals(400, createSnapshotError.getResponse().getStatusLine().getStatusCode());
        final var deleteSnapshotError = assertThrows(ResponseException.class, () -> deleteSnapshot(repoName, snapshots.getFirst(), true));
        assertEquals(400, deleteSnapshotError.getResponse().getStatusLine().getStatusCode());

        deleteRepository(repoName);
    }

    public void testRestore() throws IOException {
        final var repoName = randomRepoName();
        final var path = randomPath();
        registerHdfsRepository(hdfsUri0(), repoName, path, false, false);

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());

        ensureGreen(indexName);

        final var snapshotName = randomSnapshotName();
        createSnapshot(repoName, snapshotName, true);

        closeIndex(indexName);

        restoreSnapshot(repoName, snapshotName, true);

        final var indexRecovery = getIndexRecovery(indexName);
        final var shard0 = indexName + ".shards.0.";
        assertEquals("SNAPSHOT", indexRecovery.get(shard0 + "type"));
        assertEquals("DONE", indexRecovery.get(shard0 + "stage"));
        assertEquals(Integer.valueOf(1), indexRecovery.get(shard0 + "index.files.recovered"));
        assertTrue((int) indexRecovery.get(shard0 + "index.size.recovered_in_bytes") >= 0);
        assertEquals(Integer.valueOf(0), indexRecovery.get(shard0 + "index.files.reused"));
        assertEquals(Integer.valueOf(0), indexRecovery.get(shard0 + "index.size.reused_in_bytes"));

        deleteSnapshot(repoName, snapshotName, false);
        deleteRepository(repoName);
    }

    protected String hdfsUri0() {
        return hdfsUri(0);
    }

    protected String hdfsUri(int nodeId) {
        return "hdfs://localhost:" + hdfsFixture().getPort(nodeId);
    }

    protected void registerHdfsRepository(String uri, String repoName, String path, boolean verify, boolean readOnly) throws IOException {
        final var settings = Settings.builder().put("uri", uri).put("path", path).put(READONLY_SETTING_KEY, readOnly);
        final var principal = securityPrincipal();
        if (principal != null) {
            settings.put("security.principal", principal);
            settings.put("conf.dfs.data.transfer.protection", "authentication");
        }
        registerRepository(repoName, "hdfs", verify, settings.build());
    }

    @Nullable
    String securityPrincipal() {
        return null;
    }

    String basePath() {
        // meaningless but different bases
        return randomFrom("", randomIdentifier(), "/test", "/user/elasticsearch/test");
    }

    private String randomPath() {
        return basePath() + "/" + randomIdentifier();
    }
}
