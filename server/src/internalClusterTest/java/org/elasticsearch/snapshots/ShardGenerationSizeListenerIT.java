/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.internal.ShardGenerationSizeListener;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ShardGenerationSizeListenerIT extends AbstractSnapshotIntegTestCase {

    private record Call(String repository, IndexId indexId, int shardId, ShardGeneration generation, long retainedBytes) {}

    /**
     * A ShardGenerationSizeListener registered through SPI (see META-INF/services) that lets the test set the listener at runtime. It does
     * nothing until the test sets one, so that it is harmless to every other test that finds it on the classpath.
     */
    public static class DelegatingShardGenerationSizeListener implements ShardGenerationSizeListener {
        private static volatile ShardGenerationSizeListener delegate = ShardGenerationSizeListener.NOOP;

        @Override
        public void onShardGenerationSize(
            ProjectRepo projectRepo,
            IndexId indexId,
            int shardId,
            ShardGeneration generation,
            long retainedBytes
        ) {
            delegate.onShardGenerationSize(projectRepo, indexId, shardId, generation, retainedBytes);
        }
    }

    private static final List<Call> CALLS = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean FAIL = new AtomicBoolean();

    @Before
    public void installRecordingListener() {
        CALLS.clear();
        FAIL.set(false);
        DelegatingShardGenerationSizeListener.delegate = (projectRepo, indexId, shardId, generation, retainedBytes) -> {
            CALLS.add(new Call(projectRepo.name(), indexId, shardId, generation, retainedBytes));
            if (FAIL.get()) {
                throw new IllegalStateException("simulated listener failure");
            }
        };
    }

    @After
    public void resetListener() {
        DelegatingShardGenerationSizeListener.delegate = ShardGenerationSizeListener.NOOP;
    }

    public void testListenerSeesCommittedGenerationAfterSnapshotAndDeletion() throws IOException {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repo = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repo, "fs", repoPath);
        final String index = "test-index";
        createIndexWithContent(index);

        createFullSnapshot(repo, "snap-1");
        assertThat(CALLS, hasSize(1));
        final Call afterFirst = CALLS.get(0);
        assertThat(afterFirst.repository(), equalTo(repo));
        assertThat(afterFirst.retainedBytes(), greaterThan(0L));
        assertThat(afterFirst.retainedBytes(), equalTo(dataBlobBytes(repoPath, afterFirst)));
        assertThat(committedGeneration(repo, afterFirst.indexId()), equalTo(afterFirst.generation()));

        indexDoc(index, "another_id", "foo", "baz");
        createFullSnapshot(repo, "snap-2");
        final Call afterSecond = CALLS.get(CALLS.size() - 1);
        assertThat(afterSecond.retainedBytes(), greaterThan(afterFirst.retainedBytes()));
        assertThat(afterSecond.retainedBytes(), equalTo(dataBlobBytes(repoPath, afterSecond)));
        assertThat(committedGeneration(repo, afterSecond.indexId()), equalTo(afterSecond.generation()));

        CALLS.clear();
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repo, "snap-1").get());
        final Call afterDelete = CALLS.stream().filter(c -> c.indexId().equals(afterSecond.indexId())).findFirst().orElseThrow();
        assertThat(afterDelete.retainedBytes(), equalTo(dataBlobBytes(repoPath, afterDelete)));
        assertThat(committedGeneration(repo, afterDelete.indexId()), equalTo(afterDelete.generation()));
    }

    public void testListenerSeesGenerationWrittenByClone() throws IOException {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repo = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repo, "fs", repoPath);
        final String index = "test-index";
        createIndexWithContent(index);
        createSnapshot(repo, "snap-1", List.of(index));
        final long retainedBeforeClone = CALLS.get(CALLS.size() - 1).retainedBytes();

        CALLS.clear();
        assertAcked(clusterAdmin().prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repo, "snap-1", "clone-1").setIndices(index).get());

        assertThat(CALLS, hasSize(1));
        final Call afterClone = CALLS.get(0);
        // A clone shares every file with its source, so it retains nothing more.
        assertThat(afterClone.retainedBytes(), equalTo(retainedBeforeClone));
        assertThat(afterClone.retainedBytes(), equalTo(dataBlobBytes(repoPath, afterClone)));
        assertThat(committedGeneration(repo, afterClone.indexId()), equalTo(afterClone.generation()));
    }

    public void testListenerFailureDoesNotFailSnapshotOrDeletion() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repo = "test-repo";
        createRepository(repo, "fs");
        createIndexWithContent("test-index");

        FAIL.set(true);
        createFullSnapshot(repo, "snap-1");
        createFullSnapshot(repo, "snap-2");
        assertThat(CALLS, hasSize(2));

        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repo, "snap-1").get());
        assertThat(getRepositoryData(repo).getSnapshotIds(), hasSize(1));
    }

    /** The listener is called for every repository, so it is up to the implementation to ignore the ones it has no use for. */
    public void testListenerIsToldAboutEveryRepository() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository("first", "fs");
        createRepository("second", "fs");
        createIndexWithContent("test-index");

        createFullSnapshot("first", "snap-1");
        createFullSnapshot("second", "snap-1");

        assertThat(CALLS.stream().map(Call::repository).toList(), contains("first", "second"));
    }

    /**
     * The notification must not wait for the data files of the shard snapshot to be uploaded. If it did, it would have to run from the
     * listener that lives until every upload has finished, and that listener would keep the shard's whole snapshot history reachable
     * for the duration of the uploads, for every shard on the node at once.
     */
    public void testListenerIsCalledBeforeDataFilesAreUploaded() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repo = "test-repo";
        createRepository(repo, "mock", Settings.builder().put("location", randomRepoPath()));
        createIndexWithContent("test-index");

        blockAllDataNodes(repo);
        final var snapshot = startFullSnapshot(repo, "snap-1");
        waitForBlockOnAnyDataNode(repo);
        assertThat("the listener was called while the data files were still being uploaded", CALLS, hasSize(1));

        unblockAllDataNodes(repo);
        assertSuccessful(snapshot);
        assertThat(CALLS, hasSize(1));
    }

    private ShardGeneration committedGeneration(String repo, IndexId indexId) {
        final RepositoryData repositoryData = getRepositoryData(repo);
        return repositoryData.shardGenerations().getGens(indexId).get(0);
    }

    /** The total length of the data blobs in the shard's directory of an {@code fs} repository. */
    private static long dataBlobBytes(Path repoPath, Call call) throws IOException {
        final Path shardPath = repoPath.resolve("indices").resolve(call.indexId().getId()).resolve(Integer.toString(call.shardId()));
        try (Stream<Path> blobs = Files.list(shardPath)) {
            return blobs.filter(blob -> blob.getFileName().toString().startsWith("__")).mapToLong(blob -> {
                try {
                    return Files.size(blob);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }).sum();
        }
    }
}
