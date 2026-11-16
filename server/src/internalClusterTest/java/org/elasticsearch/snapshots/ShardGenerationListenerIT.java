/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.plugins.internal.ShardGenerationListener;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ShardGenerationListenerIT extends AbstractSnapshotIntegTestCase {

    private record Call(String repository, IndexId indexId, int shardId, ShardGeneration generation, BlobStoreIndexShardSnapshots shards) {}

    /**
     * A ShardGenerationListener registered through SPI (see META-INF/services) that lets the test set the listener at runtime. It does
     * nothing until the test sets one, so that it is harmless to every other test that finds it on the classpath.
     */
    public static class DelegatingShardGenerationListener implements ShardGenerationListener {
        private static volatile ShardGenerationListener delegate = ShardGenerationListener.NOOP;

        @Override
        public void onShardGenerationWritten(
            ProjectRepo projectRepo,
            IndexId indexId,
            int shardId,
            ShardGeneration generation,
            BlobStoreIndexShardSnapshots shardSnapshots
        ) {
            delegate.onShardGenerationWritten(projectRepo, indexId, shardId, generation, shardSnapshots);
        }
    }

    private static final List<Call> CALLS = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean FAIL = new AtomicBoolean();

    @Before
    public void installRecordingListener() {
        CALLS.clear();
        FAIL.set(false);
        DelegatingShardGenerationListener.delegate = (projectRepo, indexId, shardId, generation, shards) -> {
            CALLS.add(new Call(projectRepo.name(), indexId, shardId, generation, shards));
            if (FAIL.get()) {
                throw new IllegalStateException("simulated listener failure");
            }
        };
    }

    @After
    public void resetListener() {
        DelegatingShardGenerationListener.delegate = ShardGenerationListener.NOOP;
    }

    public void testListenerSeesCommittedGenerationAfterSnapshotAndDeletion() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repo = "test-repo";
        createRepository(repo, "fs");
        final String index = "test-index";
        createIndexWithContent(index);

        createFullSnapshot(repo, "snap-1");
        assertThat(CALLS, hasSize(1));
        final Call afterFirst = CALLS.get(0);
        assertThat(afterFirst.repository(), equalTo(repo));
        assertThat(afterFirst.shards().snapshots().stream().map(s -> s.snapshot()).toList(), contains("snap-1"));
        assertThat(committedGeneration(repo, afterFirst.indexId()), equalTo(afterFirst.generation()));

        indexDoc(index, "another_id", "foo", "baz");
        createFullSnapshot(repo, "snap-2");
        final Call afterSecond = CALLS.get(CALLS.size() - 1);
        assertThat(afterSecond.shards().snapshots(), hasSize(2));
        assertThat(committedGeneration(repo, afterSecond.indexId()), equalTo(afterSecond.generation()));

        CALLS.clear();
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repo, "snap-1").get());
        final Call afterDelete = CALLS.stream().filter(c -> c.indexId().equals(afterSecond.indexId())).findFirst().orElseThrow();
        assertThat(afterDelete.shards().snapshots().stream().map(s -> s.snapshot()).toList(), contains("snap-2"));
        assertThat(committedGeneration(repo, afterDelete.indexId()), equalTo(afterDelete.generation()));
    }

    public void testListenerSeesGenerationWrittenByClone() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repo = "test-repo";
        createRepository(repo, "fs");
        final String index = "test-index";
        createIndexWithContent(index);
        createSnapshot(repo, "snap-1", List.of(index));

        CALLS.clear();
        assertAcked(clusterAdmin().prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repo, "snap-1", "clone-1").setIndices(index).get());

        assertThat(CALLS, hasSize(1));
        final Call afterClone = CALLS.get(0);
        assertThat(afterClone.shards().snapshots().stream().map(s -> s.snapshot()).toList(), containsInAnyOrder("snap-1", "clone-1"));
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

    private ShardGeneration committedGeneration(String repo, IndexId indexId) {
        final RepositoryData repositoryData = getRepositoryData(repo);
        return repositoryData.shardGenerations().getGens(indexId).get(0);
    }
}
