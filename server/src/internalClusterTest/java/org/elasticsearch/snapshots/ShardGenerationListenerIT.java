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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerationListener;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ShardGenerationListenerIT extends AbstractSnapshotIntegTestCase {

    private record Call(String repository, IndexId indexId, int shardId, ShardGeneration generation, BlobStoreIndexShardSnapshots shards) {}

    public static class TestPlugin extends Plugin implements RepositoryPlugin {
        static final List<Call> CALLS = new CopyOnWriteArrayList<>();
        static final AtomicBoolean FAIL = new AtomicBoolean();

        @Override
        public ShardGenerationListener getShardGenerationListener() {
            return (projectRepo, indexId, shardId, generation, shards) -> {
                CALLS.add(new Call(projectRepo.name(), indexId, shardId, generation, shards));
                if (FAIL.get()) {
                    throw new IllegalStateException("simulated listener failure");
                }
            };
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class);
    }

    @Before
    public void resetListenerState() {
        TestPlugin.CALLS.clear();
        TestPlugin.FAIL.set(false);
    }

    public void testListenerSeesCommittedGenerationAfterSnapshotAndDeletion() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repo = "test-repo";
        createRepository(repo, "fs");
        final String index = "test-index";
        createIndexWithContent(index);

        createFullSnapshot(repo, "snap-1");
        assertThat(TestPlugin.CALLS, hasSize(1));
        final Call afterFirst = TestPlugin.CALLS.get(0);
        assertThat(afterFirst.repository(), equalTo(repo));
        assertThat(afterFirst.shards().snapshots().stream().map(s -> s.snapshot()).toList(), contains("snap-1"));
        assertThat(committedGeneration(repo, afterFirst.indexId()), equalTo(afterFirst.generation()));

        indexDoc(index, "another_id", "foo", "baz");
        createFullSnapshot(repo, "snap-2");
        final Call afterSecond = TestPlugin.CALLS.get(TestPlugin.CALLS.size() - 1);
        assertThat(afterSecond.shards().snapshots(), hasSize(2));
        assertThat(committedGeneration(repo, afterSecond.indexId()), equalTo(afterSecond.generation()));

        TestPlugin.CALLS.clear();
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repo, "snap-1").get());
        final Call afterDelete = TestPlugin.CALLS.stream().filter(c -> c.indexId().equals(afterSecond.indexId())).findFirst().orElseThrow();
        assertThat(afterDelete.shards().snapshots().stream().map(s -> s.snapshot()).toList(), contains("snap-2"));
        assertThat(committedGeneration(repo, afterDelete.indexId()), equalTo(afterDelete.generation()));
    }

    public void testListenerFailureDoesNotFailSnapshotOrDeletion() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repo = "test-repo";
        createRepository(repo, "fs");
        createIndexWithContent("test-index");

        TestPlugin.FAIL.set(true);
        createFullSnapshot(repo, "snap-1");
        createFullSnapshot(repo, "snap-2");
        assertThat(TestPlugin.CALLS, hasSize(2));

        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repo, "snap-1").get());
        assertThat(getRepositoryData(repo).getSnapshotIds(), hasSize(1));
    }

    private ShardGeneration committedGeneration(String repo, IndexId indexId) {
        final RepositoryData repositoryData = getRepositoryData(repo);
        return repositoryData.shardGenerations().getGens(indexId).get(0);
    }
}
