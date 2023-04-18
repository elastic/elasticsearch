/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class StatelessCommitServiceTests extends ESTestCase {

    private final StatelessCommitService commitService = new StatelessCommitService();

    public void testUploadFileOfUnregisteredShardThrowsExceptions() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        commitService.register(shardId);
        Set<String> commit1 = Set.of("file-name1");
        commitService.markNewCommit(shardId, commit1, commit1);
        commitService.markFileUploaded(shardId, "file-name1", new BlobLocation(1, "1", 0, 10));
        Set<String> commit2 = Set.of("file-name1", "file-name2");
        commitService.markNewCommit(shardId, commit2, Set.of("file-name2"));
        commitService.unregisterShard(shardId);
        expectThrows(
            AlreadyClosedException.class,
            () -> commitService.markFileUploaded(shardId, "file-name2", new BlobLocation(1, "2", 0, 20))
        );
    }

    public void testMarkAndResolveFiles() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        commitService.register(shardId);
        Set<String> commit1 = Set.of("file-name1");
        commitService.markNewCommit(shardId, commit1, commit1);
        Set<String> commit2 = Set.of("file-name1", "file-name2");
        commitService.markNewCommit(shardId, commit2, Set.of("file-name2"));
        commitService.markFileUploaded(shardId, "file-name2", new BlobLocation(1, "2", 0, 20));
        List<String> missing = commitService.resolveMissingFiles(shardId, commit2);
        assertThat(missing.size(), equalTo(1));
        assertThat(missing, contains("file-name1"));
    }

    public void testMapIsPrunedOnIndexDelete() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        commitService.register(shardId);
        Set<String> commit1 = Set.of("file-name1");
        commitService.markNewCommit(shardId, commit1, commit1);
        commitService.markFileUploaded(shardId, "file-name1", new BlobLocation(1, "1", 0, 10));
        Set<String> commit2 = Set.of("file-name1", "file-name2");
        commitService.markNewCommit(shardId, commit2, Set.of("file-name2"));
        commitService.markFileUploaded(shardId, "file-name2", new BlobLocation(1, "2", 0, 20));
        Set<String> commit3 = Set.of("file-name2", "file-name3");
        commitService.markNewCommit(shardId, commit3, Set.of("file-name3"));
        commitService.markFileUploaded(shardId, "file-name3", new BlobLocation(1, "3", 0, 30));

        Set<String> files = commitService.getFileToBlobFile(shardId).keySet();

        assertThat(files.size(), equalTo(3));
        assertThat(files, containsInAnyOrder("file-name1", "file-name2", "file-name3"));

        commitService.markCommitDeleted(shardId, commit1);
        files = commitService.getFileToBlobFile(shardId).keySet();

        assertThat(files.size(), equalTo(3));
        assertThat(files, containsInAnyOrder("file-name1", "file-name2", "file-name3"));

        commitService.markCommitDeleted(shardId, commit2);
        files = commitService.getFileToBlobFile(shardId).keySet();

        assertThat(files.size(), equalTo(2));
        assertThat(files, containsInAnyOrder("file-name2", "file-name3"));

        commitService.markCommitDeleted(shardId, commit3);
        files = commitService.getFileToBlobFile(shardId).keySet();
        assertThat(files.size(), equalTo(0));
    }
}
