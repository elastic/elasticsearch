/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class StatelessCommitServiceTests extends ESTestCase {

    private final StatelessCommitService commitService = new StatelessCommitService(
        () -> "fake_node_ephemeral_id",
        new ThreadContext(Settings.EMPTY)
    );

    public void testUploadFileOfUnregisteredShardThrowsExceptions() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        commitService.register(shardId);
        Set<String> commit1 = Set.of("file-name1");
        commitService.markNewCommit(shardId, commit1, commit1);
        commitService.markFileUploaded(shardId, "file-name1", new BlobLocation(1, "1", 0, 10));
        Set<String> commit2 = Set.of("file-name1", "file-name2");
        commitService.markNewCommit(shardId, commit2, Set.of("file-name2"));
        commitService.unregister(shardId);
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

    public void testWaitForGeneration() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        commitService.register(shardId);

        Map<String, BlobLocation> fileMap = Map.of("file-name1", new BlobLocation(0, "blob-location", 0, 10));
        commitService.markNewCommit(shardId, fileMap.keySet(), fileMap.keySet());

        PlainActionFuture<Void> generation1Future = PlainActionFuture.newFuture();
        PlainActionFuture<Void> generation2Future = PlainActionFuture.newFuture();
        PlainActionFuture<Void> generation3Future = PlainActionFuture.newFuture();
        commitService.addOrNotify(shardId, 1, generation1Future);
        commitService.addOrNotify(shardId, 2, generation2Future);
        commitService.addOrNotify(shardId, 3, generation3Future);

        assertFalse(generation1Future.isDone());
        assertFalse(generation2Future.isDone());
        assertFalse(generation3Future.isDone());

        StatelessCompoundCommit commit = new StatelessCompoundCommit(shardId, 2, 0, "node-id", fileMap);
        commitService.markCommitUploaded(shardId, commit);

        assertTrue(generation1Future.isDone());
        assertTrue(generation2Future.isDone());
        assertFalse(generation3Future.isDone());

        PlainActionFuture<Void> immediateFuture = PlainActionFuture.newFuture();
        commitService.addOrNotify(shardId, 2, immediateFuture);

        assertTrue(immediateFuture.isDone());

        commitService.markCommitUploaded(shardId, new StatelessCompoundCommit(shardId, 3, 0, "node-id", Collections.emptyMap()));

        assertTrue(generation3Future.isDone());

        // Make sure none throw exceptions
        generation1Future.actionGet();
        generation2Future.actionGet();
        generation3Future.actionGet();
        immediateFuture.actionGet();
    }

    public void testWaitForGenerationFailsForClosedShard() {
        ShardId shardId = new ShardId("index", "uuid", 0);

        expectThrows(AlreadyClosedException.class, () -> commitService.addOrNotify(shardId, 1, PlainActionFuture.newFuture()));

        commitService.register(shardId);

        Map<String, BlobLocation> fileMap = Map.of("file-name1", new BlobLocation(0, "blob-location", 0, 10));
        commitService.markNewCommit(shardId, fileMap.keySet(), fileMap.keySet());

        PlainActionFuture<Void> failedFuture = PlainActionFuture.newFuture();
        commitService.addOrNotify(shardId, 1, failedFuture);

        commitService.unregister(shardId);

        expectThrows(AlreadyClosedException.class, failedFuture::actionGet);
    }
}
