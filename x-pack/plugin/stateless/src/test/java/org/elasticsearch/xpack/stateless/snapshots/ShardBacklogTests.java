/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.commitFiles;
import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.shardSnapshots;
import static org.hamcrest.Matchers.equalTo;

public class ShardBacklogTests extends ESTestCase {

    private static final ShardGeneration GENERATION = new ShardGeneration("gen");

    private static RepositoryShardFiles repositoryFiles(Object... files) {
        return RepositoryShardFiles.of(GENERATION, shardSnapshots(files));
    }

    public void testFilesTheRepositoryHoldsAreNotCounted() {
        final var commit = commitFiles("_0.cfs", 100L, "_1.cfs", 50L);
        assertThat(ShardBacklog.of(commit, repositoryFiles("_0.cfs", 100L, "_1.cfs", 50L)), equalTo(new ShardBacklog(0, 0)));
    }

    public void testFilesMissingFromTheRepositoryAreCounted() {
        final var commit = commitFiles("_0.cfs", 100L, "_1.cfs", 50L);
        assertThat(ShardBacklog.of(commit, repositoryFiles("_0.cfs", 100L)), equalTo(new ShardBacklog(50, 0)));
    }

    public void testAShardWithoutShardLevelMetadataCountsAllItsFiles() {
        final var commit = commitFiles("_0.cfs", 100L, "_1.cfs", 50L);
        assertThat(ShardBacklog.of(commit, RepositoryShardFiles.NONE), equalTo(new ShardBacklog(150, 0)));
    }

    public void testAFileWithTheSameNameButAnotherLengthIsNotInTheRepository() {
        final var commit = commitFiles("_0.cfs", 100L);
        assertThat(ShardBacklog.of(commit, repositoryFiles("_0.cfs", 99L)), equalTo(new ShardBacklog(100, 0)));
    }

    public void testAFileInTheRepositoryWithSeveralLengthsMatchesEither() {
        final var commit = commitFiles("segments_3", 10L);
        final var files = RepositoryShardFiles.of(GENERATION, shardSnapshots("segments_3", 20L, "segments_3", 10L));
        assertThat(ShardBacklog.of(commit, files), equalTo(new ShardBacklog(0, 0)));
    }

    public void testSmallFilesStoredInTheShardMetadataAreNotCounted() {
        final var commit = commitFiles("_0.cfs", 100L, "_0.si", 7L, "segments_4", 5L);
        assertThat(ShardBacklog.of(commit, RepositoryShardFiles.NONE), equalTo(new ShardBacklog(100, 12)));
        // but they are not counted as inlined either when the repository has them
        assertThat(ShardBacklog.of(commit, repositoryFiles("_0.si", 7L, "segments_4", 5L)), equalTo(new ShardBacklog(100, 0)));
    }

    private static IndexShardSnapshotStatus startedStatus(ShardGeneration generation, long processedBytes) {
        final var status = IndexShardSnapshotStatus.newInitializing(generation, 1);
        status.moveToStarted(1, 1, 1, 100, 100);
        status.addProcessedFile(processedBytes);
        return status;
    }

    public void testUploadedBytesOfARunningSnapshotAreSubtracted() {
        final var files = repositoryFiles("_0.cfs", 10L);
        final var backlog = new ShardBacklog(100, 0);
        assertThat(backlog.minusRunningSnapshots(files, List.of(startedStatus(GENERATION, 30))), equalTo(new ShardBacklog(70, 0)));
    }

    public void testTheInlinedFilesAreNotPartOfWhatIsSubtracted() {
        final var files = repositoryFiles("_0.cfs", 10L);
        final var backlog = new ShardBacklog(100, 12);
        // the processed size contains the 12 bytes that went into the shard metadata
        assertThat(backlog.minusRunningSnapshots(files, List.of(startedStatus(GENERATION, 42))), equalTo(new ShardBacklog(70, 12)));
        assertThat(backlog.minusRunningSnapshots(files, List.of(startedStatus(GENERATION, 12))), equalTo(new ShardBacklog(100, 12)));
    }

    public void testTheSubtractionNeverGoesBelowZero() {
        final var files = repositoryFiles("_0.cfs", 10L);
        assertThat(
            new ShardBacklog(20, 0).minusRunningSnapshots(files, List.of(startedStatus(GENERATION, 1000))),
            equalTo(new ShardBacklog(0, 0))
        );
    }

    public void testASnapshotThatStartedFromOtherFilesIsIgnored() {
        final var files = repositoryFiles("_0.cfs", 10L);
        final var backlog = new ShardBacklog(100, 0);
        assertThat(backlog.minusRunningSnapshots(files, List.of(startedStatus(new ShardGeneration("other"), 30))), equalTo(backlog));
    }

    public void testAShardWithoutShardLevelMetadataTakesInSnapshotsThatStartedFromNothing() {
        final var backlog = new ShardBacklog(100, 0);
        assertThat(
            backlog.minusRunningSnapshots(RepositoryShardFiles.NONE, List.of(startedStatus(ShardGenerations.NEW_SHARD_GEN, 30))),
            equalTo(new ShardBacklog(70, 0))
        );
    }

    public void testAQueuedSnapshotHasUploadedNothing() {
        final var files = repositoryFiles("_0.cfs", 10L);
        final var backlog = new ShardBacklog(100, 0);
        assertThat(
            backlog.minusRunningSnapshots(files, List.of(IndexShardSnapshotStatus.newInitializing(GENERATION, 1))),
            equalTo(backlog)
        );
    }

    public void testAFinishedSnapshotCountsUntilTheRepositoryPointsAtItsFiles() {
        final var files = repositoryFiles("_0.cfs", 10L);
        final var backlog = new ShardBacklog(100, 0);

        final var status = startedStatus(GENERATION, 100);
        status.moveToFinalize();
        final var newGeneration = new ShardGeneration("new");
        status.moveToDone(2, new ShardSnapshotResult(newGeneration, ByteSizeValue.ofBytes(100), 1));

        // the repository data is still at the old generation: the uploaded files are not in the file list we compare with
        assertThat(backlog.minusRunningSnapshots(files, List.of(status)), equalTo(new ShardBacklog(0, 0)));

        // the file list has caught up, it contains the uploaded files already and nothing must be subtracted a second time
        final var caughtUp = new RepositoryShardFiles(newGeneration, Set.of(new RepositoryShardFiles.FileKey("_1.cfs", 100)));
        assertThat(backlog.minusRunningSnapshots(caughtUp, List.of(status)), equalTo(backlog));
    }

    public void testAFailedSnapshotHasNoEffect() {
        final var files = repositoryFiles("_0.cfs", 10L);
        final var backlog = new ShardBacklog(100, 0);
        final var status = startedStatus(GENERATION, 50);
        status.moveToFailed(2, "boom");
        assertThat(backlog.minusRunningSnapshots(files, List.of(status)), equalTo(backlog));
    }
}
