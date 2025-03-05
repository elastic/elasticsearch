/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Objects;

/**
 * Parameters of a repository-verity-integrity request.
 *
 * @param repository the name of the repository whose integrity to verify.
 * @param metaThreadPoolConcurrency the number of concurrent tasks to execute on the {@link ThreadPool.Names#SNAPSHOT_META} pool, or
 *                                  {@code 0} to use a sensible default.
 * @param blobThreadPoolConcurrency the number of concurrent tasks to execute on the {@link ThreadPool.Names#SNAPSHOT} pool, or {@code 0}
 *                                  to use a sensible default.
 * @param snapshotVerificationConcurrency the number of snapshots to verify concurrently, or {@code 0} to use a sensible default.
 * @param indexVerificationConcurrency the number of indices to verify concurrently, or {@code 0} to use a sensible default.
 * @param indexSnapshotVerificationConcurrency the number of snapshots to verify concurrently for each index, or {@code 0} to use a sensible
 *                                             default.
 * @param maxFailedShardSnapshots the maximum number of shard snapshots failures to track - we must build a list of all of them in memory
 *                                to avoid reporting spurious anomalies, and this can be overwhelming in a very broken repository.
 * @param verifyBlobContents whether to verify the contents of each data blob (which is very expensive).
 * @param maxBytesPerSec rate limit to use for blob contents verification.
 */
public record RepositoryVerifyIntegrityParams(
    String repository,
    int metaThreadPoolConcurrency,
    int blobThreadPoolConcurrency,
    int snapshotVerificationConcurrency,
    int indexVerificationConcurrency,
    int indexSnapshotVerificationConcurrency,
    int maxFailedShardSnapshots,
    boolean verifyBlobContents,
    ByteSizeValue maxBytesPerSec
) implements Writeable {

    public static final String MAX_FAILED_SHARD_SNAPSHOTS = "max_failed_shard_snapshots";

    public RepositoryVerifyIntegrityParams {
        Objects.requireNonNull(repository, "repository");
        requireNonNegative("meta_thread_pool_concurrency", metaThreadPoolConcurrency);
        requireNonNegative("blob_thread_pool_concurrency", blobThreadPoolConcurrency);
        requireNonNegative("snapshot_verification_concurrency", snapshotVerificationConcurrency);
        requireNonNegative("index_verification_concurrency", indexVerificationConcurrency);
        requireNonNegative("index_snapshot_verification_concurrency", indexSnapshotVerificationConcurrency);
        requireNonNegative(MAX_FAILED_SHARD_SNAPSHOTS, maxFailedShardSnapshots);
        if (maxBytesPerSec.getBytes() < 1) {
            throw new IllegalArgumentException("invalid rate limit");
        }
    }

    private static void requireNonNegative(String name, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("argument [" + name + "] must be at least [0]");
        }
    }

    RepositoryVerifyIntegrityParams(RestRequest restRequest) {
        this(
            restRequest.param("repository"),
            restRequest.paramAsInt("meta_thread_pool_concurrency", 0),
            restRequest.paramAsInt("blob_thread_pool_concurrency", 0),
            restRequest.paramAsInt("snapshot_verification_concurrency", 0),
            restRequest.paramAsInt("index_verification_concurrency", 0),
            restRequest.paramAsInt("index_snapshot_verification_concurrency", 0),
            restRequest.paramAsInt(MAX_FAILED_SHARD_SNAPSHOTS, 0),
            restRequest.paramAsBoolean("verify_blob_contents", false),
            restRequest.paramAsSize("max_bytes_per_sec", ByteSizeValue.ofMb(40))
        );
    }

    RepositoryVerifyIntegrityParams(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readBoolean(),
            ByteSizeValue.readFrom(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeVInt(metaThreadPoolConcurrency);
        out.writeVInt(blobThreadPoolConcurrency);
        out.writeVInt(snapshotVerificationConcurrency);
        out.writeVInt(indexVerificationConcurrency);
        out.writeVInt(indexSnapshotVerificationConcurrency);
        out.writeVInt(maxFailedShardSnapshots);
        out.writeBoolean(verifyBlobContents);
        maxBytesPerSec.writeTo(out);
    }

    public RepositoryVerifyIntegrityParams withResolvedDefaults(ThreadPool.Info metadataThreadPoolInfo) {
        if (metaThreadPoolConcurrency > 0
            && blobThreadPoolConcurrency > 0
            && snapshotVerificationConcurrency > 0
            && indexVerificationConcurrency > 0
            && indexSnapshotVerificationConcurrency > 0
            && maxFailedShardSnapshots > 0) {
            return this;
        }

        final var maxThreads = Math.max(1, metadataThreadPoolInfo.getMax());
        final var halfMaxThreads = Math.max(1, maxThreads / 2);
        return new RepositoryVerifyIntegrityParams(
            repository,
            metaThreadPoolConcurrency > 0 ? metaThreadPoolConcurrency : halfMaxThreads,
            blobThreadPoolConcurrency > 0 ? blobThreadPoolConcurrency : 1,
            snapshotVerificationConcurrency > 0 ? snapshotVerificationConcurrency : halfMaxThreads,
            indexVerificationConcurrency > 0 ? indexVerificationConcurrency : maxThreads,
            indexSnapshotVerificationConcurrency > 0 ? indexSnapshotVerificationConcurrency : 1,
            maxFailedShardSnapshots > 0 ? maxFailedShardSnapshots : 10000,
            verifyBlobContents,
            maxBytesPerSec
        );
    }
}
