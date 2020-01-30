/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

import java.nio.file.Path;
import java.util.Objects;

public class CacheKey {

    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final ShardId shardId;
    private final Path cacheDir;
    private final String fileName;
    private final long fileLength;

    CacheKey(SnapshotId snapshotId, IndexId indexId, ShardId shardId, Path cacheDir, String fileName, long fileLength) {
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indexId = Objects.requireNonNull(indexId);
        this.shardId = Objects.requireNonNull(shardId);
        this.cacheDir = Objects.requireNonNull(cacheDir);
        this.fileName = Objects.requireNonNull(fileName);
        this.fileLength = fileLength;
    }

    String getFileName() {
        return fileName;
    }

    long getFileLength() {
        return fileLength;
    }

    Path getCacheDir() {
        return cacheDir;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CacheKey cacheKey = (CacheKey) o;
        return fileLength == cacheKey.fileLength
            && Objects.equals(snapshotId, cacheKey.snapshotId)
            && Objects.equals(indexId, cacheKey.indexId)
            && Objects.equals(shardId, cacheKey.shardId)
            && Objects.equals(cacheDir, cacheKey.cacheDir)
            && Objects.equals(fileName, cacheKey.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, indexId, shardId, cacheDir, fileName, fileLength);
    }

    @Override
    public String toString() {
        return "[" +
            "snapshotId=" + snapshotId +
            ", indexId=" + indexId +
            ", shardId=" + shardId +
            ", fileName='" + fileName + '\'' +
            ", fileLength=" + fileLength +
            ", cacheDir=" + cacheDir +
            ']';
    }
}
