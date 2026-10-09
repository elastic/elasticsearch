/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.util.Version;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SnapshotBacklogTestUtils {

    private SnapshotBacklogTestUtils() {}

    /**
     * @param files file name and length, in pairs
     */
    static Map<String, Long> commitFiles(Object... files) {
        final Map<String, Long> commitFiles = new HashMap<>();
        for (int i = 0; i < files.length; i += 2) {
            commitFiles.put((String) files[i], (Long) files[i + 1]);
        }
        return commitFiles;
    }

    /**
     * @param files file name and length, in pairs
     */
    static BlobStoreIndexShardSnapshots shardSnapshots(Object... files) {
        final List<FileInfo> fileInfos = new java.util.ArrayList<>();
        for (int i = 0; i < files.length; i += 2) {
            final var metadata = new StoreFileMetadata((String) files[i], (Long) files[i + 1], "checksum", Version.LATEST.toString());
            fileInfos.add(new FileInfo("__" + files[i], metadata, null));
        }
        return BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", fileInfos, null));
    }
}
