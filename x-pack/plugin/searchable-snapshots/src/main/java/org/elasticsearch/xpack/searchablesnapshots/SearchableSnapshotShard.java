/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link SearchableSnapshotShard} exposes shard files stored in a snapshot.
 */
public abstract class SearchableSnapshotShard {

    private final SearchableSnapshotContext context;

    protected SearchableSnapshotShard(final SearchableSnapshotContext context) {
        this.context = Objects.requireNonNull(context);
    }

    /**
     * @return the shard snapshot context
     */
    public SearchableSnapshotContext getContext() {
        return context;
    }

    /**
     * Returns all the files contained in the shard snapshot
     *
     * @return a {@link Map} of {@link String} file names with their corresponding {@link FileInfo}
     *
     * @throws IOException if something went wrong during snapshot files listing
     */
    public abstract Map<String, FileInfo> listSnapshotFiles() throws IOException;

    /**
     * Reads {@code length} bytes starting from the position {@code position} of the file named {@code name}
     *
     * @param name     the name of the file to read
     * @param position the starting position in the file to start reading from
     * @param length   the number of bytes to read
     * @return a {@link ByteBuffer} instance that can used to read the file
     *
     * @throws IOException if something went wrong during snapshot file reading
     */
    public abstract ByteBuffer readSnapshotFile(String name, long position, int length) throws IOException;
}
