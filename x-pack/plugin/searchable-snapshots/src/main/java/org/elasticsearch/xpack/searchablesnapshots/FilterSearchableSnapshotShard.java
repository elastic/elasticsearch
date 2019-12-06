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

public class FilterSearchableSnapshotShard extends SearchableSnapshotShard {

    private final SearchableSnapshotShard in;

    public FilterSearchableSnapshotShard(SearchableSnapshotShard in) {
        super(Objects.requireNonNull(in).getContext());
        this.in = in;
    }

    @Override
    public SearchableSnapshotContext getContext() {
        return in.getContext();
    }

    @Override
    public Map<String, FileInfo> listSnapshotFiles() throws IOException {
        return in.listSnapshotFiles();
    }

    @Override
    public ByteBuffer readSnapshotFile(String name, long position, int length) throws IOException {
        return in.readSnapshotFile(name, position, length);
    }
}
