/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/** Opens tables for the tests: the owner is a fixed index, and the history is the one that a test names or {@link #HISTORY}. */
public final class TestTables {

    public static final String INDEX = "logs";
    public static final String INDEX_UUID = "index-uuid";
    public static final String HISTORY = "history-a";

    private TestTables() {}

    public static ShardTable open(BlobStore store, TableLocation location, int shard) {
        return open(store, location, shard, HISTORY, null);
    }

    public static ShardTable open(BlobStore store, TableLocation location, int shard, String history) {
        return open(store, location, shard, history, null);
    }

    public static ShardTable open(BlobStore store, TableLocation location, int shard, @Nullable CatalogRegistration registration) {
        return open(store, location, shard, HISTORY, registration);
    }

    public static ShardTable open(
        BlobStore store,
        TableLocation location,
        int shard,
        String history,
        @Nullable CatalogRegistration registration
    ) {
        return ShardTable.open(store, location, shard, INDEX, INDEX_UUID, history, registration);
    }

    /** The container of the file at {@code fileLocation}, a location below the root of the table. */
    private static BlobContainer containerOf(BlobStore store, TableLocation location, String fileLocation) throws IOException {
        final String[] relative = relative(location, fileLocation);
        BlobPath path = location.blobPath();
        for (String segment : Arrays.copyOf(relative, relative.length - 1)) {
            path = path.add(segment);
        }
        return store.blobContainer(path);
    }

    private static String[] relative(TableLocation location, String fileLocation) {
        if (fileLocation.startsWith(location.rootUri() + "/") == false) {
            throw new IllegalArgumentException("[" + fileLocation + "] is not below [" + location.rootUri() + "]");
        }
        return fileLocation.substring(location.rootUri().length() + 1).split("/");
    }

    /** The bytes of the data file at {@code fileLocation}. */
    public static InputStream read(BlobStore store, TableLocation location, String fileLocation) throws IOException {
        final String[] relative = relative(location, fileLocation);
        return containerOf(store, location, fileLocation).readBlob(RepositoryFileIO.PURPOSE, relative[relative.length - 1]);
    }

    /** The names of the blobs in the directory of the file at {@code fileLocation} (the file need not exist). */
    public static java.util.Set<String> blobsNextTo(BlobStore store, TableLocation location, String fileLocation) throws IOException {
        return containerOf(store, location, fileLocation).listBlobs(RepositoryFileIO.PURPOSE).keySet();
    }
}
