/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.datasources.StorageIterator;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * Abstraction for accessing objects in external storage systems.
 * Implementations handle specific protocols (HTTP, S3, GCS, local, etc.).
 * This is a read-only interface focused on ESQL's needs for querying external data.
 */
public interface StorageProvider extends Closeable {

    /** Creates a StorageObject for reading. The path must be a valid object path. */
    StorageObject newObject(StoragePath path);

    /** Creates a StorageObject with pre-known length (avoids HEAD request for remote objects). */
    StorageObject newObject(StoragePath path, long length);

    /** Creates a StorageObject with pre-known length and modification time. */
    StorageObject newObject(StoragePath path, long length, Instant lastModified);

    /**
     * Lists objects under a prefix. For blob storage, lists all objects with the given prefix.
     * Returns an iterator to support lazy loading of large directories.
     *
     * @param prefix the prefix path to list under
     * @param recursive if true, recurse into subdirectories; if false, list only immediate children
     */
    StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException;

    /** Checks if an object exists at the given path. */
    boolean exists(StoragePath path) throws IOException;

    /** Returns the URI schemes this provider handles (e.g., ["http", "https"]). */
    List<String> supportedSchemes();
}
