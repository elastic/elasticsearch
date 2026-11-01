/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;

/**
 * Metadata about an object returned from directory listing. The {@code version} is the object's ETag, generation or
 * modification time as the listing reported it, or {@code null} when the listing reported none; it addresses the
 * footer caches (see {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject#version()}).
 */
public record StorageEntry(StoragePath path, long length, Instant lastModified, @Nullable String version) {
    public StorageEntry {
        if (lastModified == null) {
            lastModified = Instant.EPOCH;
        }
    }

    public StorageEntry(StoragePath path, long length, Instant lastModified) {
        this(path, length, lastModified, null);
    }
}
