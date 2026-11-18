/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;

/**
 * A single object's cheap physical metadata: byte {@code length} and last-modified epoch millis, as last
 * read from the store, and the object {@code version} the store reported with them (see
 * {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject#version()}).
 * <p>
 * mtime is the version token that rebuilds the {@link SchemaCacheKey} and populates the resolved
 * {@code StorageEntry}; it is not a second freshness clock. {@code length} travels with it because the
 * single-file resolve rebuilds its singleton file list from both. The {@code version} addresses the footer
 * caches, and like the rest it is held for the listing clock, so an overwrite is seen when that clock ticks.
 * <p>
 * Held in the file-metadata cache under the listing clock, keyed by path and storage identity, so the entry
 * is shared by every data source reaching the object through the same storage settings.
 */
public record FileMetadata(long length, long mtimeMillis, @Nullable String version) {

    public FileMetadata(long length, long mtimeMillis) {
        this(length, mtimeMillis, null);
    }
}
