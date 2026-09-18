/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.datasources.StorageEntry;

import java.util.List;

/**
 * The immediate children of a directory-like prefix, split into data objects and subdirectories — what
 * {@link StorageProvider#listChildren} returns and partition-aware listing pruning descends: a folder a
 * filter excludes is never listed, not merely filtered.
 *
 * <p>{@code directories} carries each subdirectory without a trailing separator, so
 * {@link StoragePath#objectName()} yields the folder's name. {@code files} holds only real data objects;
 * directory placeholder keys (a console's {@code folder/} marker) are the provider's to drop.
 */
public record StorageChildren(List<StorageEntry> files, List<StoragePath> directories) {

    public StorageChildren {
        files = List.copyOf(files);
        directories = List.copyOf(directories);
    }
}
