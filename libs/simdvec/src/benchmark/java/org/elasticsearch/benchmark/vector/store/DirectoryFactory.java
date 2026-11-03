/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.benchmark.store.DirectoryType;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;
import org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Constructs the concrete {@link Directory} implementation named by a {@link DirectoryType} for
 * the vector benchmarks in this source set.
 */
public final class DirectoryFactory {

    private DirectoryFactory() {}

    public static Directory newDirectory(DirectoryType type, Path root) throws IOException {
        return switch (type) {
            case NIO -> new NIOFSDirectory(root);
            case MMAP -> new MMapDirectory(root);
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(root);
            case STATELESS_INDEX_LOCAL -> StatelessDirectoryFactory.newIndexDirectory(root);
        };
    }
}
