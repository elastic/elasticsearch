/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;
import org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The {@link Directory} implementations a benchmark can be run against, so that a benchmark can compare read paths rather
 * than only the code that reads through them.
 */
public enum DirectoryType {
    NIO,
    MMAP,
    /**
     * A searchable snapshot: the file is materialized into snapshot infrastructure when its {@code IndexOutput} is closed,
     * and read back through the shared blob cache. Write-once: a file cannot be reopened for writing.
     */
    SNAP,
    /**
     * A stateless indexing node reading a file it has just written and not yet uploaded to the object store. Reads go
     * through a {@code ReopeningIndexInput}.
     */
    STATELESS_INDEX_LOCAL;

    /**
     * Creates a directory of this type rooted at {@code root}. The returned directory owns everything it needs, so closing
     * it releases any infrastructure the type required.
     */
    public Directory newDirectory(Path root) throws IOException {
        return switch (this) {
            case NIO -> new NIOFSDirectory(root);
            case MMAP -> new MMapDirectory(root);
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(root);
            case STATELESS_INDEX_LOCAL -> StatelessDirectoryFactory.newIndexDirectory(root);
        };
    }
}
