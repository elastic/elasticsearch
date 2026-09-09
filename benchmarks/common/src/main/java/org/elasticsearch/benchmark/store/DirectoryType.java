/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.store;

/**
 * The Lucene {@code Directory} flavours that benchmarks may exercise, used as a JMH
 * {@code @Param} value so a single benchmark can compare read paths across multiple
 * implementations.
 *
 * <p>Kept as a pure enum on purpose. Instantiating the concrete {@code Directory}
 * requires implementations that live in different modules — {@code NIO}/{@code MMAP}
 * come from Lucene, {@code SNAP} from {@code :x-pack:plugin:searchable-snapshots}
 * test artifacts, and {@code STATELESS_INDEX_LOCAL} from {@code :x-pack:plugin:stateless}
 * test artifacts. Placing that factory logic here would force those dependencies onto
 * every consumer of {@code :benchmarks:common}, which is deliberately kept minimal.
 * Each benchmark suite instead owns its own factory (or inline switch) in the module
 * where it already carries the required dependencies.
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
    STATELESS_INDEX_LOCAL
}
