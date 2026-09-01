/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.function.Consumer;

/**
 * Pluggable strategy for detecting partition columns from file paths.
 * Implementations parse directory structures (Hive-style, template-based, etc.)
 * to extract partition column names, types, and per-file values.
 */
public interface PartitionDetector {

    default PartitionMetadata detect(List<StorageEntry> files) {
        return detect(files, null);
    }

    /**
     * Detects partitions, routing any client-facing notice (e.g. a partition column renamed away from a reserved
     * name) to {@code warningSink}. Detection runs on the resolver's executor chain, where a direct {@code HeaderWarning}
     * write never reaches the client, so callers there must pass a buffered sink.
     */
    PartitionMetadata detect(List<StorageEntry> files, @Nullable Consumer<String> warningSink);

    String name();
}
