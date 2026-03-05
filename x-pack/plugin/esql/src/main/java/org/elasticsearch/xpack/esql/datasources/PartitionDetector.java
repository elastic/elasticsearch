/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.List;
import java.util.Map;

/**
 * Pluggable strategy for detecting partition columns from file paths.
 * Implementations parse directory structures (Hive-style, template-based, etc.)
 * to extract partition column names, types, and per-file values.
 */
public interface PartitionDetector {

    PartitionMetadata detect(List<StorageEntry> files, Map<String, Object> config);

    String name();
}
