/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.Set;

/**
 * Former Parquet dataset kill-switches. PUT and {@code EXTERNAL} WITH reject them as unknown
 * keys. {@link DatasetRewriter} strips them from stored dataset settings so an upgrade does
 * not fail {@code FROM} against a document that still contains them.
 * <p>
 * Names are unique across formats, so the rewriter drops them unconditionally rather than
 * growing {@link org.elasticsearch.xpack.esql.datasources.spi.FormatSpec} with a removed-keys
 * component. There is no cluster-state migration: GET still returns the stored keys; they
 * leave cluster state when the operator next PUTs without them.
 */
public final class RemovedParquetDatasetSettings {

    public static final String OPTIMIZED_READER = "optimized_reader";
    public static final String LATE_MATERIALIZATION = "late_materialization";

    public static final Set<String> KEYS = Set.of(OPTIMIZED_READER, LATE_MATERIALIZATION);

    private RemovedParquetDatasetSettings() {}
}
