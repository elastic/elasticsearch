/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

/**
 * Defines the scope of an analyzer, indicating its visibility and lifecycle within Elasticsearch.
 * Analyzers can be scoped to a single index, multiple indices, or globally across the cluster.
 */
public enum AnalyzerScope {
    /**
     * Analyzer is scoped to a single specific index.
     * The analyzer lifecycle is tied to the index and is not shared.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Index-scoped analyzers are defined in index settings
     * Settings settings = Settings.builder()
     *     .put("index.analysis.analyzer.my_analyzer.type", "standard")
     *     .build();
     * }</pre>
     */
    INDEX,

    /**
     * Analyzer is scoped to multiple indices.
     * The analyzer can be shared across different indices.
     */
    INDICES,

    /**
     * Analyzer is scoped globally across the entire Elasticsearch cluster.
     * These are typically built-in analyzers like "standard" or "keyword".
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Global analyzers are available to all indices without configuration
     * // Examples: "standard", "simple", "whitespace", "keyword"
     * }</pre>
     */
    GLOBAL
}
