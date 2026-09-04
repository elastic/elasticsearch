/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.util.Check;

import java.util.Set;

/**
 * Describes a file format this plugin provides: a logical format name, the
 * file extensions that select it, and the per-dataset configuration keys it
 * recognises.
 *
 * <p>The format name must match a key in the map returned by
 * {@link DataSourcePlugin#formatReaders}. Multiple extensions may map to the
 * same format (e.g. {@code .ndjson}, {@code .jsonl}, {@code .json} all select
 * {@code "ndjson"}), but each extension should map to exactly one format across
 * all plugins.
 *
 * <p>{@code configKeys} declares the configuration keys the format reader
 * accepts per dataset (e.g. {@code "delimiter"}, {@code "encoding"} for CSV).
 * These keys must stay in sync with the reader's {@code RECOGNIZED_KEYS};
 * each format plugin's test suite should verify the symmetry.
 *
 * <p>Example:
 * {@snippet lang="java" :
 * Set.of(
 *     new FormatSpec("csv",  Set.of(".csv"), Set.of("delimiter", "quote")),
 *     new FormatSpec("tsv",  Set.of(".tsv"), Set.of("delimiter", "quote"))
 * )
 * }
 *
 * @param format     logical format name (e.g. "csv", "tsv", "parquet")
 * @param extensions file extensions with leading dot (e.g. ".csv", ".parquet")
 * @param configKeys per-dataset configuration keys the format reader recognises
 */
public record FormatSpec(String format, Set<String> extensions, Set<String> configKeys) {

    public FormatSpec {
        Check.notNull(format, "format must not be null");
        Check.notNull(extensions, "extensions must not be null");
        configKeys = configKeys != null ? Set.copyOf(configKeys) : Set.of();
    }

    /**
     * Convenience factory for formats with no per-dataset configuration keys.
     */
    public static FormatSpec of(String format, String extension) {
        return new FormatSpec(format, Set.of(extension), Set.of());
    }

    /**
     * Convenience factory for the common single-extension case with config keys.
     */
    public static FormatSpec of(String format, String extension, Set<String> configKeys) {
        return new FormatSpec(format, Set.of(extension), configKeys);
    }
}
