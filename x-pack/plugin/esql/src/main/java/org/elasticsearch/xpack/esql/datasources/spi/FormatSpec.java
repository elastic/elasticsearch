/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Set;

/**
 * Describes a file format this plugin provides: a logical format name and the
 * file extensions that select it.
 *
 * <p>The format name must match a key in the map returned by
 * {@link DataSourcePlugin#formatReaders}. Multiple extensions may map to the
 * same format (e.g. {@code .ndjson}, {@code .jsonl}, {@code .json} all select
 * {@code "ndjson"}), but each extension should map to exactly one format across
 * all plugins.
 *
 * <p>Example:
 * <pre>{@code
 * Set.of(
 *     new FormatSpec("csv",  Set.of(".csv")),
 *     new FormatSpec("tsv",  Set.of(".tsv"))
 * )
 * }</pre>
 *
 * @param format     logical format name (e.g. "csv", "tsv", "parquet")
 * @param extensions file extensions with leading dot (e.g. ".csv", ".parquet")
 */
public record FormatSpec(String format, Set<String> extensions) {

    /**
     * Convenience factory for the common single-extension case.
     */
    public static FormatSpec of(String format, String extension) {
        return new FormatSpec(format, Set.of(extension));
    }
}
