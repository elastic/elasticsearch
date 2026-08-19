/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.util.Check;

import java.util.Map;
import java.util.Set;

/**
 * Describes a file format this plugin provides: a logical format name, the
 * file extensions that select it, the per-dataset configuration keys it
 * recognises, and an optional value validator.
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
 * <p>{@code configValidator} is an optional per-format value validator invoked
 * at dataset registration time after key-membership checks pass. It receives
 * only the format-specific keys the user supplied and should throw
 * {@link IllegalArgumentException} for any invalid value;
 * {@link FileDataSourceValidator#validateDataset} catches these and accumulates
 * them into a {@link org.elasticsearch.common.ValidationException} so multiple
 * bad values report together. {@code null} means no value validation beyond key
 * membership — values are accepted as-is and validated at query time.
 *
 * <p>Example:
 * {@snippet lang="java" :
 * Set.of(
 *     new FormatSpec("csv", Set.of(".csv"), Set.of("delimiter", "quote"), CsvFormatReader::validateConfig),
 *     new FormatSpec("tsv", Set.of(".tsv"), Set.of("delimiter", "quote"), CsvFormatReader::validateConfig)
 * )
 * }
 *
 * @param format          logical format name (e.g. "csv", "tsv", "parquet")
 * @param extensions      file extensions with leading dot (e.g. ".csv", ".parquet")
 * @param configKeys      per-dataset configuration keys the format reader recognises
 * @param configValidator optional value validator called at PUT time; {@code null} means no-op
 */
public record FormatSpec(String format, Set<String> extensions, Set<String> configKeys, FormatConfigValidator configValidator) {

    /**
     * Validates the format-specific dataset settings supplied at registration time.
     * Implementors should throw {@link IllegalArgumentException} for invalid values;
     * {@link FileDataSourceValidator} catches these and accumulates them into a
     * {@link org.elasticsearch.common.ValidationException} so multiple bad values report together.
     *
     * <p>Only format-specific keys present in the dataset settings are forwarded;
     * base dataset fields (e.g. {@code error_mode}, {@code schema_sample_size}) are
     * validated separately and never forwarded here.
     */
    @FunctionalInterface
    public interface FormatConfigValidator {
        void validate(Map<String, Object> formatSettings);
    }

    public FormatSpec {
        Check.notNull(format, "format must not be null");
        Check.notNull(extensions, "extensions must not be null");
        configKeys = configKeys != null ? Set.copyOf(configKeys) : Set.of();
        // null configValidator means no-op; stored as null so callers can skip the call entirely.
    }

    /**
     * Convenience factory for formats with no per-dataset configuration keys or validator.
     */
    public static FormatSpec of(String format, String extension) {
        return new FormatSpec(format, Set.of(extension), Set.of(), null);
    }

    /**
     * Convenience factory for the common single-extension case with config keys but no validator.
     */
    public static FormatSpec of(String format, String extension, Set<String> configKeys) {
        return new FormatSpec(format, Set.of(extension), configKeys, null);
    }
}
