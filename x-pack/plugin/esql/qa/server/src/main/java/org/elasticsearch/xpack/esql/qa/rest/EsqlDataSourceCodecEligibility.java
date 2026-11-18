/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.Build;
import org.elasticsearch.Version;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Compression-codec eligibility for data-source csv-spec suites.
 *
 * <p>Eligibility is about what the old distribution in a mixed-version run supports, so it is
 * independent of how many parameter tuples a suite expands to.
 */
public final class EsqlDataSourceCodecEligibility {

    /**
     * The single place a Parquet internal codec's introduction version is recorded, so a new codec
     * does not grow a test-name check somewhere. Every codec here predates the earliest BWC version
     * the suites run against, which is why nothing is filtered out today.
     */
    private static final Map<String, Version> PARQUET_CODEC_INTRODUCED = Map.of(
        "snappy",
        Version.V_9_5_0,
        "gzip",
        Version.V_9_5_0,
        "zstd",
        Version.V_9_5_0,
        "lz4raw",
        Version.V_9_5_0
    );

    private EsqlDataSourceCodecEligibility() {}

    /**
     * Returns the normalized compression identity encoded by a text format, or {@code none} when
     * the format has no extension.
     */
    public static String textCodecIdentity(String format) {
        if (format == null || format.isBlank()) {
            throw new IllegalArgumentException("A text format is required");
        }
        int firstDot = format.indexOf('.');
        return firstDot < 0 ? "none" : normalizeCodecToken(format.substring(firstDot));
    }

    /**
     * Normalizes a compression suffix, full text format, or bare codec name to its matrix identity.
     *
     * <p>Extension aliases deliberately collapse to one identity. Unknown bare names are retained so
     * Parquet internal codecs such as {@code snappy} and {@code lz4raw} remain distinct.
     */
    public static String normalizeCodecToken(String codecOrSuffix) {
        if (codecOrSuffix == null || codecOrSuffix.isBlank()) {
            throw new IllegalArgumentException("A codec or suffix is required");
        }
        String normalized = codecOrSuffix.trim().toLowerCase(Locale.ROOT);
        int lastDot = normalized.lastIndexOf('.');
        if (lastDot >= 0) {
            normalized = normalized.substring(lastDot + 1);
        }
        return switch (normalized) {
            case "csv", "tsv", "ndjson", "parquet", "orc", "none" -> "none";
            case "gz", "gzip" -> "gzip";
            case "zst", "zstd" -> "zstd";
            case "bz", "bz2", "bzip2" -> "bzip2";
            default -> normalized;
        };
    }

    /**
     * Text compression formats eligible in the current or mixed build.
     *
     * <p>Gzip and zstd are GA. Bzip2 remains snapshot-only, so a mixed run requires
     * both distributions to be snapshots.
     */
    public static List<String> textCompressionFormats(String baseFormat) {
        boolean includeExperimental = EsqlDataSourceMixedClusterTestSupport.isBwcTest()
            ? EsqlDataSourceMixedClusterTestSupport.currentBuildSnapshot() && EsqlDataSourceMixedClusterTestSupport.oldBuildSnapshot()
            : Build.current().isSnapshot();
        if (includeExperimental) {
            return List.of(baseFormat + ".gz", baseFormat + ".zst", baseFormat + ".zstd", baseFormat + ".bz2", baseFormat + ".bz");
        }
        return List.of(baseFormat + ".gz", baseFormat + ".zst", baseFormat + ".zstd");
    }

    /**
     * Filters requested Parquet codecs by the single shared introduction-version registry.
     *
     * <p>Never returns an empty list: a codec column that filtered down to nothing would make the
     * parameter factory emit tuples without that column, and the suite would fail on constructor
     * arity instead of on the real cause.
     */
    public static List<String> parquetCodecs(String... requestedCodecs) {
        List<String> requested = List.of(requestedCodecs);
        if (EsqlDataSourceMixedClusterTestSupport.isBwcTest() == false) {
            return requested;
        }

        Version oldVersion = EsqlDataSourceMixedClusterTestSupport.bwcVersion();
        List<String> eligible = requested.stream().map(name -> name.toLowerCase(Locale.ROOT)).filter(name -> {
            Version introduced = PARQUET_CODEC_INTRODUCED.get(name);
            if (introduced == null) {
                throw new IllegalArgumentException("Unknown Parquet test codec [" + name + "]");
            }
            return oldVersion.onOrAfter(introduced);
        }).toList();
        if (eligible.isEmpty()) {
            throw new IllegalStateException("No requested Parquet codec of " + requested + " is supported on " + oldVersion);
        }
        return eligible;
    }
}
