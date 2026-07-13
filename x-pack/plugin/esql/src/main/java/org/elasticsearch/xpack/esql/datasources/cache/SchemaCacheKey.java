/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Cache key for schema inference results. Includes mtime-in-key for invalidation.
 * Endpoint and region are included because the same canonical path on different
 * endpoints resolves to different objects.
 * <p>
 * {@code fileSetFingerprint} carries the 128-bit fingerprint of the resolved file set for a
 * dataset-level aggregate key (see {@link #forDatasetAggregate}); it is {@code null} for every
 * per-file key. A named component rather than smuggling the fingerprint into the mtime/path slots —
 * record equality/hashCode pick it up automatically.
 */
public record SchemaCacheKey(
    String canonicalPath,
    long lastModifiedEpochMillis,
    String formatType,
    String formatConfig,
    String endpoint,
    String region,
    @Nullable FileSetFingerprint fileSetFingerprint
) {
    // Keep this set in sync with every option keyed off the WITH map by a FormatReader's
    // parseOptionsFromConfig / withConfig. The intent is broader than "changes the inferred
    // schema": any option that changes either the schema or whether schema inference fails on
    // the same input must appear here, or two queries with different formatting will collide on
    // the same cache entry.
    //
    // Notes on the less-obvious entries:
    // - max_field_size: a runtime parsing limit; doesn't change inferred types but can flip
    // schema inference between success and failure on the same bytes.
    // - schema_sample_size: bounds how many rows feed type inference; smaller samples can
    // widen/narrow the inferred type for borderline columns.
    // - column_prefix: only changes column NAMES (when header_row=false), but names are part
    // of the schema.
    // - error_mode / max_errors / max_error_ratio: change which rows survive and which cells are
    // null-filled, so captured row and column null counts must not be shared across policies.
    // - schema_resolution: changes multi-file schema merge (FFW vs UNION_BY_NAME) and therefore
    // which per-file stats are aggregated for aggregate pushdown.
    // - mode: quoted/escaped/plain changes record boundaries (row counts), null-ness (\N) and
    // values on the same bytes, so neither schemas nor captured stats may cross modes.
    // - multi_value_syntax: brackets selects the bracket-aware record scanner (newlines inside
    // [..] are not record ends) and, on a no-quote baseline, bare brackets resolves the mode
    // to quoted — so two configs differing only in this key can interpret the same bytes with
    // different record boundaries and must not share schemas or stats.
    private static final Set<String> FORMAT_AFFECTING_PARAMS = Set.of(
        "delimiter",
        "quote",
        "escape",
        "mode",
        "multi_value_syntax",
        "encoding",
        "datetime_format",
        "hive_partitioning",
        "partition_detection",
        "partition_path",
        "format",
        "null_value",
        "header",
        "header_row",
        "column_prefix",
        "comment",
        "max_field_size",
        "schema_sample_size",
        "skip_rows",
        // trim_spaces changes stored string values and the null-ness of whitespace-only cells on the
        // same bytes, so neither captured stats nor schemas may cross it.
        "trim_spaces",
        "error_mode",
        "max_errors",
        "max_error_ratio",
        "schema_resolution"
    );

    private static final Set<String> CREDENTIAL_PARAMS = Set.of(
        "access_key",
        "secret_key",
        "connection_string",
        "key",
        "sas_token",
        "credentials",
        "token"
    );

    public static SchemaCacheKey build(String canonicalPath, long mtime, String formatType, Map<String, Object> config) {
        String endpoint = config != null ? String.valueOf(config.getOrDefault("endpoint", "")) : "";
        String region = config != null ? String.valueOf(config.getOrDefault("region", "")) : "";
        String formatConfig = buildFormatConfig(config);
        return new SchemaCacheKey(canonicalPath, mtime, formatType != null ? formatType : "", formatConfig, endpoint, region, null);
    }

    /**
     * Reserved {@code formatType} suffix namespace: extension detection ({@code detectFormatType})
     * derives {@code formatType} from a file name's last dot, so for any sane object name a
     * {@code '#'}-suffixed formatType is minted only by an explicit factory. (A pathological object name
     * literally containing {@code '#dataset-agg'} would collide on the suffix, but a per-file key carries a
     * null {@code fileSetFingerprint} so it can never equal a dataset key - the only cost is that one file
     * losing its warm enrichment, a miss, never a wrong answer.) Two members exist:
     * {@link #STRICT_DECLARED_SCHEMA_MARKER} (per-file entries on the strict-declared warm rail, which
     * the reconcile's contribution matching MUST still reach) and {@link #DATASET_AGGREGATE_MARKER}
     * (dataset-level aggregate entries, which contribution matching must NEVER reach - enforced in
     * {@code ExternalSourceCacheService#matchesContribution}). Co-located here so their distinctness is
     * visible at the declaration site.
     */
    public static final String STRICT_DECLARED_SCHEMA_MARKER = "#strict-declared";
    public static final String DATASET_AGGREGATE_MARKER = "#dataset-agg";

    /**
     * Key for a dataset-level aggregate entry: the memoized multi-file stats fold for one resolved file
     * SET under one format config. Identity is the listing's 128-bit file-set fingerprint (a commutative
     * fold of every file's path + mtime + size, plus the file count - see
     * {@code FileList#fileSetFingerprint}), which makes the key correct-or-miss by construction: any file
     * added, removed, or modified derives a different key, and the stale entry simply ages out via
     * LRU/TTL - no invalidation protocol. The fingerprint rides the dedicated {@code fileSetFingerprint}
     * record component; {@code canonicalPath} is the glob pattern (diagnostics-friendly) and the
     * marker-suffixed {@code formatType} keeps these entries out of the per-file contribution-matching
     * paths.
     * <p>
     * Known residual, inherited from the per-file rail: under a lenient error policy
     * ({@code skip_row}/{@code null_field}) a harvested row count can be declaration-dependent (see the
     * {@code warmsRowCountSafely} discussion on the strict single-file rail). The dataset aggregate
     * memoizes exactly what the per-file rail serves, so it neither narrows nor widens that residual -
     * both must be closed together by the declared-schema fingerprint follow-up.
     */
    public static SchemaCacheKey forDatasetAggregate(
        String pattern,
        FileSetFingerprint fingerprint,
        String sourceType,
        Map<String, Object> config
    ) {
        // A dataset key is identified two ways — the marker suffix on formatType and a non-null
        // fileSetFingerprint (isDatasetAggregate() vs the collision defense). Require the fingerprint here
        // so a marker-suffixed key with a null fingerprint is never representable and the two agree.
        Objects.requireNonNull(fingerprint, "dataset aggregate key requires a non-null file-set fingerprint");
        String endpoint = config != null ? String.valueOf(config.getOrDefault("endpoint", "")) : "";
        String region = config != null ? String.valueOf(config.getOrDefault("region", "")) : "";
        String formatType = (sourceType == null ? "" : sourceType) + DATASET_AGGREGATE_MARKER;
        return new SchemaCacheKey(pattern == null ? "" : pattern, 0L, formatType, buildFormatConfig(config), endpoint, region, fingerprint);
    }

    /**
     * True when this key addresses a dataset-level aggregate entry (minted by {@link #forDatasetAggregate})
     * rather than a per-file schema entry. Centralizes the {@link #DATASET_AGGREGATE_MARKER} check so the
     * taxonomy lives with the key instead of being re-derived at each call site.
     */
    public boolean isDatasetAggregate() {
        return formatType().endsWith(DATASET_AGGREGATE_MARKER);
    }

    /**
     * Canonical, node-stable identity of the row-interpretation-affecting config: the format-affecting
     * params (credentials and non-format keys excluded), sorted and rendered {@code key=value,...}.
     * Deterministic across JVMs and independent of column projection, so a coordinator and a data node
     * derive the same string for the same logical query config — the basis for the cross-node stats
     * cache fingerprint.
     */
    public static String buildFormatConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return "";
        }
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String key = entry.getKey();
            if (FORMAT_AFFECTING_PARAMS.contains(key) && CREDENTIAL_PARAMS.contains(key) == false) {
                sorted.put(key, String.valueOf(entry.getValue()));
            }
        }
        if (sorted.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return sb.toString();
    }
}
