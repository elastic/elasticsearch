/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;

import java.util.Map;
import java.util.Objects;

/**
 * Cache key for schema inference results. Includes mtime-in-key for invalidation.
 * The identity component carries what each participant reports about itself, because the same canonical path on different
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
    String identity,
    @Nullable FileSetFingerprint fileSetFingerprint,
    String definitionVersion
) {
    /**
     * The version of the stored definitions this query reads under, as a named component rather than
     * a format setting: it is not an option a reader parses, so it has no place among the settings a
     * participant reports as its own identity.
     * <p>
     * Absent for a query that reaches the cache without a registered dataset behind it, where there is
     * no definition to version. Such entries share one version value and are addressed as they were
     * before.
     */
    public static String definitionVersionOf(Map<String, Object> config) {
        if (config == null) {
            return "";
        }
        Object v = config.get(DefinitionVersion.CONFIG_KEY);
        return v instanceof String s ? s : "";
    }

    /**
     * @param identity the folded identities of the participants that decide what a record about this file holds:
     *                 what the storage provider says identifies the object, what the format reader says identifies
     *                 its own configuration, and what the coordinator says identifies its own. This key used to
     *                 derive all three itself, from a list of setting names it did not own and two string literals.
     */
    public static SchemaCacheKey build(String canonicalPath, long mtime, String formatType, String identity, Map<String, Object> config) {
        return new SchemaCacheKey(canonicalPath, mtime, formatType != null ? formatType : "", identity, null, definitionVersionOf(config));
    }

    /**
     * Reserved {@code formatType} suffix namespace: the happy path is the registry format name
     * ({@code parquet}, {@code csv}), which never contains {@code '#'}. Resolve failure still
     * last-dot-falls-back, so a {@code '#'}-suffixed formatType is normally minted only by an
     * explicit factory. A fallback suffix that {@code endsWith} {@link #DATASET_AGGREGATE_MARKER}
     * would make {@link #isDatasetAggregate()} true on a per-file key, but a per-file key carries a
     * null {@code fileSetFingerprint} so it can never equal a dataset key - the only cost is that
     * one file losing its warm enrichment, a miss, never a wrong answer. Two members exist:
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
     * Under a lenient error policy ({@code skip_row}/{@code null_field}) a harvested row count IS
     * declaration-dependent, which is why the resolved read configuration now participates in the stats identity
     * ({@link ReadConfigFingerprint}): a harvest may only enrich, and an entry may only serve, a read of the
     * same read configuration. What still crosses read configurations is the physical record count under
     * {@code FAIL_FAST}, licensed by the producer because there the count is the same number for every
     * declaration.
     * <p>
     * <b>The dataset aggregate does NOT inherit that gate</b>, and an earlier revision of this javadoc claimed it
     * did. The aggregate entry stores a bare row count with no read-configuration stamp and no licence, so the
     * serve path's unstamped pass-through — which exists for the columnar readers, that harvest without stamping —
     * fires on it. Nothing compares the configuration that produced the aggregate against the one consuming it.
     * <p>
     * It is not a wrong answer today, and each reason is an accident rather than a guard. The strict multi-file
     * rail never reaches the aggregate at all. A non-strict overlay only retypes and renames in place, never
     * appends, so a projection-less {@code COUNT(*)} sees the same survivor set under every read configuration
     * this rail can reach. And a projection-decided drop suppresses its publish at the producer, so a
     * survivor-count-dependent aggregate is never built. Change any one of those and this becomes a silent wrong
     * count with no failing test. The fix, if it is ever worth doing, is to stamp the aggregate with the fold's
     * read configuration and licence and gate the serve, exactly as the per-file rail does.
     */
    public static SchemaCacheKey forDatasetAggregate(
        String pattern,
        FileSetFingerprint fingerprint,
        String sourceType,
        String identity,
        Map<String, Object> config
    ) {
        // A dataset key is identified two ways — the marker suffix on formatType and a non-null
        // fileSetFingerprint (isDatasetAggregate() vs the collision defense). Require the fingerprint here
        // so a marker-suffixed key with a null fingerprint is never representable and the two agree.
        Objects.requireNonNull(fingerprint, "dataset aggregate key requires a non-null file-set fingerprint");
        String formatType = (sourceType == null ? "" : sourceType) + DATASET_AGGREGATE_MARKER;
        return new SchemaCacheKey(pattern == null ? "" : pattern, 0L, formatType, identity, fingerprint, definitionVersionOf(config));
    }

    /**
     * True when this key addresses a dataset-level aggregate entry (minted by {@link #forDatasetAggregate})
     * rather than a per-file schema entry. Centralizes the {@link #DATASET_AGGREGATE_MARKER} check so the
     * taxonomy lives with the key instead of being re-derived at each call site.
     */
    public boolean isDatasetAggregate() {
        return formatType().endsWith(DATASET_AGGREGATE_MARKER);
    }

}
