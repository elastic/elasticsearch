/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;
import org.elasticsearch.xpack.esql.datasources.spi.WidenedColumn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Folds one file's harvest into a single relation-level {@code _stats} map and then drops that file's
 * column statistics. Production reconciliation and FIRST_FILE_WINS gathers use this fold.
 * {@link ExternalSourceResolver#batchAggregateFileStatistics} is the listing-order oracle the differential
 * tests compare against. A widened running type is rescaled with
 * {@link SourceStatisticsSerializer#normalizeStatsToReconciled} before the next file joins, and the
 * cross-file arithmetic is {@link SplitStats#fold}.
 * <p>
 * Not thread-safe. The gather calls {@link #accept} under one lock.
 */
final class RunningFileStatsFold {

    private static final Logger logger = LogManager.getLogger(RunningFileStatsFold.class);

    enum Mode {
        RECONCILIATION,
        FIRST_FILE_WINS
    }

    private final Mode mode;
    private final boolean implicitNullsForAbsentColumn;
    private final Map<String, DataType> anchorTypes;
    private final Set<String> declaredTypeColumns;

    private Map<String, DataType> runningTypes = Map.of();
    private SplitStats accumulator;
    /** The one adjusted harvest, kept only until a second file arrives so a single-file fold returns it unchanged. */
    private Map<String, Object> single;
    private boolean failed;
    private int accepted;

    private String agreedFingerprint;
    private boolean fingerprintSeen;
    private boolean mixedFingerprint;
    private boolean allLicensed = true;

    private final Set<String> invalidCountColumns = new HashSet<>();
    private final Set<String> unsignedForeignDomainColumns = new HashSet<>();

    private RunningFileStatsFold(
        Mode mode,
        boolean implicitNullsForAbsentColumn,
        @Nullable Map<String, DataType> anchorTypes,
        Set<String> declaredTypeColumns
    ) {
        this.mode = mode;
        this.implicitNullsForAbsentColumn = implicitNullsForAbsentColumn;
        this.anchorTypes = anchorTypes == null ? Map.of() : anchorTypes;
        this.declaredTypeColumns = declaredTypeColumns;
    }

    static RunningFileStatsFold reconciliation(boolean implicitNullsForAbsentColumn) {
        return new RunningFileStatsFold(Mode.RECONCILIATION, implicitNullsForAbsentColumn, null, Set.of());
    }

    static RunningFileStatsFold firstFileWins(
        Map<String, DataType> anchorTypes,
        boolean implicitNullsForAbsentColumn,
        Set<String> declaredTypeColumns
    ) {
        return new RunningFileStatsFold(
            Mode.FIRST_FILE_WINS,
            implicitNullsForAbsentColumn,
            anchorTypes,
            declaredTypeColumns == null ? Set.of() : declaredTypeColumns
        );
    }

    /**
     * Folds listing position {@code index}. A repeated path is a second call with the same metadata, matching
     * a scan that reads that file twice. A file with no row count fails the whole fold.
     */
    void accept(int index, SourceMetadata meta) {
        accepted++;
        if (failed) {
            return;
        }
        Map<String, Object> flat = ExternalSourceResolver.flatStatsOf(meta);
        if (flat == null) {
            fail(meta.location(), "has no statistics");
            return;
        }
        noteIdentity(flat);
        flat = adjust(index, meta, flat);
        if (failed) {
            return;
        }
        SplitStats stats = SplitStats.of(flat);
        if (stats == null) {
            fail(meta.location(), "has no row count");
            return;
        }
        if (accumulator == null) {
            accumulator = stats;
            single = flat;
            return;
        }
        accumulator = SplitStats.fold(List.of(accumulator, stats), implicitNullsForAbsentColumn);
        single = null;
        if (accumulator == null) {
            fail(meta.location(), "fold returned no aggregate");
        }
    }

    /**
     * The relation-level fold, or null when any accepted file lacked a row count. Pinned columns are not
     * applied here; they are known only once every schema has been reconciled. See {@link #applyPinnedColumns}.
     */
    @Nullable
    Map<String, Object> finish() {
        if (failed || accepted == 0 || accumulator == null) {
            return null;
        }
        if (single != null) {
            return finishSingle(single);
        }
        Map<String, Object> merged = new HashMap<>(accumulator.toMap());
        SourceStatisticsSerializer.attachFoldedReadConfigIdentity(mixedFingerprint, agreedFingerprint, allLicensed, merged);
        return finishMerged(merged);
    }

    /**
     * Reconciliation pins, applied to the finished accumulator. A {@code SKIP_ROW} pin drops that file's row
     * count and therefore the whole aggregate. Otherwise pinned columns that are still present are poisoned
     * on the accumulator. A pin whose column the fold already dropped (text, implicit nulls off, column
     * absent from a non-empty file) is not put back: overlay-then-merge drops it too.
     */
    @Nullable
    static Map<String, Object> applyPinnedColumns(
        @Nullable Map<String, Object> aggregated,
        @Nullable Map<?, Set<String>> perFilePinnedColumns,
        boolean dropPinnedRowCount
    ) {
        if (aggregated == null || perFilePinnedColumns == null || perFilePinnedColumns.isEmpty()) {
            return aggregated;
        }
        boolean anyPin = false;
        Set<String> union = new HashSet<>();
        for (Set<String> pinned : perFilePinnedColumns.values()) {
            if (pinned != null && pinned.isEmpty() == false) {
                anyPin = true;
                for (String column : pinned) {
                    if (columnPresent(aggregated, column)) {
                        union.add(column);
                    }
                }
            }
        }
        if (anyPin == false) {
            return aggregated;
        }
        if (dropPinnedRowCount) {
            return null;
        }
        if (union.isEmpty()) {
            return aggregated;
        }
        return SourceStatisticsSerializer.overlayPinnedColumnsOnStats(aggregated, union, false);
    }

    /** Exact stat-key match. A prefix check would treat a pin on {@code a} as present because of {@code a.b}. */
    private static boolean columnPresent(Map<String, Object> stats, String column) {
        return stats.containsKey(SourceStatisticsSerializer.columnMinKey(column))
            || stats.containsKey(SourceStatisticsSerializer.columnMaxKey(column))
            || stats.containsKey(SourceStatisticsSerializer.columnValueCountKey(column))
            || stats.containsKey(SourceStatisticsSerializer.columnNullCountKey(column))
            || stats.containsKey(SourceStatisticsSerializer.columnSizeBytesKey(column))
            || stats.containsKey(SourceStatisticsSerializer.columnMinUnservableKey(column))
            || stats.containsKey(SourceStatisticsSerializer.columnMaxUnservableKey(column));
    }

    /**
     * Per-file record kept after the harvest has been folded: schema, read-config fingerprint, and file-level
     * counts. Column min/max/null/value/size keys are omitted.
     */
    static SourceMetadata slim(SourceMetadata meta) {
        Map<String, Object> slim = new HashMap<>();
        Map<String, Object> base = meta.sourceMetadata();
        if (base != null) {
            for (Map.Entry<String, Object> entry : base.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(SourceStatisticsSerializer.STATS_COL_PREFIX) || ExternalStats.isStripeBookkeeping(key)) {
                    continue;
                }
                slim.put(key, entry.getValue());
            }
        }
        Map<String, Object> flat = ExternalSourceResolver.flatStatsOf(meta);
        if (flat != null) {
            copyFileLevel(flat, slim, SourceStatisticsSerializer.STATS_ROW_COUNT);
            copyFileLevel(flat, slim, SourceStatisticsSerializer.STATS_SIZE_BYTES);
            copyFileLevel(flat, slim, SourceStatisticsSerializer.STATS_READABLE_UNIT_COUNT);
            copyFileLevel(flat, slim, ExternalStats.READ_CONFIG_FINGERPRINT_KEY);
            copyFileLevel(flat, slim, ExternalStats.ROW_COUNT_READ_CONFIG_INDEPENDENT_KEY);
        }
        SimpleSourceMetadata slimMeta = new SimpleSourceMetadata(
            meta.schema(),
            meta.sourceType(),
            meta.location(),
            null,
            meta.partitionColumns().orElse(null),
            slim,
            meta.config()
        );
        List<String> warnings = meta.warnings();
        if (warnings.isEmpty() == false) {
            slimMeta = slimMeta.withWarnings(warnings);
        }
        List<WidenedColumn> widenedColumns = meta.widenedColumns();
        return widenedColumns.isEmpty() ? slimMeta : slimMeta.withWidenedColumns(widenedColumns);
    }

    private Map<String, Object> adjust(int index, SourceMetadata meta, Map<String, Object> flat) {
        if (mode == Mode.FIRST_FILE_WINS) {
            if (index == 0) {
                return flat;
            }
            return ExternalSourceResolver.rewriteNonAnchorHarvest(
                flat,
                ExternalSourceResolver.attributesToTypeMap(meta.schema()),
                anchorTypes,
                implicitNullsForAbsentColumn,
                declaredTypeColumns,
                invalidCountColumns,
                unsignedForeignDomainColumns
            );
        }
        Map<String, DataType> fileTypes = ExternalSourceResolver.attributesToTypeMap(meta.schema());
        Map<String, DataType> widened = widen(runningTypes, fileTypes);
        if (accumulator != null && existingTypeWidened(runningTypes, widened)) {
            Map<String, Object> rescaled = SourceStatisticsSerializer.normalizeStatsToReconciled(
                accumulator.toMap(),
                runningTypes,
                widened
            );
            accumulator = SplitStats.of(rescaled);
            if (accumulator == null) {
                fail(meta.location(), "rescale dropped the row count");
                return flat;
            }
        }
        runningTypes = widened;
        return SourceStatisticsSerializer.normalizeStatsToReconciled(flat, fileTypes, runningTypes);
    }

    private Map<String, Object> finishSingle(Map<String, Object> one) {
        if (invalidCountColumns.isEmpty() && unsignedForeignDomainColumns.isEmpty()) {
            return one;
        }
        Map<String, Object> copy = new HashMap<>(one);
        return finishMerged(copy);
    }

    private Map<String, Object> finishMerged(Map<String, Object> merged) {
        if (invalidCountColumns.isEmpty() == false || unsignedForeignDomainColumns.isEmpty() == false) {
            ExternalSourceResolver.dropColumnCounts(merged, invalidCountColumns, true);
            ExternalSourceResolver.dropColumnCounts(merged, unsignedForeignDomainColumns, false);
        }
        return merged;
    }

    private void noteIdentity(Map<String, Object> flat) {
        String fingerprint = SourceStatisticsSerializer.readConfigFingerprint(flat);
        if (fingerprintSeen == false) {
            agreedFingerprint = fingerprint;
            fingerprintSeen = true;
        } else if (Objects.equals(agreedFingerprint, fingerprint) == false) {
            mixedFingerprint = true;
        }
        allLicensed &= Boolean.TRUE.equals(flat.get(ExternalStats.ROW_COUNT_READ_CONFIG_INDEPENDENT_KEY));
    }

    private void fail(String location, String reason) {
        failed = true;
        accumulator = null;
        single = null;
        logger.debug("multi-file stats aggregate incomplete: [{}] {}", location, reason);
    }

    private static void copyFileLevel(Map<String, Object> from, Map<String, Object> to, String key) {
        if (from.containsKey(key)) {
            to.put(key, from.get(key));
        }
    }

    /** Running reconciled type. New columns are recorded; an existing column moves only via {@link TypeWidening#join}. */
    private static Map<String, DataType> widen(Map<String, DataType> running, Map<String, DataType> fileTypes) {
        if (fileTypes.isEmpty()) {
            return running;
        }
        Map<String, DataType> next = null;
        for (Map.Entry<String, DataType> entry : fileTypes.entrySet()) {
            DataType prior = running.get(entry.getKey());
            DataType joined = prior == null ? entry.getValue() : TypeWidening.join(prior, entry.getValue());
            if (prior != joined) {
                if (next == null) {
                    next = new HashMap<>(running);
                }
                next.put(entry.getKey(), joined);
            }
        }
        return next == null ? running : next;
    }

    private static boolean existingTypeWidened(Map<String, DataType> before, Map<String, DataType> after) {
        for (Map.Entry<String, DataType> entry : before.entrySet()) {
            if (entry.getValue() != after.get(entry.getKey())) {
                return true;
            }
        }
        return false;
    }
}
