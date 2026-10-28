/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Objects;

/**
 * Class holding all the flags that can be used to change behavior for certain features in ESQL.
 * The flags are backed by {@link Setting}s so they can be dynamically changed.
 * When adding a new flag, make sure to add it to {@link #ALL_ESQL_FLAGS_SETTINGS}
 * so it gets registered and unit tests can pass.
 */
public class EsqlFlags {
    public static final Setting<Boolean> ESQL_STRING_LIKE_ON_INDEX = Setting.boolSetting(
        "esql.query.string_like_on_index",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The maximum number of rounding points to push down to Lucene for the {@code roundTo} function at cluster level.
     * {@code ReplaceRoundToWithQueryAndTags} checks this threshold before rewriting {@code RoundTo} to range queries.
     *
     * There is also a query level ROUNDTO_PUSHDOWN_THRESHOLD defined in {@code QueryPragmas}.
     * The cluster level threshold defaults to 127, it is the same as the maximum number of buckets used in {@code Rounding}.
     * The query level threshold defaults to -1, which means this query level setting is not set and cluster level upper limit will be used.
     * If query level threshold is set to greater than or equals to 0, the query level threshold will be used, and it overrides the cluster
     * level threshold.
     *
     * If the cluster level threshold is set to -1 or 0, no {@code RoundTo} pushdown will be performed, query level threshold is not set to
     * -1 or 0.
     */
    public static final Setting<Integer> ESQL_ROUNDTO_PUSHDOWN_THRESHOLD = Setting.intSetting(
        "esql.query.roundto_pushdown_threshold",
        127,
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether the coordinator may plan the fetch phase: load the columns a query only needs after a {@code LIMIT} or
     * {@code TopN} for the rows that survive it, instead of on every data node for every candidate row. Nothing can
     * run such a plan yet, so only tests turn it on.
     */
    public enum FetchPhaseMode {
        DISABLED,
        ENABLED
    }

    /**
     * Cluster-wide cap on the number of leaf branches an independently executed query may use.
     * View compaction also uses this as the flatten budget for nested view unions.
     * An explicit {@link QueryPragmas#MAX_BRANCH_COUNT} pragma overrides this value for that query.
     */
    public static final Setting<Integer> ESQL_MAX_BRANCH_COUNT = Setting.intSetting(
        "esql.query.max_branch_count",
        QueryPragmas.MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cluster-wide cap on the number of commands a statement may have, counting the main query and all its subqueries together.
     * An explicit {@link QueryPragmas#MAX_COMMAND_COUNT} pragma overrides this value for that query.
     * <p>
     * The main pipeline is also bounded by {@code LogicalPlanBuilder#MAX_QUERY_DEPTH} (500), a fixed recursion guard in the parser.
     * Raising this setting above 500 only lets subqueries and expanded views use the extra budget; a main pipeline with more than
     * 500 commands is still rejected.
     */
    public static final Setting<Integer> ESQL_MAX_COMMAND_COUNT = Setting.intSetting(
        "esql.query.max_command_count",
        QueryPragmas.MAX_COMMAND_COUNT.getDefault(Settings.EMPTY),
        500,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cluster-wide cap on how deeply {@code UnionAll}s may nest in an independently executed query.
     * An explicit {@link QueryPragmas#MAX_BRANCH_LEVEL} pragma overrides this value for that query.
     */
    public static final Setting<Integer> ESQL_MAX_BRANCH_LEVEL = Setting.intSetting(
        "esql.query.max_branch_level",
        QueryPragmas.MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cluster-wide cap on how many direct children one merge may have. Applies to {@code FORK}, dataset rewrite, and a top-level PromQL
     * {@code or} chain. An explicit {@link QueryPragmas#MAX_BRANCH_COUNT_PER_MERGE} pragma overrides this value for that query.
     */
    public static final Setting<Integer> ESQL_MAX_BRANCH_COUNT_PER_MERGE = Setting.intSetting(
        "esql.query.max_branch_count_per_merge",
        QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
        1,
        QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE_MAX,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cap on the columns all {@code FILLNULL} commands of a query fill, counting each FORK branch and view reference that runs one; an IN
     * subquery runs as a query of its own with its own cap. Each filled column is an operator copying the page's block array, a cost no
     * breaker tracks that grows with columns times page width, and consecutive FILLNULLs chain into one pipeline, so they add up.
     */
    // Operator-only where operator privileges are enabled (Cloud, Serverless) while FILLNULL is snapshot-only; make it Dynamic once
    // FILLNULL ships.
    public static final Setting<Integer> ESQL_MAX_FILLNULL_FIELDS = Setting.intSetting(
        "esql.query.max_fillnull_fields",
        // the default of index.mapping.total_fields.limit; HeapAttackFillNullSweepIT measures the cost
        1000,
        1,
        100_000,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    // this is only used for testing purposes right now
    public static List<Setting<?>> ALL_ESQL_FLAGS_SETTINGS = List.of(
        ESQL_STRING_LIKE_ON_INDEX,
        ESQL_ROUNDTO_PUSHDOWN_THRESHOLD,
        ESQL_MAX_BRANCH_COUNT,
        ESQL_MAX_COMMAND_COUNT,
        ESQL_MAX_BRANCH_LEVEL,
        ESQL_MAX_BRANCH_COUNT_PER_MERGE,
        ESQL_MAX_FILLNULL_FIELDS
    );

    /**
     * All flags at their registered defaults. Tests and benchmarks use this; production constructs
     * flags from {@link ClusterSettings}.
     */
    public static final EsqlFlags DEFAULTS = new EsqlFlags(
        ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY),
        ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY),
        ESQL_MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
        ESQL_MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
        ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
        ESQL_MAX_FILLNULL_FIELDS.getDefault(Settings.EMPTY),
        ESQL_MAX_COMMAND_COUNT.getDefault(Settings.EMPTY),
        FetchPhaseMode.DISABLED
    );

    private final boolean stringLikeOnIndex;

    private final int roundToPushdownThreshold;

    private final int maxBranchCount;

    private final int maxBranchLevel;

    private final int maxBranchCountPerMerge;

    private final int maxFillNullFields;

    private final int maxCommandCount;

    private final FetchPhaseMode fetchPhaseMode;

    /**
     * Constructor for tests.
     */
    public EsqlFlags(boolean stringLikeOnIndex) {
        this(stringLikeOnIndex, ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY));
    }

    /**
     * Constructor for tests.
     */
    public EsqlFlags(int roundToPushdownThreshold) {
        this(ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY), roundToPushdownThreshold);
    }

    /**
     * Constructor for tests.
     */
    public EsqlFlags(boolean stringLikeOnIndex, int roundToPushdownThreshold) {
        this(
            stringLikeOnIndex,
            roundToPushdownThreshold,
            ESQL_MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
            ESQL_MAX_FILLNULL_FIELDS.getDefault(Settings.EMPTY)
        );
    }

    /**
     * Test helper that leaves the other flags at their defaults.
     */
    public static EsqlFlags withMaxBranchLimits(int maxBranchCount, int maxBranchLevel) {
        return withMaxBranchLimits(maxBranchCount, maxBranchLevel, ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY));
    }

    /**
     * Test helper that leaves the other flags at their defaults.
     */
    public static EsqlFlags withMaxBranchCountPerMerge(int maxBranchCountPerMerge) {
        return withMaxBranchLimits(
            ESQL_MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
            maxBranchCountPerMerge
        );
    }

    public static EsqlFlags withMaxBranchLimits(int maxBranchCount, int maxBranchLevel, int maxBranchCountPerMerge) {
        return new EsqlFlags(
            ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY),
            ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY),
            maxBranchCount,
            maxBranchLevel,
            maxBranchCountPerMerge,
            ESQL_MAX_FILLNULL_FIELDS.getDefault(Settings.EMPTY)
        );
    }

    /**
     * Test helper that leaves the other flags at their defaults.
     */
    public static EsqlFlags withFetchPhase(boolean enabled) {
        return new EsqlFlags(
            ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY),
            ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
            ESQL_MAX_FILLNULL_FIELDS.getDefault(Settings.EMPTY),
            ESQL_MAX_COMMAND_COUNT.getDefault(Settings.EMPTY),
            enabled ? FetchPhaseMode.ENABLED : FetchPhaseMode.DISABLED
        );
    }

    public EsqlFlags(
        boolean stringLikeOnIndex,
        int roundToPushdownThreshold,
        int maxBranchCount,
        int maxBranchLevel,
        int maxFillNullFields
    ) {
        this(
            stringLikeOnIndex,
            roundToPushdownThreshold,
            maxBranchCount,
            maxBranchLevel,
            ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
            maxFillNullFields
        );
    }

    /**
     * Test helper that leaves the other flags at their defaults.
     */
    public static EsqlFlags withMaxFillNullFields(int maxFillNullFields) {
        return new EsqlFlags(
            ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY),
            ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
            maxFillNullFields
        );
    }

    public EsqlFlags(
        boolean stringLikeOnIndex,
        int roundToPushdownThreshold,
        int maxBranchCount,
        int maxBranchLevel,
        int maxBranchCountPerMerge,
        int maxFillNullFields
    ) {
        this(
            stringLikeOnIndex,
            roundToPushdownThreshold,
            maxBranchCount,
            maxBranchLevel,
            maxBranchCountPerMerge,
            maxFillNullFields,
            ESQL_MAX_COMMAND_COUNT.getDefault(Settings.EMPTY)
        );
    }

    public EsqlFlags(
        boolean stringLikeOnIndex,
        int roundToPushdownThreshold,
        int maxBranchCount,
        int maxBranchLevel,
        int maxBranchCountPerMerge,
        int maxFillNullFields,
        int maxCommandCount
    ) {
        this(
            stringLikeOnIndex,
            roundToPushdownThreshold,
            maxBranchCount,
            maxBranchLevel,
            maxBranchCountPerMerge,
            maxFillNullFields,
            maxCommandCount,
            FetchPhaseMode.DISABLED
        );
    }

    public EsqlFlags(
        boolean stringLikeOnIndex,
        int roundToPushdownThreshold,
        int maxBranchCount,
        int maxBranchLevel,
        int maxBranchCountPerMerge,
        int maxFillNullFields,
        int maxCommandCount,
        FetchPhaseMode fetchPhaseMode
    ) {
        this.stringLikeOnIndex = stringLikeOnIndex;
        this.roundToPushdownThreshold = roundToPushdownThreshold;
        this.maxBranchCount = maxBranchCount;
        this.maxBranchLevel = maxBranchLevel;
        this.maxBranchCountPerMerge = maxBranchCountPerMerge;
        this.maxFillNullFields = maxFillNullFields;
        this.maxCommandCount = maxCommandCount;
        this.fetchPhaseMode = fetchPhaseMode;
    }

    public EsqlFlags(ClusterSettings settings) {
        this(
            settings.get(ESQL_STRING_LIKE_ON_INDEX),
            settings.get(ESQL_ROUNDTO_PUSHDOWN_THRESHOLD),
            settings.get(ESQL_MAX_BRANCH_COUNT),
            settings.get(ESQL_MAX_BRANCH_LEVEL),
            settings.get(ESQL_MAX_BRANCH_COUNT_PER_MERGE),
            settings.get(ESQL_MAX_FILLNULL_FIELDS),
            settings.get(ESQL_MAX_COMMAND_COUNT)
        );
    }

    /**
     * Controls whether {@code WHERE _index LIKE pattern} is consistent with the compute engine
     * when pushed down to Lucene or uses legacy behavior that was not consistent.
     * See https://github.com/elastic/elasticsearch/issues/129511.
     */
    public boolean stringLikeOnIndex() {
        return stringLikeOnIndex;
    }

    public int roundToPushdownThreshold() {
        return roundToPushdownThreshold;
    }

    public int maxBranchCount() {
        return maxBranchCount;
    }

    public int maxBranchLevel() {
        return maxBranchLevel;
    }

    public int maxBranchCountPerMerge() {
        return maxBranchCountPerMerge;
    }

    public int maxFillNullFields() {
        return maxFillNullFields;
    }

    public int maxCommandCount() {
        return maxCommandCount;
    }

    /**
     * Whether the coordinator may plan the fetch phase. See {@link FetchPhaseMode}.
     */
    public FetchPhaseMode fetchPhaseMode() {
        return fetchPhaseMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EsqlFlags that = (EsqlFlags) o;
        return stringLikeOnIndex == that.stringLikeOnIndex
            && roundToPushdownThreshold == that.roundToPushdownThreshold
            && maxBranchCount == that.maxBranchCount
            && maxBranchLevel == that.maxBranchLevel
            && maxBranchCountPerMerge == that.maxBranchCountPerMerge
            && maxFillNullFields == that.maxFillNullFields
            && maxCommandCount == that.maxCommandCount
            && fetchPhaseMode == that.fetchPhaseMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            stringLikeOnIndex,
            roundToPushdownThreshold,
            maxBranchCount,
            maxBranchLevel,
            maxBranchCountPerMerge,
            maxFillNullFields,
            maxCommandCount,
            fetchPhaseMode
        );
    }

    @Override
    public String toString() {
        return "EsqlFlags[stringLikeOnIndex="
            + stringLikeOnIndex
            + ", roundToPushdownThreshold="
            + roundToPushdownThreshold
            + ", maxBranchCount="
            + maxBranchCount
            + ", maxBranchLevel="
            + maxBranchLevel
            + ", maxBranchCountPerMerge="
            + maxBranchCountPerMerge
            + ", maxFillNullFields="
            + maxFillNullFields
            + ", maxCommandCount="
            + maxCommandCount
            + ", fetchPhaseMode="
            + fetchPhaseMode
            + ']';
    }
}
