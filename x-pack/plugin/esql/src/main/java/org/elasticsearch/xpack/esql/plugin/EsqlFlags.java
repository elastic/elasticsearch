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
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.TimeValue;

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
     * Builds that carry the fetch phase. It is on in snapshot builds and off in release builds unless the
     * {@code es.esql_fetch_phase_feature_flag_enabled} system property turns it on. It decides nothing else than whether
     * the {@code fetch_phase} pragma can take effect.
     */
    public static final FeatureFlag FETCH_PHASE_FEATURE_FLAG = new FeatureFlag("esql_fetch_phase");

    /**
     * Whether the coordinator may plan the fetch phase: load the columns a query only needs after a {@code TopN} for the
     * rows that survive it, instead of on every data node for every candidate row. The build decides the mode of the
     * cluster. The {@code fetch_phase} pragma of a query overrides it, except {@link #UNAVAILABLE}.
     */
    public enum FetchPhaseMode {
        /** The build does not carry the fetch phase. Nothing turns it on, not even the {@code fetch_phase} pragma. */
        UNAVAILABLE,
        /** Every plan loads its columns eagerly. */
        DISABLED,
        /** The fetch phase for the plans that can use it and are likely faster with it. */
        AUTO,
        /** The fetch phase for every plan that can use it, whatever it costs. Tests and benchmarks use it to cover every shape. */
        ALWAYS
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

    /**
     * Cap on how far back {@code METRICS_INFO} and {@code TS_INFO} scan data. When specified, the time range of
     * these commands is narrowed to the most recent part of the range of the request filter: {@code [max(t1, t2 - cap), t2]} for a
     * request filter with the range {@code [t1, t2]}, and {@code [now - cap, now]} when the request filter has no complete range.
     * A warning is returned if the time range is narrowed. Defaults to uncapped.
     */
    public static final Setting<TimeValue> ESQL_METRICS_INFO_MAX_TIME_RANGE = Setting.timeSetting(
        "esql.query.metrics_info_max_time_range",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * When enabled, time-series aggregates are reduced separately in each index before results are combined. This uses less memory and can
     * improve query performance, but results may be approximate for time buckets that overlap index rollover boundaries and for windowed
     * aggregates whose windows reach across those boundaries.
     */
    public static final Setting<Boolean> ESQL_TIME_SERIES_REDUCE_PER_INDEX = Setting.boolSetting(
        "esql.query.time_series.reduce_per_index",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cluster-wide cap on the binary operators in one expression, i.e. between two consecutive separators of a query. A comma separates
     * at any nesting level, a pipe or the semicolon that ends a {@code SET} or {@code LET} statement only outside parentheses, so a
     * nested pipe, e.g. in a subquery, cannot be used to reset the count of the enclosing expression. A flat chain such as
     * {@code 0+0+0+...} or a balanced tree such as {@code ((a+a)+(a+a))+...} stays under the parenthesis depth limit but makes ANTLR
     * allocate a very large parse tree, so such queries are rejected from the token stream before parsing starts. The check is
     * syntax-blind: unary {@code +}/{@code -} are conservatively counted too, and so is the {@code *} in {@code COUNT(*)}. Commas
     * separate independent expressions (fields of an {@code EVAL}, arguments of a function), so wide but shallow queries such as an
     * {@code EVAL} with thousands of fields are not affected. Raise it only if legitimate queries are rejected.
     */
    public static final Setting<Integer> ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION = Setting.intSetting(
        "esql.query.max_binary_operators_per_expression",
        1000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cluster-wide cap on the parentheses of one top level item of a query, which are the ones of function calls, subqueries and
     * parenthesized expressions. Unlike the binary operators, they are not reset inside parentheses, only by a comma, a pipe or a
     * statement semicolon outside of any parentheses, e.g. between the fields of an {@code EVAL}. A balanced tree of nested function
     * calls such as {@code max(max(0,0),max(0,0))} contains no binary operators, so the binary operator limit does not see it, but its
     * prediction cost in ANTLR grows with the number of the nested calls, and a few thousand characters can take gigabytes of heap.
     * The check runs on the token stream before the query is parsed. The setting counts the opening and the closing parentheses, and
     * the parser compares the opening ones, which are half of them in a valid query, with half of the value (rounded down), so the
     * default of 1000 allows 500 function calls and the like. Raise it only if legitimate queries are rejected.
     */
    public static final Setting<Integer> ESQL_MAX_PARENTHESES_COUNT = Setting.intSetting(
        "esql.query.max_parentheses_count",
        1000,
        // at least one opening parenthesis, so that a function call is possible
        2,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // this is only used for testing purposes right now
    public static List<Setting<?>> ALL_ESQL_FLAGS_SETTINGS = List.of(
        ESQL_STRING_LIKE_ON_INDEX,
        ESQL_ROUNDTO_PUSHDOWN_THRESHOLD,
        ESQL_MAX_BRANCH_COUNT,
        ESQL_MAX_COMMAND_COUNT,
        ESQL_MAX_BRANCH_LEVEL,
        ESQL_MAX_BRANCH_COUNT_PER_MERGE,
        ESQL_MAX_FILLNULL_FIELDS,
        ESQL_METRICS_INFO_MAX_TIME_RANGE,
        ESQL_TIME_SERIES_REDUCE_PER_INDEX,
        ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION,
        ESQL_MAX_PARENTHESES_COUNT
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
        ESQL_METRICS_INFO_MAX_TIME_RANGE.getDefault(Settings.EMPTY),
        ESQL_TIME_SERIES_REDUCE_PER_INDEX.getDefault(Settings.EMPTY),
        fetchPhaseMode(FETCH_PHASE_FEATURE_FLAG.isEnabled()),
        ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION.getDefault(Settings.EMPTY),
        ESQL_MAX_PARENTHESES_COUNT.getDefault(Settings.EMPTY)
    );

    private final boolean stringLikeOnIndex;

    private final int roundToPushdownThreshold;

    private final int maxBranchCount;

    private final int maxBranchLevel;

    private final int maxBranchCountPerMerge;

    private final int maxFillNullFields;

    private final int maxCommandCount;

    private final TimeValue metricsInfoMaxTimeRange;

    private final boolean timeSeriesReducePerIndex;

    private final FetchPhaseMode fetchPhaseMode;

    private final int maxBinaryOperatorsPerExpression;

    private final int maxParenthesesCount;

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
            ESQL_MAX_COMMAND_COUNT.getDefault(Settings.EMPTY),
            ESQL_METRICS_INFO_MAX_TIME_RANGE.getDefault(Settings.EMPTY)
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
        TimeValue metricsInfoMaxTimeRange
    ) {
        this(
            stringLikeOnIndex,
            roundToPushdownThreshold,
            maxBranchCount,
            maxBranchLevel,
            maxBranchCountPerMerge,
            maxFillNullFields,
            maxCommandCount,
            metricsInfoMaxTimeRange,
            ESQL_TIME_SERIES_REDUCE_PER_INDEX.getDefault(Settings.EMPTY),
            DEFAULTS.fetchPhaseMode
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
        TimeValue metricsInfoMaxTimeRange,
        boolean timeSeriesReducePerIndex,
        FetchPhaseMode fetchPhaseMode
    ) {
        this(
            stringLikeOnIndex,
            roundToPushdownThreshold,
            maxBranchCount,
            maxBranchLevel,
            maxBranchCountPerMerge,
            maxFillNullFields,
            maxCommandCount,
            metricsInfoMaxTimeRange,
            timeSeriesReducePerIndex,
            fetchPhaseMode,
            ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION.getDefault(Settings.EMPTY),
            ESQL_MAX_PARENTHESES_COUNT.getDefault(Settings.EMPTY)
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
        TimeValue metricsInfoMaxTimeRange,
        boolean timeSeriesReducePerIndex,
        FetchPhaseMode fetchPhaseMode,
        int maxBinaryOperatorsPerExpression,
        int maxParenthesesCount
    ) {
        this.stringLikeOnIndex = stringLikeOnIndex;
        this.roundToPushdownThreshold = roundToPushdownThreshold;
        this.maxBranchCount = maxBranchCount;
        this.maxBranchLevel = maxBranchLevel;
        this.maxBranchCountPerMerge = maxBranchCountPerMerge;
        this.maxFillNullFields = maxFillNullFields;
        this.maxCommandCount = maxCommandCount;
        this.metricsInfoMaxTimeRange = metricsInfoMaxTimeRange;
        this.timeSeriesReducePerIndex = timeSeriesReducePerIndex;
        this.fetchPhaseMode = fetchPhaseMode;
        this.maxBinaryOperatorsPerExpression = maxBinaryOperatorsPerExpression;
        this.maxParenthesesCount = maxParenthesesCount;
    }

    public EsqlFlags(ClusterSettings settings) {
        this(
            settings.get(ESQL_STRING_LIKE_ON_INDEX),
            settings.get(ESQL_ROUNDTO_PUSHDOWN_THRESHOLD),
            settings.get(ESQL_MAX_BRANCH_COUNT),
            settings.get(ESQL_MAX_BRANCH_LEVEL),
            settings.get(ESQL_MAX_BRANCH_COUNT_PER_MERGE),
            settings.get(ESQL_MAX_FILLNULL_FIELDS),
            settings.get(ESQL_MAX_COMMAND_COUNT),
            settings.get(ESQL_METRICS_INFO_MAX_TIME_RANGE),
            settings.get(ESQL_TIME_SERIES_REDUCE_PER_INDEX),
            fetchPhaseMode(FETCH_PHASE_FEATURE_FLAG.isEnabled()),
            settings.get(ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION),
            settings.get(ESQL_MAX_PARENTHESES_COUNT)
        );
    }

    /**
     * Test helper that leaves the other flags at their defaults.
     */
    public static EsqlFlags withParserLimits(int maxBinaryOperatorsPerExpression, int maxParenthesesCount) {
        return new EsqlFlags(
            ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY),
            ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
            ESQL_MAX_FILLNULL_FIELDS.getDefault(Settings.EMPTY),
            ESQL_MAX_COMMAND_COUNT.getDefault(Settings.EMPTY),
            ESQL_METRICS_INFO_MAX_TIME_RANGE.getDefault(Settings.EMPTY),
            ESQL_TIME_SERIES_REDUCE_PER_INDEX.getDefault(Settings.EMPTY),
            DEFAULTS.fetchPhaseMode,
            maxBinaryOperatorsPerExpression,
            maxParenthesesCount
        );
    }

    /**
     * Test helper that leaves the other flags at their defaults.
     */
    public static EsqlFlags withTimeSeriesReducePerIndex(boolean enabled) {
        return new EsqlFlags(
            ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY),
            ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
            ESQL_MAX_FILLNULL_FIELDS.getDefault(Settings.EMPTY),
            ESQL_MAX_COMMAND_COUNT.getDefault(Settings.EMPTY),
            ESQL_METRICS_INFO_MAX_TIME_RANGE.getDefault(Settings.EMPTY),
            enabled,
            DEFAULTS.fetchPhaseMode
        );
    }

    /**
     * Test helper that leaves the other flags at their defaults. It sets the mode whatever the feature flag of the test
     * JVM says, so planner tests behave the same on release builds.
     */
    public static EsqlFlags withFetchPhase(FetchPhaseMode fetchPhaseMode) {
        return new EsqlFlags(
            ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY),
            ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_LEVEL.getDefault(Settings.EMPTY),
            ESQL_MAX_BRANCH_COUNT_PER_MERGE.getDefault(Settings.EMPTY),
            ESQL_MAX_FILLNULL_FIELDS.getDefault(Settings.EMPTY),
            ESQL_MAX_COMMAND_COUNT.getDefault(Settings.EMPTY),
            ESQL_METRICS_INFO_MAX_TIME_RANGE.getDefault(Settings.EMPTY),
            ESQL_TIME_SERIES_REDUCE_PER_INDEX.getDefault(Settings.EMPTY),
            fetchPhaseMode
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

    public TimeValue metricsInfoMaxTimeRange() {
        return metricsInfoMaxTimeRange;
    }

    public boolean timeSeriesReducePerIndex() {
        return timeSeriesReducePerIndex;
    }

    /**
     * The only place the feature flag is read for planning. A build with the flag leaves the fetch phase off until a
     * query turns it on with the {@code fetch_phase} pragma.
     */
    static FetchPhaseMode fetchPhaseMode(boolean featureFlagEnabled) {
        return featureFlagEnabled ? FetchPhaseMode.DISABLED : FetchPhaseMode.UNAVAILABLE;
    }

    /**
     * Whether the coordinator may plan the fetch phase, as far as the build is concerned. The {@code fetch_phase} pragma
     * of a query can override it. See {@link FetchPhaseMode}.
     */
    public FetchPhaseMode fetchPhaseMode() {
        return fetchPhaseMode;
    }

    public int maxBinaryOperatorsPerExpression() {
        return maxBinaryOperatorsPerExpression;
    }

    public int maxParenthesesCount() {
        return maxParenthesesCount;
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
            && timeSeriesReducePerIndex == that.timeSeriesReducePerIndex
            && Objects.equals(metricsInfoMaxTimeRange, that.metricsInfoMaxTimeRange)
            && fetchPhaseMode == that.fetchPhaseMode
            && maxBinaryOperatorsPerExpression == that.maxBinaryOperatorsPerExpression
            && maxParenthesesCount == that.maxParenthesesCount;
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
            metricsInfoMaxTimeRange,
            timeSeriesReducePerIndex,
            fetchPhaseMode,
            maxBinaryOperatorsPerExpression,
            maxParenthesesCount
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
            + ", metricsInfoMaxTimeRange="
            + metricsInfoMaxTimeRange
            + ", timeSeriesReducePerIndex="
            + timeSeriesReducePerIndex
            + ", fetchPhaseMode="
            + fetchPhaseMode
            + ", maxBinaryOperatorsPerExpression="
            + maxBinaryOperatorsPerExpression
            + ", maxParenthesesCount="
            + maxParenthesesCount
            + ']';
    }
}
