/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateUnitCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.isDateNanos;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNull;

/**
 * PromQL built-in function definitions that do not correspond to a dedicated ES|QL function class.
 */
public class PromqlBuiltinFunctionDefinitions {

    /**
     * {@code topk(k, v)} selects the {@code k} series with the highest values rather than reducing the input
     * vector to a single value, so - unlike {@code sum}/{@code avg}/{@code max}/{@code min} - it has no ES|QL
     * aggregate function to build: the ctor reference passes the input field through unchanged, and the PromQL
     * translator (see {@code AcrossSeriesReduction#translate}) appends the ranking/limiting step itself.
     */
    private static final String LABEL_FUNCTION_NOT_SUPPORTED_YET =
        "A call that isn't aggregated or is under `without(...)`, and a destination label named `step`, which is an output "
            + "column of the `PROMQL` command, aren't supported yet.\n\n"
            + "{applies_to}`stack: ga =9.6` A call over an aggregation, such as `label_replace(sum by (job) (x), ...)`, "
            + "isn't supported yet.\n\n"
            + "{applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` A call over anything other than a `by(...)` aggregation, "
            + "such as `sum(x)`, a `without(...)` aggregation, `topk`, or a binary operator, and a call that overwrites a "
            + "grouping label, such as `label_replace(sum by (job) (x), \"job\", \"$1\", \"job\", \"(.*)\")`, aren't "
            + "supported yet. Write a new label instead.\n\n";

    private static final String SORT_DIVERGENCE =
        "A call below the top level of the query, such as `abs(sort(x))`, isn't supported yet. {{prom}} applies the "
            + "ordering there too. On a range query the ordering is discarded in both, but {{prom}} returns the series ordered by their "
            + "labels, where {{es}} returns them in input order.";

    public static final PromqlFunctionDefinition TOPK = PromqlFunctionDefinition.def()
        .acrossSeriesBinaryReduceSortDesc(PromqlFunctionDefinition.K)
        .description(
            "Returns `k` time series with the highest values, keeping their full label set. "
                + "When used with `by`, `topk` ranks independently within each group."
        )
        .example("topk(3, http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .stdDivergence(
            "`without(...)`, such as `topk without (instance) (1, x)`, and a `k` that isn't a number literal, such as "
                + "`topk(scalar(a), b)`, aren't supported yet. "
                + "A `k` close to `Integer.MAX_VALUE` can trip {{es}}'s circuit breaker (the execution engine allocates a "
                + "buffer sized to `k`, not to the number of matching series), whereas {{prom}} has no equivalent limit. "
                + "A `NaN` value ranks above `+Inf` rather than last, so a series whose value is `NaN` wins a slot ahead "
                + "of a series with a comparable value. {{prom}} ranks `NaN` farthest from the top and returns the "
                + "comparable series instead. `bottomk` is unaffected, because its ascending ranking already places "
                + "`NaN` last."
        )
        .name("topk");

    /**
     * {@code bottomk(k, v)} is the ascending counterpart to {@link #TOPK}; it uses the same passthrough aggregation
     * and switches only the ranking direction in {@code TopNBy}.
     */
    public static final PromqlFunctionDefinition BOTTOMK = PromqlFunctionDefinition.def()
        .acrossSeriesBinaryReduceSortAsc(PromqlFunctionDefinition.K)
        .description(
            "Returns `k` time series with the lowest values, keeping their full label set. "
                + "When used with `by`, `bottomk` ranks independently within each group."
        )
        .example("bottomk(3, http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .stdDivergence(
            "`without(...)`, such as `bottomk without (instance) (1, x)`, isn't supported yet. "
                + "A `k` close to `Integer.MAX_VALUE` can trip {{es}}'s circuit breaker (the execution engine allocates a "
                + "buffer sized to `k`, not to the number of matching series), whereas {{prom}} has no equivalent limit."
        )
        .name("bottomk");

    /**
     * {@code limitk(k, v)} returns {@code k} arbitrary elements (sample) from the input vector.
     */
    public static final PromqlFunctionDefinition LIMITK = PromqlFunctionDefinition.def()
        .acrossSeriesBinaryReduceUnordered(PromqlFunctionDefinition.K)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "Returns `k` arbitrary elements from the input vector, keeping their full label set. "
                + "Unlike `topk` and `bottomk`, selection is not sort-based and histogram samples are included."
        )
        .example("limitk(3, http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .stdDivergence(
            "`without(...)`, such as `limitk without (instance) (1, x)`, isn't supported yet. "
                + "Within a shard, elements follow storage order (first-k) rather than truly arbitrary order. "
                + "Across shards the merge keeps k of those partials with no global tsid sort, so the subset is not "
                + "stable. "
                + "A `k` close to `Integer.MAX_VALUE` can trip {{es}}'s circuit breaker (the execution engine allocates a "
                + "buffer sized to `k`, not to the number of matching series), whereas {{prom}} has no equivalent limit."
        )
        .name("limitk");

    /**
     * {@code limit_ratio(r, v)} keeps each series iff its identity-hash sampling offset falls below {@code r},
     * like Prometheus. The kept subset is stable across node restarts; no per-group state.
     */
    public static final PromqlFunctionDefinition LIMIT_RATIO = PromqlFunctionDefinition.def()
        .acrossSeriesBinaryRatioReduce(PromqlFunctionDefinition.RATIO)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Returns a ratio `r` of the series from the input vector, keeping their full label set.")
        .example("limit_ratio(0.5, http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .stdDivergence(
            "`without(...)`, such as `limit_ratio without (instance) (0.5, x)`, isn't supported yet. "
                + "Series are kept by hashing the series identity rather than the {{prom}} label serialization, "
                + "so the kept subset has the same statistical properties but is generally a different subset than "
                + "the one {{prom}} keeps. `by` is a membership no-op as in "
                + "{{prom}}."
        )
        .name("limit_ratio");

    /**
     * {@code label_replace(v, dst_label, replacement, src_label, regex)} matches {@code regex} (fully anchored) against the
     * value of {@code src_label} and, on a match, sets {@code dst_label} to the expanded {@code replacement}. It manipulates
     * only labels/identity, never sample values, so it has no ES|QL function to build: it resolves into a dedicated node and
     * is translated directly (see {@code ResolvePromqlFunctions} / {@code MetadataManipulationFunction#translate}).
     */
    public static final PromqlFunctionDefinition LABEL_REPLACE = PromqlFunctionDefinition.def()
        .metadataManipulation(
            new PromqlFunctionDefinition.PromqlFunctionArity(5, 5),
            false,
            List.of(
                PromqlFunctionDefinition.INSTANT_VECTOR,
                PromqlFunctionDefinition.PromqlParamInfo.of("dst_label", PromqlDataType.SCALAR, "Name of the label to set."),
                PromqlFunctionDefinition.PromqlParamInfo.of(
                    "replacement",
                    PromqlDataType.SCALAR,
                    "Replacement value, with `$1`/`$name`/`${name}` capture-group expansion."
                ),
                PromqlFunctionDefinition.PromqlParamInfo.of("src_label", PromqlDataType.SCALAR, "Name of the label to read."),
                PromqlFunctionDefinition.PromqlParamInfo.of(
                    "regex",
                    PromqlDataType.SCALAR,
                    "Regular expression matched against `src_label`."
                )
            )
        )
        .description(
            "Matches the regular expression `regex` against the value of the label `src_label`. On a match, sets the label "
                + "`dst_label` to the expansion of `replacement`, substituting `$1`, `$name`, and `${name}` with the matched "
                + "capture groups; on no match the input series is returned unchanged."
        )
        .example("""
            sum by (job2) (label_replace(http_requests_total, "job2", "$1", "job", "(.*)-server"))""")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .extendedDescription(
            "The destination can be a new label or an existing stored label (a dimension or `__name__`). Regular "
                + "expressions use the RE2 engine (RE2 syntax including `(?P<name>)`, `$1`/`$name`/`${name}` replacement "
                + "expansion, no backreferences, fully anchored as `^(?s:regex)$`), matching Prometheus.\n\n"
                + "{applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` A call over a `by(...)` aggregation, directly or under "
                + "`topk`, `bottomk`, `limitk`, or `limit_ratio`, is supported when the destination is a new label, such as "
                + "`label_replace(sum by (job) (x), \"dst\", \"$1\", \"job\", \"(.*)\")`."
        )
        .stdDivergence(
            LABEL_FUNCTION_NOT_SUPPORTED_YET
                + "Reading or overwriting `__name__` behaves like {{prom}} only for {{prom}}-style data that stores `__name__` as a label; "
                + "for OpenTelemetry-style metrics the metric name is not exposed as a readable or writable label here."
        )
        .name("label_replace");

    /**
     * {@code label_join(v, dst_label, separator, src_label_1, ... src_label_N)} sets {@code dst_label} to the values of the
     * source labels joined by {@code separator}. Variadic in the source labels. Like {@code label_replace} it manipulates
     * only labels/identity and is translated directly rather than lowered through the generic function builder.
     */
    public static final PromqlFunctionDefinition LABEL_JOIN = PromqlFunctionDefinition.def()
        .metadataManipulation(
            new PromqlFunctionDefinition.PromqlFunctionArity(3, Integer.MAX_VALUE),
            true,
            List.of(
                PromqlFunctionDefinition.INSTANT_VECTOR,
                PromqlFunctionDefinition.PromqlParamInfo.of("dst_label", PromqlDataType.SCALAR, "Name of the label to set."),
                PromqlFunctionDefinition.PromqlParamInfo.of(
                    "separator",
                    PromqlDataType.SCALAR,
                    "String inserted between the source values."
                ),
                PromqlFunctionDefinition.PromqlParamInfo.of("src_label", PromqlDataType.SCALAR, "Name of a label to join (repeatable).")
            )
        )
        .description(
            "Joins the values of the source labels `src_label_1` .. `src_label_N` using `separator` and stores the result in "
                + "the label `dst_label`. A missing source label contributes an empty string."
        )
        .example("""
            sum by (endpoint) (label_join(http_requests_total, "endpoint", "/", "job", "instance"))""")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .extendedDescription(
            "The destination can be a new label or an existing stored label (a dimension or `__name__`).\n\n"
                + "{applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` A call over a `by(...)` aggregation, directly or under "
                + "`topk`, `bottomk`, `limitk`, or `limit_ratio`, is supported when the destination is a new label, such as "
                + "`label_replace(sum by (job) (x), \"dst\", \"$1\", \"job\", \"(.*)\")`."
        )
        .stdDivergence(
            LABEL_FUNCTION_NOT_SUPPORTED_YET
                + "Reading or overwriting `__name__` behaves like {{prom}} only for {{prom}}-style data that stores `__name__` as a label; "
                + "for OpenTelemetry-style metrics the metric name is not exposed as a readable or writable label here."
        )
        .name("label_join");

    public static final PromqlFunctionDefinition SORT = PromqlFunctionDefinition.def()
        .resultOrdering()
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Sorts instant-vector series by sample value in ascending order. NaN values are sorted last.")
        .example("sort(http_requests_total)")
        .extendedDescription(
            "Ordering is observable only on instant queries. Range queries return the series in their input order, "
                + "without applying the requested ordering."
        )
        .stdDivergence(SORT_DIVERGENCE)
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .name("sort");

    public static final PromqlFunctionDefinition SORT_DESC = PromqlFunctionDefinition.def()
        .resultOrdering()
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Sorts instant-vector series by sample value in descending order. NaN values are sorted last.")
        .example("sort_desc(http_requests_total)")
        .extendedDescription(
            "Ordering is observable only on instant queries. Range queries return the series in their input order, "
                + "without applying the requested ordering."
        )
        .stdDivergence(SORT_DIVERGENCE)
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .name("sort_desc");

    /**
     * Behavior shared by {@code sort_by_label} and {@code sort_by_label_desc}, shown on each function's own page.
     */
    private static final String SORT_BY_LABEL_BEHAVIOR = "Ordering is observable only on instant queries. Range queries "
        + "return the series in their input order, without applying the requested ordering. Label comparison uses "
        + "natural order, with sequences of digits compared numerically.";

    /**
     * Prometheus divergences shared by {@code sort_by_label} and {@code sort_by_label_desc}. One bullet per divergence
     * so the generated "Differences from Prometheus" section stays scannable.
     */
    private static final String SORT_BY_LABEL_DIFFERENCES = "- A call below the top level of the query, such as "
        + "`abs(sort_by_label(x, \"job\"))`, isn't supported yet. {{prom}} applies the ordering there too.\n"
        + "- When the requested labels are equal, ties are broken by series identity, approximating {{prom}} "
        + "`labels.Compare`. The order can differ in edge cases, such as a series missing a label or labels of "
        + "non-string types.\n"
        + "- On a range query the ordering is discarded in both, but {{prom}} returns the series ordered by their "
        + "labels, where {{es}} returns them in input order.\n"
        + "- Sequences of digits that are numerically equal but differ in leading zeros (for example, `item1` and "
        + "`item01`), and empty label values, get a stable order rather than {{prom}}'s unstable natural sort order.\n"
        + "- Sequences of digits longer than about 20 digits are still compared numerically, where {{prom}} falls back "
        + "to text comparison.";

    public static final PromqlFunctionDefinition SORT_BY_LABEL = PromqlFunctionDefinition.def()
        .resultOrderingByLabel()
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "Sorts instant-vector series by the values of the given labels in natural ascending order. "
                + "Missing labels compare as the empty string."
        )
        .example("sort_by_label(http_requests_total, \"job\", \"instance\")")
        .extendedDescription(SORT_BY_LABEL_BEHAVIOR)
        .stdDivergence(SORT_BY_LABEL_DIFFERENCES)
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_7)
        .name("sort_by_label");

    public static final PromqlFunctionDefinition SORT_BY_LABEL_DESC = PromqlFunctionDefinition.def()
        .resultOrderingByLabel()
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "Sorts instant-vector series by the values of the given labels in natural descending order. "
                + "Missing labels compare as the empty string."
        )
        .example("sort_by_label_desc(http_requests_total, \"job\", \"instance\")")
        .extendedDescription(SORT_BY_LABEL_BEHAVIOR)
        .stdDivergence(SORT_BY_LABEL_DIFFERENCES)
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_7)
        .name("sort_by_label_desc");

    public static final PromqlFunctionDefinition VECTOR = PromqlFunctionDefinition.def()
        .vectorConversion()
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Returns the scalar as a vector with no labels.")
        .example("vector(1)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("vector");

    static final PromqlFunctionDefinition SCALAR = PromqlFunctionDefinition.def()
        .scalarConversion(Scalar::new)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            Returns the sample value of a single-element instant vector as a scalar. \
            If the input vector does not have exactly one element, scalar returns NaN.""")
        .example("scalar(sum(http_requests_total))")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("scalar");

    static final PromqlFunctionDefinition YEAR = dateExtraction(ChronoField.YEAR).description(
        "Returns the year for each of the input timestamps (in UTC)."
    ).example("year()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("year");

    static final PromqlFunctionDefinition MONTH = dateExtraction(ChronoField.MONTH_OF_YEAR).description(
        "Returns the month of the year for each of the input timestamps (in UTC). Returned values are from 1 to 12."
    ).example("month()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("month");

    static final PromqlFunctionDefinition DAY_OF_MONTH = dateExtraction(ChronoField.DAY_OF_MONTH).description(
        "Returns the day of the month for each of the input timestamps (in UTC). Returned values are from 1 to 31."
    ).example("day_of_month()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("day_of_month");

    static final PromqlFunctionDefinition DAY_OF_YEAR = dateExtraction(ChronoField.DAY_OF_YEAR).description(
        "Returns the day of the year for each of the input timestamps (in UTC). Returned values are from 1 to 366."
    ).example("day_of_year()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("day_of_year");

    // DateExtract(DAY_OF_WEEK) returns 1=Mon..7=Sun; PromQL expects 0=Sun..6=Sat, so we apply % 7.
    static final PromqlFunctionDefinition DAY_OF_WEEK = PromqlFunctionDefinition.def()
        .dateTime(
            (source, date, configuration) -> new Mod(
                source,
                new ToDouble(
                    source,
                    new DateExtract(
                        source,
                        Literal.keyword(source, ChronoField.DAY_OF_WEEK.name()),
                        date,
                        configuration.withSetting(QuerySettings.TIME_ZONE, ZoneOffset.UTC)
                    )
                ),
                Literal.fromDouble(source, 7.0)
            )
        )
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "Returns the day of the week for each of the input timestamps (in UTC). Returned values are from 0 to 6, where 0 means Sunday."
        )
        .example("day_of_week()")
        .stack(PromqlFunctionDefinition.STACK_GA_9_5)
        .name("day_of_week");

    static final PromqlFunctionDefinition HOUR = dateExtraction(ChronoField.HOUR_OF_DAY).description(
        "Returns the hour of the day for each of the input timestamps (in UTC). Returned values are from 0 to 23."
    ).example("hour()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("hour");

    static final PromqlFunctionDefinition MINUTE = dateExtraction(ChronoField.MINUTE_OF_HOUR).description(
        "Returns the minute of the hour for each of the input timestamps (in UTC). Returned values are from 0 to 59."
    ).example("minute()").stack(PromqlFunctionDefinition.STACK_GA_9_5).name("minute");

    static final PromqlFunctionDefinition DAYS_IN_MONTH = PromqlFunctionDefinition.def()
        .dateTime(
            (source, date, configuration) -> new ToDouble(
                source,
                new DateUnitCount(
                    source,
                    Literal.keyword(source, "day"),
                    Literal.keyword(source, "month"),
                    date,
                    configuration.withSetting(QuerySettings.TIME_ZONE, ZoneOffset.UTC)
                )
            )
        )
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "Returns the number of days in the month for each of the input timestamps (in UTC). Returned values are from 28 to 31."
        )
        .example("days_in_month()")
        .stack(PromqlFunctionDefinition.STACK_GA_9_5)
        .name("days_in_month");

    static final PromqlFunctionDefinition TIME = PromqlFunctionDefinition.def()
        .scalarWithStep((source, step) -> new Div(source, new ToDouble(source, step), Literal.fromDouble(source, 1000.0)))
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            Returns the number of seconds since January 1, 1970 UTC. \
            Note that this does not actually return the current time, but the time at which the expression is being evaluated.""")
        .example("time()")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("time");

    /**
     * {@code timestamp(v)} returns each sample's timestamp as seconds since the Unix epoch.
     * <p>
     * Instant selectors keep the scrape timestamp of the selected sample (via {@code last_over_time} on the
     * evaluation timestamp field, restricted to documents that have the metric). Aggregations and range-vector
     * functions stamp results at the evaluation timestamp, matching Prometheus.
     */
    static final PromqlFunctionDefinition TIMESTAMP = PromqlFunctionDefinition.def()
        .unaryTimeExtraction(
            (source, target, ctx, extraParams) -> new Div(
                source,
                new ToDouble(source, sampleTimestamp(source, target, ctx)),
                Literal.fromDouble(source, 1000.0)
            )
        )
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            Returns the timestamp of each sample of the given vector as the number of seconds since January 1, 1970 UTC. \
            It acts on float and histogram samples in the same way.""")
        .example("timestamp(http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .name("timestamp");

    static final PromqlFunctionDefinition ROUND = PromqlFunctionDefinition.def()
        .binaryOptionalValueTransformation(PromqlFunctionDefinition.TO_NEAREST, (source, value, toNearest, configuration) -> {
            if (toNearest == null) {
                return new Round(source, value, null, true);
            } else {
                return promqlRoundToNearest(source, value, toNearest, configuration);
            }
        })
        .example("round(rate(http_requests_total[5m]))")
        .description("Rounds the sample values to the nearest integer, or to the nearest multiple of the optional argument.")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("round");

    /**
     * PromQL {@code round(v, to_nearest)} rounds to the nearest multiple of {@code to_nearest},
     * with ties resolved by rounding up. Matches Prometheus:
     * {@code floor(v * (1 / to_nearest) + 0.5) / (1 / to_nearest)}.
     * <p>
     * The arithmetic nodes built here use the non-finite-preserving math variant so the degenerate {@code to_nearest = 0}
     * input reproduces Prometheus's result of {@code NaN} ({@code 1 / 0 = +Inf}, {@code v * +Inf = ±Inf/NaN},
     * {@code floor(...) / +Inf = NaN}) instead of hitting the divide-by-zero guard and dropping the series.
     * </p>
     */
    private static Expression promqlRoundToNearest(Source source, Expression value, Expression toNearest, Configuration configuration) {
        Expression inverse = new Div(source, Literal.fromDouble(source, 1.0), toNearest, null, true);
        Expression half = Literal.fromDouble(source, 0.5);
        Expression scaled = new Mul(source, value, inverse, true);
        Expression withHalf = new Add(source, scaled, half, configuration, true);
        return new Div(source, new Floor(source, withHalf), inverse, null, true);
    }

    private static PromqlFunctionDefinition.Builder dateExtraction(ChronoField field) {
        return PromqlFunctionDefinition.def()
            .dateTime(
                (source, date, configuration) -> new ToDouble(
                    source,
                    new DateExtract(
                        source,
                        Literal.keyword(source, field.name()),
                        date,
                        configuration.withSetting(QuerySettings.TIME_ZONE, ZoneOffset.UTC)
                    )
                )
            )
            .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED);
    }

    /**
     * Resolves the timestamp expression for {@code timestamp(v)}.
     * Instant-selector {@link LastOverTime} (no window) preserves scrape timestamps; otherwise the evaluation
     * step/timestamp is used, matching Prometheus sample stamping after aggregations and range functions.
     */
    private static Expression sampleTimestamp(Source source, Expression target, PromqlFunctionRegistry.PromqlContext ctx) {
        LastOverTime instantLast = findInstantSelectorLastOverTime(target);
        if (instantLast != null) {
            Expression metricPresent = new IsNotNull(source, instantLast.field());
            Expression filter = Literal.TRUE.equals(instantLast.filter())
                ? metricPresent
                : new And(source, instantLast.filter(), metricPresent);
            return new LastOverTime(source, instantLast.timestamp(), instantLast.timestamp(), filter, instantLast.window());
        }
        if (isDateTime(target.dataType()) || isDateNanos(target.dataType())) {
            return target;
        }
        Expression step = ctx.step();
        return (step == null || isNull(step.dataType())) ? ctx.timestamp() : step;
    }

    /** Finds the windowless {@link LastOverTime} produced for an instant selector, possibly under value transforms. */
    private static LastOverTime findInstantSelectorLastOverTime(Expression expression) {
        if (expression instanceof LastOverTime lastOverTime && lastOverTime.hasWindow() == false) {
            return lastOverTime;
        }
        for (Expression child : expression.children()) {
            LastOverTime found = findInstantSelectorLastOverTime(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private PromqlBuiltinFunctionDefinitions() {}
}
