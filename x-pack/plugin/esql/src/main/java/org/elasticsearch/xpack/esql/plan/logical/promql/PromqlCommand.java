/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Container plan for embedded PromQL queries.
 * Gets eliminated by the analyzer once the query is validated.
 */
public class PromqlCommand extends UnaryPlan implements TelemetryAware, TimestampAware, TimestampBoundsAware.OfLogicalPlan {
    /**
     * The name of the column containing the step value (aka time bucket) in range queries.
     */

    public static final String TIME = "time";
    public static final String START = "start";
    public static final String END = "end";
    public static final String STEP = "step";
    public static final String BUCKETS = "buckets";
    public static final String SCRAPE_INTERVAL = "scrape_interval";
    public static final String RANGE = "range";
    public static final String INDEX = "index";
    public static final String DEFAULT_PROMQL_INDEX_PATTERN = "metrics-*";
    public static final Set<String> PROMQL_ALLOWED_PARAMS = Set.of(TIME, START, END, STEP, BUCKETS, SCRAPE_INTERVAL, INDEX);

    /** Synthetic column tagging each merge branch with its position, used for left-preferring dedup. */
    private static final String BRANCH_COLUMN = "_branch";

    /** Synthetic column name for the materialised {@code @timestamp + offset} expression. */
    private static final String TIMESTAMP_COLUMN = "_timestamp";

    // TODO make configurable via lookback_delta parameter and (cluster?) setting
    // Prometheus selector lookback delta for plain instant selectors without an explicit [range].
    public static final Duration DEFAULT_LOOKBACK = Duration.ofMinutes(5);
    public static final int DEFAULT_PROMQL_BUCKETS = 100;

    private final LogicalPlan promqlPlan;
    private final Literal start;
    private final Literal end;
    private final Literal step;
    private final Literal buckets;
    private final Literal scrapeInterval;
    // TODO: this should be made available through the planner
    private final Expression timestamp;
    private final String valueColumnName;
    private final NameId valueId;
    private final NameId stepId;
    private List<Attribute> output;

    // Range query constructor
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        Literal buckets,
        Literal scrapeInterval,
        String valueColumnName,
        Expression timestamp
    ) {
        this(source, child, promqlPlan, start, end, step, buckets, scrapeInterval, valueColumnName, new NameId(), new NameId(), timestamp);
    }

    // Full constructor
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        Literal buckets,
        Literal scrapeInterval,
        String valueColumnName,
        NameId valueId,
        NameId stepId,
        Expression timestamp
    ) {
        super(source, child);
        this.promqlPlan = promqlPlan;
        this.start = start;
        this.end = end;
        this.step = step;
        this.buckets = buckets;
        this.scrapeInterval = scrapeInterval;
        this.valueColumnName = valueColumnName;
        this.valueId = valueId;
        this.stepId = stepId;
        this.timestamp = timestamp;
    }

    @Override
    protected NodeInfo<PromqlCommand> info() {
        return NodeInfo.create(
            this,
            PromqlCommand::new,
            child(),
            promqlPlan(),
            start(),
            end(),
            step(),
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp()
        );
    }

    @Override
    public PromqlCommand replaceChild(LogicalPlan newChild) {
        return new PromqlCommand(
            source(),
            newChild,
            promqlPlan(),
            start(),
            end(),
            step(),
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp()
        );
    }

    public PromqlCommand withPromqlPlan(LogicalPlan newPromqlPlan) {
        if (newPromqlPlan == promqlPlan) {
            return this;
        }
        return new PromqlCommand(
            source(),
            child(),
            newPromqlPlan,
            start(),
            end(),
            step(),
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp()
        );
    }

    /**
     * Bounds are only needed when {@code buckets} is specified without an explicit time range.
     * When {@code step} alone is set, the query can proceed without start/end because the step
     * directly defines the bucket size; {@link #postAnalysisVerification} validates that case.
     */
    @Override
    public boolean needsTimestampBounds() {
        return buckets.value() != null && hasTimeRange() == false;
    }

    @Override
    public PromqlCommand withTimestampBounds(Literal start, Literal end) {
        return new PromqlCommand(
            source(),
            child(),
            promqlPlan(),
            start,
            end,
            step(),
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp()
        );
    }

    @Override
    public boolean expressionsResolved() {
        return promqlPlan.resolved() && timestamp.resolved();
    }

    @Override
    public String telemetryLabel() {
        return "PROMQL";
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("serialization not supported");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("serialization not supported");
    }

    public LogicalPlan promqlPlan() {
        return promqlPlan;
    }

    public Literal start() {
        return start;
    }

    public Literal end() {
        return end;
    }

    public Literal step() {
        return step;
    }

    /**
     * Number of buckets for auto-derived range-query bucket size.
     */
    public Literal buckets() {
        return buckets;
    }

    /**
     * The expected scrape interval used to derive implicit range selector windows.
     */
    public Literal scrapeInterval() {
        return scrapeInterval;
    }

    public boolean hasTimeRange() {
        return start.value() != null && end.value() != null;
    }

    public boolean isInstantQuery() {
        return step.value() == null && buckets.value() == null;
    }

    public boolean isRangeQuery() {
        return isInstantQuery() == false;
    }

    public String valueColumnName() {
        return valueColumnName;
    }

    public String stepColumnName() {
        return STEP;
    }

    /** Name of the synthetic column tagging each merge branch with its position, used for left-preferring dedup. */
    public String branchColumnName() {
        return BRANCH_COLUMN;
    }

    /** Name of the synthetic column materialising the offset-shifted {@code @timestamp + offset} evaluation time. */
    public String timestampColumnName() {
        return TIMESTAMP_COLUMN;
    }

    public NameId valueId() {
        return valueId;
    }

    public NameId stepId() {
        return stepId;
    }

    public ReferenceAttribute valueAttribute() {
        return new ReferenceAttribute(source(), null, valueColumnName, DataType.DOUBLE, Nullability.FALSE, valueId, false);
    }

    public ReferenceAttribute stepAttribute() {
        return new ReferenceAttribute(source(), null, stepColumnName(), DataType.DATETIME, Nullability.FALSE, stepId, false);
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            List<Attribute> additionalOutput = promqlPlan.output();
            output = new ArrayList<>(additionalOutput.size() + 2);
            output.add(valueAttribute());
            output.add(stepAttribute());
            output.addAll(additionalOutput);
        }
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), promqlPlan, start, end, step, buckets, scrapeInterval, valueColumnName, valueId, stepId, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {

            PromqlCommand other = (PromqlCommand) obj;
            return Objects.equals(child(), other.child())
                && Objects.equals(promqlPlan, other.promqlPlan)
                && Objects.equals(start, other.start)
                && Objects.equals(end, other.end)
                && Objects.equals(step, other.step)
                && Objects.equals(buckets, other.buckets)
                && Objects.equals(scrapeInterval, other.scrapeInterval)
                && Objects.equals(valueColumnName, other.valueColumnName)
                && Objects.equals(valueId, other.valueId)
                && Objects.equals(stepId, other.stepId)
                && Objects.equals(timestamp, other.timestamp);
        }

        return false;
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName());
        sb.append(" start=[").append(renderLiteral(start, mapper));
        sb.append("] end=[").append(renderLiteral(end, mapper));
        sb.append("] step=[").append(renderLiteral(step, mapper));
        sb.append("] buckets=[").append(renderLiteral(buckets, mapper));
        sb.append("] scrape_interval=[").append(renderLiteral(scrapeInterval, mapper));
        sb.append("] valueColumnName=[").append(valueColumnName == null ? "null" : mapper.column(valueColumnName));
        sb.append("] promql=[<>\n");
        sb.append(promqlPlan.toString(format, mapper));
        sb.append("\n<>]]");
    }

    // Route the literal value through the mapper rather than appending it raw: identity is the raw
    // value (matching the prior rendering), anonymization an interned token. No identity branch.
    private static String renderLiteral(Literal lit, NodeStringMapper mapper) {
        return lit == null ? "null" : mapper.literal(lit.value(), lit.dataType());
    }

    @Override
    protected AttributeSet computeReferences() {
        // Ensures field resolution is aware of all attributes used in the PromQL plan.
        AttributeSet.Builder references = AttributeSet.builder();
        promqlPlan().forEachDown(lp -> references.addAll(lp.references()));
        return references.build();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        throw new IllegalStateException(
            "PromqlCommand verification and translation should already have been completed: [" + sourceText() + "]"
        );
    }

    /** Admit the aggregation prototype once, before any node assumes typed metadata or complete named labels. */
    public void verify(Failures failures, AnalyzerContext analyzer) {
        if (analyzer.flags().promqlFunctionalTimeSeries() == false) {
            failures.add(
                fail(
                    this,
                    "The experimental PromQL aggregation path is disabled; enable [{}]",
                    EsqlFlags.ESQL_PROMQL_FUNCTIONAL_TIMESERIES.getKey()
                )
            );
            return;
        }
        if (TimeSeriesMetadataAttribute.supportTimeSeriesMetadataType(analyzer.minimumVersion()) == false) {
            failures.add(fail(this, "The experimental PromQL aggregation path requires support on every participating node"));
            return;
        }

        LogicalPlan p = promqlPlan();
        if (valueColumnName().equals(stepColumnName())
            || p.output().stream().anyMatch(attribute -> valueColumnName().equals(attribute.name()))) {
            failures.add(
                fail(
                    this,
                    "Result column name [{}] is already used by another output column; choose a different result column name",
                    valueColumnName()
                )
            );
            return;
        }
        boolean hasStep = step.value() != null;
        boolean hasRangeAndBuckets = start.value() != null && end.value() != null && buckets.value() != null;
        if (isInstantQuery() == false && hasStep == false && hasRangeAndBuckets == false) {
            failures.add(
                fail(
                    this,
                    "unable to create a bucket; provide either [{}] or all of [{}], [{}], and [{}] [{}]",
                    "step",
                    "start",
                    "end",
                    "buckets",
                    sourceText()
                )
            );
            return;
        }

        if (p instanceof RangeSelector && isRangeQuery()) {
            failures.add(
                fail(p, "invalid expression type \"range vector\" for range query, must be scalar or instant vector", p.sourceText())
            );
        } else if (p instanceof RangeSelector) {
            // Instant range-vector results require returning the raw samples rather than one value per evaluation step.
            failures.add(fail(p, "range vector results are not supported at this time [{}]", p.sourceText()));
        }
        verifyAggregationTree(p, failures);
    }

    /** Stop at selectors: their source relation is outside the PromQL expression being admitted. */
    private void verifyAggregationTree(LogicalPlan plan, Failures failures) {
        switch (plan) {
            case AcrossSeriesAggregate aggregate -> {
                for (Attribute grouping : aggregate.groupings()) {
                    if (stepColumnName().equals(grouping.name())) {
                        failures.add(
                            fail(
                                aggregate,
                                "label [{}] collides with the built-in [{}] output column [{}]",
                                stepColumnName(),
                                stepColumnName(),
                                aggregate.sourceText()
                            )
                        );
                    }
                }
                verifyAggregationTree(aggregate.child(), failures);
            }
            case WithinSeriesAggregate aggregate -> verifyAggregationTree(aggregate.child(), failures);
            case InstantSelector selector -> verifySelector(selector, failures);
            case RangeSelector selector -> verifySelector(selector, failures);
            default -> failures.add(
                fail(
                    plan,
                    "The experimental PromQL aggregation path supports only metric selectors, per-series range functions, "
                        + "and aggregations; unsupported expression [{}]",
                    plan.sourceText()
                )
            );
        }
    }

    private static void verifySelector(Selector selector, Failures failures) {
        if (selector.labelMatchers().nameLabel() != null && selector.labelMatchers().nameLabel().matcher().isRegex()) {
            failures.add(fail(selector, "regex label selectors on __name__ are not supported at this time [{}]", selector.sourceText()));
        }
        if (selector.series() == null) {
            failures.add(fail(selector, "__name__ label selector is required at this time [{}]", selector.sourceText()));
        }
        if (selector.evaluation() != null && selector.evaluation().at().value() != null) {
            failures.add(fail(selector, "@ modifiers are not supported at this time [{}]", selector.sourceText()));
        }
    }

    /**
     * The source selector's signed offset, as a constant time shift to add to {@code @timestamp}.
     * The admitted aggregation tree has one source selector. Returns {@link Duration#ZERO} when there is no offset.
     */
    public Duration collectFirstOffsetForBranch(LogicalPlan branch) {
        for (Selector selector : branch.collect(Selector.class)) {
            if (selector instanceof LiteralSelector == false && selector.evaluation() != null) {
                return selector.evaluation().offsetDuration();
            }
        }
        return Duration.ZERO;
    }

    /**
     * Returns the source-side timestamp lookback window.
     * Explicit and implicit range selectors contribute their requested window.
     * Instant queries extend that window to at least the Prometheus lookback delta.
     */
    public Duration sourceFilterWindow() {
        Duration window = maxRangeSelectorWindow();
        if (isInstantQuery() && DEFAULT_LOOKBACK.compareTo(window) > 0) {
            window = DEFAULT_LOOKBACK;
        }
        return window;
    }

    /**
     * Returns the local evaluation timestamp for the current selector branch.
     * <p>
     * Unlike {@link PromqlCommand#timestamp()}, which returns the global evaluation timestamp,
     * this function returns the plan branch timestamp and includes any applied offset.
     */
    public Expression collectEvaluationTimestampForBranch(LogicalPlan branch) {
        var offset = collectFirstOffsetForBranch(branch);
        var timestamp = timestamp();
        if (timestamp == null || timestamp.resolved() == false) {
            return timestamp;
        }
        boolean normalizeToMillis = timestamp.dataType() == DataType.DATE_NANOS;
        if (offset.isZero() && normalizeToMillis == false) {
            return timestamp;
        }
        // TODO: use unique names?
        DataType type = normalizeToMillis ? DataType.DATETIME : timestamp.dataType();
        return new ReferenceAttribute(source(), null, TIMESTAMP_COLUMN, type);
    }

    /**
     * Returns the TSTEP bucket step for instant queries: the max range-selector window,
     * falling back to {@link #DEFAULT_LOOKBACK} only when no range selectors are present.
     * Unlike {@link #sourceFilterWindow()}, this does not floor explicit windows up to
     * DEFAULT_LOOKBACK.
     */
    public Duration resolveInstantQueryWindow() {
        Duration window = maxRangeSelectorWindow();
        return window.isZero() ? DEFAULT_LOOKBACK : window;
    }

    /**
     * Resolves the implicit range placeholder to a concrete duration based on step and scrape interval.
     * The implicit window is calculated as {@code max(step, scrape_interval)}.
     */
    public Literal resolveImplicitRangeWindow() {
        Duration step = foldDuration(resolveTimeBucketSize(), STEP);
        Duration scrapeInterval = foldDuration(scrapeInterval(), SCRAPE_INTERVAL);
        return Literal.timeDuration(source(), step.compareTo(scrapeInterval) >= 0 ? step : scrapeInterval);
    }

    /**
     * The window a range selector reads: its explicit range, or the {@link #resolveImplicitRangeWindow() implicit window}
     * when the range is the placeholder an instant vector gets where a range vector is expected.
     */
    public Expression resolveRangeWindow(Expression range) {
        return isImplicitRangePlaceholder(range) ? resolveImplicitRangeWindow() : range;
    }

    public Expression resolveTimeBucketSize() {
        if (isRangeQuery()) {
            return Literal.timeDuration(source(), PromqlLogicalPlanBuilder.foldStep(timestamp(), start(), end(), step(), buckets()));
        }
        return Literal.timeDuration(source(), DEFAULT_LOOKBACK);
    }

    private Duration maxRangeSelectorWindow() {
        Duration window = Duration.ZERO;
        for (var selector : promqlPlan().collect(RangeSelector.class)) {
            var r = selector.range();
            Duration local;
            if (isImplicitRangePlaceholder(r)) {
                local = foldDuration(resolveImplicitRangeWindow(), RANGE);
            } else if (r.foldable()) {
                local = foldDuration(r, RANGE);
            } else {
                continue;
            }
            if (local.compareTo(window) > 0) {
                window = local;
            }
        }
        return window;
    }

    private static boolean isImplicitRangePlaceholder(Expression range) {
        return range.foldable()
            && range.fold(FoldContext.small()) instanceof Duration duration
            && duration.equals(PromqlLogicalPlanBuilder.IMPLICIT_RANGE_PLACEHOLDER);
    }

    private static Duration foldDuration(Expression expression, String paramName) {
        if (expression != null && expression.foldable() && expression.fold(FoldContext.small()) instanceof Duration duration) {
            return duration;
        }
        throw new QlIllegalArgumentException("Expected [{}] to be a duration literal, got [{}]", paramName, expression);
    }
}
