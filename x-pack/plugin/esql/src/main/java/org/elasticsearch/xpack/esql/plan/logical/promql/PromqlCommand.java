/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
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

    /** Synthetic column tagging each union branch with its position, used for left-preferring dedup. */
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

    /** Name of the synthetic column tagging each union branch with its position, used for left-preferring dedup. */
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

    public void verify(Failures failures) {
        LogicalPlan p = promqlPlan();
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
        }

        // Validate entire plan
        // UNION set operators that form a contiguous chain from the root (e.g. `(a or b) or c`) are all
        // considered "top-level": they are flattened into a single UnionAll during translation.
        Set<VectorBinarySet> topLevelUnions = collectTopLevelUnionChain(p);
        if (topLevelUnions.isEmpty() == false) {
            // A connected chain of U union nodes has U+1 leaf operands (branches), which the translator combines
            // into a single UnionAll. Reject chains exceeding the UnionAll branch limit with a clear message here
            // rather than failing later during translation.
            int branchCount = topLevelUnions.size() + 1;
            if (Fork.exceedsMaxBranches(branchCount)) {
                failures.add(fail(p, "PromQL set operator [or] supports up to [{}] operands, got [{}]", Fork.MAX_BRANCHES, branchCount));
            }
        }
        Holder<Boolean> root = new Holder<>(true);
        p.forEachDown(lp -> {
            switch (lp) {
                case Selector s -> {
                    if (s.labelMatchers().nameLabel() != null && s.labelMatchers().nameLabel().matcher().isRegex()) {
                        failures.add(fail(s, "regex label selectors on __name__ are not supported at this time [{}]", s.sourceText()));
                    }
                    if (s.series() == null) {
                        failures.add(fail(s, "__name__ label selector is required at this time [{}]", s.sourceText()));
                    }
                    if (s.evaluation() != null) {
                        // Only constant per-selector time shift is supported at the moment.
                        // TODO(sidosera): Support heterogeneous offset on binary operators.
                        if (s.evaluation().at().value() != null) {
                            failures.add(fail(s, "@ modifiers are not supported at this time [{}]", s.sourceText()));
                        }
                    }
                }
                case AcrossSeriesAggregate agg -> {
                    if (agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT && usesWithoutGrouping(agg.child())) {
                        failures.add(fail(agg, "nested WITHOUT over WITHOUT is not supported at this time [{}]", agg.sourceText()));
                    }
                    // Reject labels whose name collides with the built-in step column.
                    // If this proves too restrictive, we could add an option to rename the built-in step column.
                    for (Attribute grouping : agg.groupings()) {
                        if (stepColumnName().equals(grouping.name())) {
                            failures.add(
                                fail(
                                    agg,
                                    "label [{}] collides with the built-in [{}] output column [{}]",
                                    stepColumnName(),
                                    stepColumnName(),
                                    agg.sourceText()
                                )
                            );
                        }
                    }
                }
                case PromqlFunctionCall functionCall -> {
                    // ok — counter/gauge type mismatches are coerced during translation
                }
                case ScalarFunction scalarFunction -> {
                    // ok
                }
                case VectorBinaryOperator binaryOperator -> {
                    binaryOperator.children().forEach(child -> {
                        if (child instanceof RangeSelector) {
                            failures.add(
                                fail(child, "binary expression must contain only scalar and instant vector types", child.sourceText())
                            );
                        }
                    });
                    if (binaryOperator.match() != VectorMatch.NONE) {
                        failures.add(
                            fail(
                                lp,
                                "{} queries with group modifiers are not supported at this time [{}]",
                                lp.getClass().getSimpleName(),
                                lp.sourceText()
                            )
                        );
                    }
                    if (binaryOperator instanceof VectorBinaryComparison comp) {
                        if (root.get() == false) {
                            failures.add(
                                fail(lp, "comparison operators are only supported at the top-level at this time [{}]", lp.sourceText())
                            );
                        }
                        if (comp.right() instanceof LiteralSelector == false) {
                            failures.add(
                                fail(
                                    lp,
                                    "comparison operators with non-literal right-hand side are not supported at this time [{}]",
                                    lp.sourceText()
                                )
                            );
                        }
                        if (comp.boolMode() == false && PromqlPlan.returnsScalar(comp.left()) && PromqlPlan.returnsScalar(comp.right())) {
                            failures.add(fail(comp, "Comparisons [{}] between scalars must use the BOOL modifier", comp.op()));
                        }
                    }
                    if (binaryOperator instanceof VectorBinarySet setOp) {
                        verifySetOperator(failures, setOp, topLevelUnions.contains(setOp));
                    }
                    if (usesWithoutGrouping(binaryOperator.left()) || usesWithoutGrouping(binaryOperator.right())) {
                        failures.add(fail(lp, "binary expressions with WITHOUT are not supported at this time [{}]", lp.sourceText()));
                    }
                    if (hasSourceBackedExpression(binaryOperator.left())
                        && hasSourceBackedExpression(binaryOperator.right())
                        && (usesNestedAcrossSeriesAggregation(binaryOperator.left())
                            || usesNestedAcrossSeriesAggregation(binaryOperator.right()))) {
                        failures.add(
                            fail(lp, "binary expressions with nested aggregations are not supported at this time [{}]", lp.sourceText())
                        );
                    }
                    // Arithmetic/comparison binary operators merge both source-backed operands into a single
                    // TimeSeriesAggregate (one shared time bucket and timestamp), which cannot represent two
                    // different offsets. `or` (UNION) translates to independent branches, so per-branch offsets
                    // are fine and excluded here.
                    if (binaryOperator instanceof VectorBinarySet == false
                        && hasSourceBackedExpression(binaryOperator.left())
                        && hasSourceBackedExpression(binaryOperator.right())
                        && distinctSelectorOffsets(binaryOperator).size() > 1) {
                        failures.add(
                            fail(lp, "binary expressions with different offsets are not supported at this time [{}]", lp.sourceText())
                        );
                    }
                }
                case PlaceholderRelation placeholderRelation -> {
                    // ok
                }
                default -> failures.add(
                    fail(lp, "{} queries are not supported at this time [{}]", lp.getClass().getSimpleName(), lp.sourceText())
                );
            }
            root.set(false);
        });
    }

    /**
     * Set operators ({@code and}/{@code or}/{@code unless}) are only partially supported. Phase 1 allows the
     * {@code or} (UNION) operator when it appears at the top level of the expression and both operands are
     * instant vectors. The failures here fall into two categories:
     * <ul>
     *   <li>Scalar operands are illegal for set operators in PromQL itself, not just in our implementation. We
     *       mirror Prometheus' wording ({@code set operator "or" not allowed in binary scalar expression}) and
     *       check it first, so the message does not falsely imply the shape might be supported later.</li>
     *   <li>Unsupported {@code and}/{@code unless} operators and non-top-level {@code or} are genuine current
     *       implementation limitations, flagged with "at this time".</li>
     * </ul>
     */
    private static void verifySetOperator(Failures failures, VectorBinarySet setOp, boolean isTopLevelUnion) {
        if (PromqlPlan.returnsScalar(setOp.left()) || PromqlPlan.returnsScalar(setOp.right())) {
            failures.add(fail(setOp, "set operator \"{}\" not allowed in binary scalar expression", setOp.op().keyword()));
            return;
        }
        if (setOp.op() != VectorBinarySet.SetOp.UNION) {
            failures.add(fail(setOp, "set operator [{}] is not supported at this time [{}]", setOp.op().keyword(), setOp.sourceText()));
            return;
        }
        if (isTopLevelUnion == false) {
            failures.add(fail(setOp, "set operator [or] is only supported at the top-level at this time [{}]", setOp.sourceText()));
        }
    }

    /**
     * Collects the {@link VectorBinarySet} UNION nodes that form a contiguous chain starting at the plan root.
     * PromQL {@code or} is left-associative, so {@code a or b or c} parses to {@code (a or b) or c}; explicit
     * parentheses can also produce right-nested chains. All such union nodes are flattened into a single
     * {@code UnionAll} during translation, so the verifier treats them all as top-level.
     */
    private static Set<VectorBinarySet> collectTopLevelUnionChain(LogicalPlan p) {
        Set<VectorBinarySet> chain = new HashSet<>();
        collectTopLevelUnionChain(p, chain);
        return chain;
    }

    private static void collectTopLevelUnionChain(LogicalPlan p, Set<VectorBinarySet> chain) {
        if (p instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
            chain.add(setOp);
            collectTopLevelUnionChain(setOp.left(), chain);
            collectTopLevelUnionChain(setOp.right(), chain);
        }
    }

    private static boolean hasSourceBackedExpression(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof Selector && (p instanceof LiteralSelector) == false);
    }

    private static boolean usesNestedAcrossSeriesAggregation(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof AcrossSeriesAggregate agg && agg.child().anyMatch(AcrossSeriesAggregate.class::isInstance));
    }

    private static boolean usesWithoutGrouping(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof AcrossSeriesAggregate agg && agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT);
    }

    /**
     * Collects the distinct signed offsets of the source-backed selectors under {@code plan}. Literal selectors
     * carry no data window and are excluded. More than one distinct value within a merged binary operator is
     * unsupported (see the verifier guard).
     */
    private static Set<Duration> distinctSelectorOffsets(LogicalPlan plan) {
        Set<Duration> offsets = new HashSet<>();
        for (Selector selector : plan.collect(Selector.class)) {
            if (selector instanceof LiteralSelector == false && selector.evaluation() != null) {
                offsets.add(selector.evaluation().offsetDuration());
            }
        }
        return offsets;
    }

    /**
     * The signed offset shared by the source-backed selectors in {@code branch}, as a constant time shift to add to
     * {@code @timestamp}. The verifier guarantees a merged branch is offset-uniform (heterogeneous offsets in a binary
     * expression are rejected), so the first source-backed selector's offset is representative. {@link Duration#ZERO}
     * when there is none.
     */
    public Duration offset(LogicalPlan branch) {
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
     * this function returns the plan fragment timestamp and includes any applied offset.
     */
    public Expression timestamp(LogicalPlan fragment) {
        var offset = offset(fragment);
        var timestamp = timestamp();
        if (offset.isZero() || timestamp == null || timestamp.resolved() == false) {
            return timestamp;
        }
        // TODO: use unique names?
        return new ReferenceAttribute(source(), null, TIMESTAMP_COLUMN, timestamp.dataType());
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
