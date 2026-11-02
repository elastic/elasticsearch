/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryArithmetic;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

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
    private final NameId timeseriesId;
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
        this(
            source,
            child,
            promqlPlan,
            start,
            end,
            step,
            buckets,
            scrapeInterval,
            valueColumnName,
            new NameId(),
            new NameId(),
            new NameId(),
            timestamp
        );
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
        NameId timeseriesId,
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
        this.timeseriesId = timeseriesId;
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
            timeseriesId(),
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
            timeseriesId(),
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
            timeseriesId(),
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
            timeseriesId(),
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

    /** The id of the {@code _timeseries} column the command exposes for its result's time-series metadata. */
    public NameId timeseriesId() {
        return timeseriesId;
    }

    /**
     * The type of the {@code _timeseries} column the command exposes: typed metadata wherever the type is enabled, whatever
     * the participating nodes read - a keyword read on an older node is cast to it. Keyword where the type is disabled.
     */
    public static DataType timeseriesType() {
        return EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE.isEnabled() ? DataType.TIMESERIES_DATA_TYPE : DataType.KEYWORD;
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
            List<Attribute> labels = promqlPlan.output();
            output = new ArrayList<>(labels.size() + 2);
            output.add(valueAttribute());
            output.add(stepAttribute());
            for (Attribute label : labels) {
                // the result's time-series metadata is exposed as the one _timeseries column, under the command's own id
                if (PromqlLabels.isMetadataLabel(label)) {
                    output.add(
                        new ReferenceAttribute(
                            source(),
                            null,
                            MetadataAttribute.TIMESERIES,
                            timeseriesType(),
                            Nullability.TRUE,
                            timeseriesId,
                            false
                        )
                    );
                    continue;
                }
                // A BY label named "value" (or rarely "step") would duplicate the sample/step columns. Keep the
                // labels. prefix so Project names stay unique; the Prometheus response listener strips it for metric keys.
                String canonical = PromqlLabels.labelName(label);
                if (canonical.equals(valueColumnName) || canonical.equals(stepColumnName())) {
                    String qualified = PromqlLabels.PROMETHEUS_LABELS_PREFIX + canonical;
                    output.add(
                        label.name().equals(qualified)
                            ? label
                            : new ReferenceAttribute(
                                label.source(),
                                null,
                                qualified,
                                label.dataType(),
                                label.nullable(),
                                label.id(),
                                label.synthetic()
                            )
                    );
                } else {
                    output.add(label);
                }
            }
        }
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            child(),
            promqlPlan,
            start,
            end,
            step,
            buckets,
            scrapeInterval,
            valueColumnName,
            valueId,
            stepId,
            timeseriesId,
            timestamp
        );
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
                && Objects.equals(timeseriesId, other.timeseriesId)
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

    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    static final Instant EPOCH_MIN = Instant.EPOCH;
    static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    /**
     * Translates this command into its ES|QL plan: every top-level branch translated, combined, and projected to the
     * declared {@link #output()}. The plan reads its labels through references ({@link UnresolvedPromqlAttribute}) that
     * {@code BindPromqlLabelReferences} binds right after.
     */
    public LogicalPlan translate(AnalyzerContext analyzer) {
        var context = new TranslationContext(this, analyzer);
        LogicalPlan plan = promqlPlan();
        // Set operators with matching modifiers translate themselves (e.g. {@code or on ()}); arithmetic/comparison
        // operators with {@code on}/{@code ignoring}/{@code group_*} use the join path below.
        if (plan instanceof VectorBinaryOperator op && (plan instanceof VectorBinarySet) == false) {
            VectorMatch match = op.match();
            if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != VectorMatch.Joining.NONE) {
                return finish(emitFinalProjection(op.translateAsLeftJoin(context).plan(), false), false);
            }
        }

        // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
        // cannot compose as a single-value sub-expression.
        List<LogicalPlan> branches = VectorBinarySet.flattenUnion(plan);
        if (branches.size() == 1) {
            IntermediateResult result = context.translateIntermediate(plan, stepId(), valueId());
            boolean constant = result.kind().constant;
            return finish(emitFinalProjection(result.plan(), constant), constant);
        }
        // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
        LogicalPlan union = VectorBinarySet.translateUnion(
            context,
            branches,
            branches.stream().map(b -> context.translateIntermediate(b, new NameId(), new NameId())).toList()
        );
        return finish(emitFinalProjection(union, false), false);
    }

    /** Keeps only steps within the query range; steps are anchored at {@code start} and offset-independent. */
    private LogicalPlan emitByStepFilter(LogicalPlan plan) {
        var source = source();
        var step = stepAttribute();
        var start = start();
        var end = end();
        var lo = new GreaterThanOrEqual(source, step, start.value() != null ? start : Literal.dateTime(source, EPOCH_MIN));
        var hi = new LessThanOrEqual(source, step, end.value() != null ? end : Literal.dateTime(source, EPOCH_MAX));
        return new Filter(source, plan, new And(source, lo, hi));
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    public static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    /** Every command drops missing samples and bounds its evaluation steps after exposing the public columns. */
    private LogicalPlan finish(LogicalPlan plan, boolean localRelation) {
        plan = emitNullsFilter(source(), plan, valueAttribute());
        return localRelation ? plan : emitByStepFilter(plan);
    }

    /**
     * Projects the command output. Value and step bind to the finished table's columns by name; every label the command
     * declares, and its {@code _timeseries}, is a reference aliased to its declared name and id. A constant table carries no
     * labels.
     */
    private LogicalPlan emitFinalProjection(LogicalPlan plan, boolean constant) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.put(attr.name(), attr);
        }
        // Under a passthrough mapping the plan carries the concrete field (`labels.job`) while the command declares
        // the label alone, so fall back to the canonical name. Do not let a BY label named like the sample/step
        // column steal those lookup keys (by (value) would otherwise bind the value column to the label field).
        for (var attr : plan.output()) {
            String finite = PromqlLabels.labelName(attr);
            if (finite.equals(valueColumnName()) || finite.equals(stepColumnName())) {
                continue;
            }
            lookupMap.putIfAbsent(finite, attr);
        }
        var projected = new ArrayList<NamedExpression>(output());
        var evals = new ArrayList<Alias>();
        List<Attribute> tsIdentity = promqlPlan().output();
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i).toAttribute();
            // the value and step lead the declared output, then the result's tsIdentity
            if (i >= 2 && constant == false) {
                var label = UnresolvedPromqlAttribute.of(promqlPlan().source(), tsIdentity.get(i - 2));
                projected.set(i, new Alias(promqlPlan().source(), attr.name(), label, attr.id()));
                continue;
            }
            var lookupAttr = lookupMap.get(attr.name());
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                if (lookupAttr.resolved()) {
                    // Remap the finished table's column onto the command's id via Eval, then project the attribute.
                    evals.add(alias);
                    projected.set(i, alias.toAttribute());
                } else {
                    // Union branches often expose an unresolved value/step until Bind resolves aggregates under them.
                    // Wrapping that in Eval makes Alias.toAttribute() an UnresolvedAttribute that shadows the real
                    // column by name once the child resolves (Unknown column [value]/result]). Keep the Alias in
                    // the Project so ResolveRefs rebinds it after the union output is refreshed.
                    projected.set(i, alias);
                }
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(source(), plan, evals);
        }
        return new Project(source(), plan, projected);
    }

    /**
     * Whether {@code _timeseries} is typed metadata for this query: the feature is on and every participating node reads
     * it. Otherwise it is the keyword column, and labels left out of it come only from the source.
     */
    public static boolean usesTimeSeriesMetadata(AnalyzerContext analyzer) {
        return EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE.isEnabled()
            && TimeSeriesMetadataAttribute.supportTimeSeriesMetadataType(analyzer.minimumVersion());
    }

    /** Verifies the PromQL constructs translation supports. */
    public void verify(Failures failures, AnalyzerContext analyzer) {
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
        } else if (p instanceof RangeSelector) {
            // Instant range-vector results require returning the raw samples rather than one value per evaluation step.
            failures.add(fail(p, "range vector results are not supported at this time [{}]", p.sourceText()));
        }
        var warnings = new ArrayList<String>();
        verifyTree(
            p,
            failures,
            warnings,
            analyzer.maxBranchCountPerMerge(),
            analyzer.maxBranchCountPerMergeLimitSource(),
            usesTimeSeriesMetadata(analyzer)
        );
        if (failures.hasFailures() == false) {
            warnings.forEach(HeaderWarning::addWarning);
        }
    }

    private void verifyGroupingLabels(LogicalPlan plan, List<Attribute> groupings, Failures failures) {
        for (Attribute grouping : groupings) {
            String name = PromqlLabels.labelName(grouping);
            if (stepColumnName().equals(name) || valueColumnName().equals(name)) {
                failures.add(fail(plan, "label [{}] collides with the built-in [{}] output column [{}]", name, name, plan.sourceText()));
            }
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

    private void verifyTree(
        LogicalPlan p,
        Failures failures,
        List<String> warnings,
        int maxBranches,
        String limitSource,
        boolean typedMetadata
    ) {
        // Validate entire plan. Each contiguous `or` chain (top-level or nested) flattens to one UnionAll; reject
        // chains that exceed the UnionAll branch limit here rather than during translation.
        Set<VectorBinarySet> topLevelUnions = collectTopLevelUnionChain(p);
        Set<VectorBinarySet> countedUnionChains = new HashSet<>();
        p.forEachDown(VectorBinarySet.class, setOp -> {
            if (setOp.op() != VectorBinarySet.SetOp.UNION || countedUnionChains.contains(setOp)) {
                return;
            }
            List<LogicalPlan> branches = VectorBinarySet.flattenUnion(setOp);
            setOp.forEachDown(VectorBinarySet.class, nested -> {
                if (nested.op() == VectorBinarySet.SetOp.UNION) {
                    countedUnionChains.add(nested);
                }
            });
            if (branches.size() > maxBranches) {
                failures.add(
                    fail(
                        setOp,
                        "PromQL set operator [or] supports up to [{}] operands, got [{}], exceeding the limit set by the {}",
                        maxBranches,
                        branches.size(),
                        limitSource
                    )
                );
            }
        });
        // Keyword _timeseries cannot apply unset (ts_unset). Reject any plan that needs labels left out of metadata.
        if (typedMetadata == false) {
            Holder<LogicalPlan> needsUnset = new Holder<>();
            p.forEachDown(lp -> {
                if (needsUnset.get() != null) {
                    return;
                }
                for (Attribute attr : lp.output()) {
                    if (attr instanceof UnresolvedPromqlAttribute ref && ref.isMetadata() && ref.unset().isEmpty() == false) {
                        needsUnset.set(lp);
                        return;
                    }
                }
            });
            if (needsUnset.get() != null) {
                failures.add(
                    fail(
                        needsUnset.get(),
                        "leaving labels out of _timeseries requires typed time-series metadata [{}]",
                        needsUnset.get().sourceText()
                    )
                );
            }
        }
        Holder<Boolean> root = new Holder<>(true);
        p.forEachDown(lp -> {
            switch (lp) {
                case Selector selector -> verifySelector(selector, failures);
                case AcrossSeriesAggregate agg -> {
                    verifyGroupingLabels(agg, agg.groupings(), failures);
                    if (agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT && usesWithoutGrouping(agg.child())) {
                        failures.add(fail(agg, "nested WITHOUT over WITHOUT is not supported at this time [{}]", agg.sourceText()));
                    }
                }
                case AcrossSeriesReduction reduction -> verifyGroupingLabels(reduction, reduction.groupings(), failures);
                case HistogramFunctionCall histogram -> {
                    LogicalPlan buckets = histogram.child();
                    // Regrouping by every label but `le` needs the labels as named columns. A reduction or a WITHOUT over
                    // opaque series hides them in time-series metadata (typed lifts follow-up).
                    if (hasConcreteLabels(buckets) == false && (usesReduction(buckets) || usesWithoutGrouping(buckets))) {
                        failures.add(
                            fail(
                                histogram,
                                "{} over topk, bottomk, limitk, limit_ratio or a WITHOUT aggregate is not supported at this time " + "[{}]",
                                histogram.functionName(),
                                histogram.sourceText()
                            )
                        );
                    } else if (readsNativeHistograms(buckets) == false && inputLabel(buckets, HistogramFunctionCall.LE_LABEL) == null) {
                        // like Prometheus: an input without buckets evaluates to nothing
                        warnings.add(histogram.functionName() + ": input vector has no le label; no buckets to evaluate");
                    }
                }
                case MetadataManipulationFunction relabel -> verifyRelabelInputs(relabel, failures);
                case PromqlFunctionCall functionCall -> {
                    if (functionCall instanceof ResultOrderingFunction && root.get() == false) {
                        failures.add(
                            fail(
                                lp,
                                "[{}] is only supported at the top-level at this time [{}]",
                                functionCall.functionName(),
                                lp.sourceText()
                            )
                        );
                    }
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
                    if (binaryOperator instanceof VectorBinarySet == false
                        && binaryOperator.match() != VectorMatch.NONE
                        && EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled() == false) {
                        failures.add(fail(lp, "PromQL vector matching is not enabled in this build [{}]", lp.sourceText()));
                        return;
                    }
                    if (binaryOperator instanceof VectorBinarySet == false
                        && binaryOperator.hasMismatchedLabelSets()
                        && EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled() == false) {
                        // Default matching between different label sets translates as a join too.
                        failures.add(
                            fail(
                                lp,
                                "binary operations between vectors with mismatched grouping keys are not yet supported [{}]",
                                lp.sourceText()
                            )
                        );
                        return;
                    }
                    if (binaryOperator instanceof VectorBinaryArithmetic || binaryOperator instanceof VectorBinaryComparison) {
                        boolean scalarOperand = PromqlPlan.returnsScalar(binaryOperator.left())
                            || PromqlPlan.returnsScalar(binaryOperator.right());
                        if (binaryOperator.match() != VectorMatch.NONE && scalarOperand) {
                            failures.add(fail(lp, "vector matching only allowed between instant vectors [{}]", lp.sourceText()));
                        } else if (needsConcreteMatchKeys(binaryOperator)) {
                            // on(...)/ignoring(...) declare the match keys as unresolved label refs; ResolvePromqlAttributes
                            // materializes them from time-series metadata/_timeseries (#157669). Default matching (including an unmatched
                            // op composed over a prior match) packs a metadata signature for opaque sides and named keys
                            // for concrete ones — same joinKey path as concrete×opaque without nesting.
                            failures.add(fail(lp, "vector matching requires operands with concrete label sets [{}]", lp.sourceText()));
                        }
                    }
                    if (binaryOperator instanceof VectorBinaryComparison && root.get() == false) {
                        failures.add(
                            fail(lp, "comparison operators are only supported at the top-level at this time [{}]", lp.sourceText())
                        );
                    }
                    if (binaryOperator instanceof VectorBinaryComparison comp) {
                        // Filter-mode comparisons finish inside translateOverSharedFrame (literal RHS) or translateAsLeftJoin
                        // (vector RHS). A non-literal scalar RHS (e.g. `v > scalar(w)`) is still unsupported for
                        // unmatched comparisons; two instant vectors join like arithmetic.
                        if (comp.match() == VectorMatch.NONE
                            && comp.right() instanceof LiteralSelector == false
                            && PromqlPlan.returnsScalar(comp.right())) {
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
                    if (binaryOperator instanceof VectorBinarySet == false
                        && (usesWithoutGrouping(binaryOperator.left()) || usesWithoutGrouping(binaryOperator.right()))) {
                        // TODO: Support WITHOUT-grouped operands in binary expressions (typed lifts).
                        failures.add(fail(lp, "binary expressions with WITHOUT are not supported at this time [{}]", lp.sourceText()));
                    }
                    if (hasSourceBackedExpression(binaryOperator.left()) && hasSourceBackedExpression(binaryOperator.right())) {
                        verifySourceBackedOperands(failures, binaryOperator, typedMetadata);
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

        verifyMetadataManipulationPlacement(p, null, failures, typedMetadata);
    }

    /**
     * Prometheus labels are strings, and {@code label_replace}/{@code label_join} have no meaning for other values: every label
     * whose value a derivation reads must be a string where the input has it.
     */
    private void verifyRelabelInputs(MetadataManipulationFunction relabel, Failures failures) {
        for (String label : relabel.valueLabels()) {
            Attribute declared = inputLabel(relabel.child(), label);
            if (declared != null && declared.dataType() != DataType.NULL && DataType.isString(declared.dataType()) == false) {
                failures.add(
                    fail(
                        relabel,
                        "{} requires string labels, [{}] is [{}]; map it as keyword [{}]",
                        relabel.functionName(),
                        label,
                        declared.dataType().typeName(),
                        relabel.sourceText()
                    )
                );
            }
        }
    }

    /**
     * The label {@code name} of {@code input}'s result as its type declares it: one the result promotes, or a dimension of the
     * source its metadata keeps. Null when the result has no such label.
     */
    private @Nullable Attribute inputLabel(LogicalPlan input, String name) {
        Attribute promoted = PromqlLabels.getLabel(
            input.output().stream().filter(attribute -> PromqlLabels.isMetadataLabel(attribute) == false).toList(),
            name
        );
        if (promoted != null) {
            return promoted;
        }
        Set<String> excluded = PromqlLabels.excluded(input);
        if (excluded == null || excluded.contains(name) || PromqlLogicalPlanBuilder.tryFoldRelation(this, child()) != null) {
            return null;
        }
        return PromqlLabels.getLabel(child(), name);
    }

    /** Whether {@code input} selects native histograms, which carry their buckets in the value rather than in {@code le}. */
    private static boolean readsNativeHistograms(LogicalPlan input) {
        return input.anyMatch(
            node -> node instanceof Selector selector && selector.series().resolved() && selector.series().dataType().isHistogram()
        );
    }

    /**
     * Enforces the supported scope for {@code label_replace}/{@code label_join}. Walking down the plan, each relabel is
     * checked against the nearest enclosing <i>identity consumer</i>.
     * <p>
     * With typed {@code _timeseries}, opaque time-series metadata is rewritten via {@code ts_set}/{@code ts_get} ({@code #157672}), so
     * bare calls, {@code without(...)}, reductions, and binary operands are admitted. Without typed metadata, only
     * placements that cannot leak a pre-replace label through keyword metadata remain:
     * <ul>
     *   <li>an enclosing {@code by(...)} aggregate (projects away every other label and any time-series metadata)</li>
     *   <li>a child that already has a concrete label set (no time-series metadata), e.g. {@code label_join(sum by (...) (...), ...)}
     *       at the top level or under {@code topk}</li>
     * </ul>
     *
     * @param consumer the nearest enclosing identity consumer for a relabel at this position, or {@code null} at the root
     * @param typed whether this query rewrites time-series metadata with typed timeseries functions
     */
    private static void verifyMetadataManipulationPlacement(LogicalPlan node, LogicalPlan consumer, Failures failures, boolean typed) {
        if (node instanceof MetadataManipulationFunction relabel) {
            checkRelabelConsumer(relabel, consumer, failures, typed);
            // A relabel passes identity through unchanged, so a nested relabel is consumed by the same enclosing consumer.
            verifyMetadataManipulationPlacement(relabel.child(), consumer, failures, typed);
            return;
        }
        // A relabel appearing beneath an identity-consuming node (see PromqlPlan#isIdentityTransparent) is consumed by it;
        // identity-transparent nodes (and any non-PromqlPlan node such as a relation) keep the parent's consumer.
        LogicalPlan childConsumer = node instanceof PromqlPlan promqlPlan && promqlPlan.isIdentityTransparent() == false ? node : consumer;
        for (LogicalPlan child : node.children()) {
            verifyMetadataManipulationPlacement(child, childConsumer, failures, typed);
        }
    }

    private static void checkRelabelConsumer(MetadataManipulationFunction relabel, LogicalPlan consumer, Failures failures, boolean typed) {
        // Typed time-series metadata is rewritten in place; concrete children have no time-series metadata to leak the pre-replace value.
        if (/* typed lifts follow-up */ hasConcreteLabels(relabel.child())) {
            return;
        }
        String name = relabel.definition().name();
        if (consumer instanceof AcrossSeriesAggregate agg) {
            if (agg.grouping() == AcrossSeriesAggregate.Grouping.BY) {
                return;
            }
            String reason = agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT
                ? "with a `without(...)` grouping"
                : "without a `by(...)` grouping";
            failures.add(
                fail(
                    relabel,
                    "[{}] is only supported inside a `by(...)` aggregation, but was used {} [{}]",
                    name,
                    reason,
                    relabel.sourceText()
                )
            );
            return;
        }
        String context = switch (consumer) {
            case null -> "as a top-level (non-aggregated) expression";
            case AcrossSeriesReduction reduction -> "under [" + reduction.definition().name() + "]";
            case VectorBinaryOperator binaryOperator -> "as an operand of a binary operator";
            default -> "in an unsupported position";
        };
        failures.add(
            fail(
                relabel,
                "[{}] is only supported inside a `by(...)` aggregation, but was used {} [{}]",
                name,
                context,
                relabel.sourceText()
            )
        );
    }

    /**
     * Set operators ({@code and}/{@code or}/{@code unless}) are only partially supported.
     * <ul>
     *   <li>Scalar operands are illegal for set operators in PromQL itself; we mirror Prometheus' wording.</li>
     *   <li>{@code and} (INTERSECT) is supported: it keeps left series that match on the right (default or
     *       {@code on}/{@code ignoring}).</li>
     *   <li>{@code unless} (SUBTRACT) is supported for default matching between operands with the same label set: it
     *       keeps left series with no match on the right. {@code unless} with {@code on}/{@code ignoring}, or between
     *       label sets that never match, still needs an anti-join ({@code #158179}).</li>
     *   <li>{@code or} is supported as a top-level or nested chain without {@code on}/{@code ignoring}
     *       ({@code #158182}), and as the empty-guard idiom {@code <labelless> or on () <labelless>}
     *       (typically {@code sum(...) or on () vector(1)}).</li>
     * </ul>
     */
    private static void verifySetOperator(Failures failures, VectorBinarySet setOp, boolean isTopLevelUnion) {
        if (PromqlPlan.returnsScalar(setOp.left()) || PromqlPlan.returnsScalar(setOp.right())) {
            failures.add(fail(setOp, "set operator \"{}\" not allowed in binary scalar expression", setOp.op().keyword()));
            return;
        }
        if (setOp.op() != VectorBinarySet.SetOp.UNION) {
            // TODO: Support and/unless set operators (typed lifts).
            // https://github.com/elastic/elasticsearch/issues/158179
            failures.add(fail(setOp, "set operator [{}] is not supported at this time [{}]", setOp.op().keyword(), setOp.sourceText()));
            return;
        }
        if (setOp.match() != VectorMatch.NONE) {
            // TODO: Support or with on/ignoring modifiers.
            // https://github.com/elastic/elasticsearch/issues/158181
            failures.add(fail(setOp, "set operator [or] with on/ignoring is not supported at this time [{}]", setOp.sourceText()));
        } else if (isTopLevelUnion == false) {
            // TODO: Support or below the top level.
            // https://github.com/elastic/elasticsearch/issues/158182
            failures.add(fail(setOp, "set operator [or] is only supported at the top-level at this time [{}]", setOp.sourceText()));
        }
    }

    private static boolean hasSourceBackedExpression(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof Selector && (p instanceof LiteralSelector) == false);
    }

    /**
     * Whether vector matching still needs every operand to name its labels up front. {@code on(...)} / {@code ignoring(...)}
     * declare the match keys; unresolved refs materialize them from an opaque {@code _timeseries}. Default matching —
     * including an unmatched op composed over a prior match — packs a metadata signature for any opaque side (and named
     * keys from a concrete sibling), so opaque×concrete does not need labels named up front. Explicit {@code group_*}
     * without {@code on}/{@code ignoring} over a single opaque side still does.
     */
    private static boolean needsConcreteMatchKeys(VectorBinaryOperator op) {
        if (hasConcreteLabels(op.left()) && hasConcreteLabels(op.right())) {
            return false;
        }
        VectorMatch.Filter filter = op.match().filter();
        if (filter == VectorMatch.Filter.ON || filter == VectorMatch.Filter.IGNORING) {
            return false;
        }
        // Default matching: joinKey packs opaque time-series metadata (and EMPTY_SIGNATURE on a labelless/concrete sibling).
        if (op.match() == VectorMatch.NONE) {
            return false;
        }
        // Both opaque (e.g. two {@code group_right} results): translateAsLeftJoin packs time-series metadata like cross-metric default
        // matching.
        if (PromqlLabels.metadataLabel(op.left().output()) != null && PromqlLabels.metadataLabel(op.right().output()) != null) {
            return false;
        }
        return true;
    }

    /**
     * Verifies the operands of a binary operator whose both sides read source data.
     * <p>
     * Nested across-series, aggregated×raw, different offsets, and {@code scalar()}×labeled join or
     * step-broadcast ({@link EsqlCapabilities.Cap#PROMQL_BINARY_INDEPENDENT_JOIN}). Opaque
     * {@code topk}/{@code bottomk}/{@code limitk}/{@code limit_ratio} join under typed {@code _timeseries};
     * keyword metadata keeps the pre-typed rejection (cannot regroup those results). Concrete named-label
     * reductions always join.
     */
    private static void verifySourceBackedOperands(Failures failures, VectorBinaryOperator binaryOperator, boolean typedMetadata) {
        // Typed lifts that admit these shapes land in a follow-up; keep pre-typed rejections here.
        LogicalPlan left = binaryOperator.left();
        LogicalPlan right = binaryOperator.right();
        String text = binaryOperator.sourceText();
        boolean fused = binaryOperator instanceof VectorBinarySet == false && binaryOperator.match() == VectorMatch.NONE;
        Predicate<LogicalPlan> isAggregation = fused ? PromqlCommand::isAcrossSeries : AcrossSeriesAggregate.class::isInstance;
        if (fused && (scalarOfVector(left) && hasLabels(right) || scalarOfVector(right) && hasLabels(left))) {
            failures.add(
                fail(
                    binaryOperator,
                    "binary operations between scalar() of a vector and a vector with labels are not supported at this time [{}]",
                    text
                )
            );
        } else if (usesNestedAggregation(left, isAggregation) || usesNestedAggregation(right, isAggregation)) {
            failures.add(fail(binaryOperator, "binary expressions with nested aggregations are not supported at this time [{}]", text));
        } else if (fused && (usesReduction(left) || usesReduction(right))) {
            failures.add(
                fail(
                    binaryOperator,
                    "binary operations over topk, bottomk, limitk or limit_ratio are not supported at this time [{}]",
                    text
                )
            );
        } else if (fused && aggregatesAcrossSeries(left) != aggregatesAcrossSeries(right)) {
            failures.add(
                fail(binaryOperator, "binary operations between an aggregated and a raw vector are not supported at this time [{}]", text)
            );
        }
    }

    private static boolean usesReduction(LogicalPlan plan) {
        return plan.anyMatch(AcrossSeriesReduction.class::isInstance);
    }

    /**
     * A {@code topk}/{@code bottomk}/… whose output names at least one label (no time-series metadata). Those join on the named labels;
     * an unlabeled or opaque reduction cannot under keyword metadata (bind regrouping).
     */
    private static boolean joinableConcreteReduction(LogicalPlan plan) {
        return usesReduction(plan) && hasConcreteLabels(plan) && namedLabels(plan).isEmpty() == false;
    }

    private static List<String> namedLabels(LogicalPlan plan) {
        return PromqlLabels.labelNames(plan.output().stream().filter(a -> PromqlLabels.isMetadataLabel(a) == false).toList());
    }

    private static boolean hasConcreteLabels(LogicalPlan plan) {
        return PromqlLabels.metadataLabel(plan.output()) == null;
    }

    /**
     * The distinct signed offsets of the source-backed selectors under {@code plan}. Literal selectors contribute none.
     * An empty set means the branch has no offset-bearing selector (or only literals).
     */
    public static Set<Duration> distinctOffsets(LogicalPlan plan) {
        Set<Duration> offsets = new HashSet<>();
        for (Selector selector : plan.collect(Selector.class)) {
            if (selector instanceof LiteralSelector == false && selector.evaluation() != null) {
                offsets.add(selector.evaluation().offsetDuration());
            }
        }
        return offsets;
    }

    /**
     * The signed offset for source-backed selectors in {@code branch}, as a constant time shift to add to
     * {@code @timestamp}. When a joined operand is translated on its own, every selector under it shares that branch's
     * shift; a fold frame only forms when both sides agree ({@link VectorBinaryOperator#hasHeterogeneousOffsets()}).
     * Returns {@link Duration#ZERO} when there is no offset.
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

    private static boolean scalarOfVector(LogicalPlan plan) {
        return plan instanceof ScalarConversionFunction && hasSourceBackedExpression(plan);
    }

    private static boolean hasLabels(LogicalPlan plan) {
        return namedLabels(plan).isEmpty() == false;
    }

    private static boolean usesNestedAggregation(LogicalPlan plan, Predicate<LogicalPlan> isAggregation) {
        return plan.children().stream().anyMatch(child -> child.anyMatch(isAggregation));
    }

    private static boolean aggregatesAcrossSeries(LogicalPlan plan) {
        return plan.anyMatch(PromqlCommand::isAcrossSeries);
    }

    private static boolean isAcrossSeries(LogicalPlan plan) {
        return plan instanceof AcrossSeriesAggregate || plan instanceof AcrossSeriesReduction;
    }

    private static boolean usesWithoutGrouping(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof AcrossSeriesAggregate agg && agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT);
    }

}
