/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.Build;
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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
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
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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

    /** The id of the {@code _timeseries} column the command exposes for its result's label container. */
    public NameId timeseriesId() {
        return timeseriesId;
    }

    /**
     * Whether {@code _timeseries} is a typed label container for this query: the feature is on and every participating node reads
     * it. Otherwise the translated plan is lowered to the keyword column.
     */
    public static boolean usesLabelContainer(AnalyzerContext analyzer) {
        return EsqlCapabilities.Cap.ENABLE_TSMD_DATA_TYPE.isEnabled()
            && DataType.TIME_SERIES_METADATA_DATA_TYPE.supportedVersion()
                .supportedOn(analyzer.minimumVersion(), Build.current().isSnapshot());
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
                // the result's label container is exposed as the one _timeseries column, under the command's own id
                if (PromqlLabels.isTagsLabel(label)) {
                    output.add(
                        new ReferenceAttribute(
                            source(),
                            null,
                            MetadataAttribute.TIMESERIES,
                            // a typed label container is internal: the command exposes its keyword form
                            DataType.KEYWORD,
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
     * declared {@link #output()}: each declared label is read off the result's label container, which keeps the rest.
     * {@code PushDownPromqlLabels} splits the containers into columns right after.
     */
    public LogicalPlan translate(AnalyzerContext analyzer) {
        var context = new TranslationContext(this, analyzer);
        LogicalPlan plan = promqlPlan();
        // Set operators with matching modifiers translate themselves; arithmetic/comparison operators with
        // {@code on}/{@code ignoring}/{@code group_*} use the join path below.
        if (plan instanceof VectorBinaryOperator op && (plan instanceof VectorBinarySet) == false) {
            VectorMatch match = op.match();
            if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != VectorMatch.Joining.NONE) {
                IntermediateResult joined = op.translateAsLeftJoin(context);
                return finish(emitFinalProjection(joined.plan(), joined.labels(), false, context), false);
            }
        }

        // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
        // cannot compose as a single-value sub-expression.
        List<LogicalPlan> branches = VectorBinarySet.flattenUnion(plan);
        if (branches.size() == 1) {
            IntermediateResult result = context.translateIntermediate(plan, stepId(), valueId());
            boolean constant = result.kind().constant;
            return finish(emitFinalProjection(result.plan(), result.labels(), constant, context), constant);
        }
        // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
        LogicalPlan union = VectorBinarySet.translateUnion(
            context,
            branches.stream().map(b -> context.translateIntermediate(b, new NameId(), new NameId())).toList()
        );
        Attribute labels = union.output().stream().filter(a -> a.name().equals(MetadataAttribute.TIMESERIES)).findFirst().orElseThrow();
        return finish(emitFinalProjection(union, labels, false, context), false);
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
     * Projects the command output. Value and step bind to the finished table's columns by name. Every label the command
     * declares reads the result's label container {@code labels}, under its declared name, id and type, and the declared
     * {@code _timeseries} is the container without them. A constant table carries no labels. Typed {@code _timeseries}
     * stays internal: it binds under an id of its own and the command exposes its keyword form.
     */
    private LogicalPlan emitFinalProjection(LogicalPlan plan, @Nullable Expression labels, boolean constant, TranslationContext context) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.putIfAbsent(attr.name(), attr);
        }
        var projected = new ArrayList<NamedExpression>(output());
        var evals = new ArrayList<Alias>();
        List<Attribute> declared = promqlPlan().output();
        // A column exposed only to order the result is read from the container but stays in _timeseries too
        Set<NameId> orderingOnly = new HashSet<>();
        promqlPlan().forEachDown(LogicalPlan.class, p -> {
            if (p instanceof ResultOrderingFunction ordering) {
                ordering.orderingOnlyColumns().forEach(column -> orderingOnly.add(column.id()));
            }
        });
        List<String> named = PromqlLabels.labelNames(
            declared.stream().filter(a -> PromqlLabels.isTagsLabel(a) == false && orderingOnly.contains(a.id()) == false).toList()
        );
        Source source = promqlPlan().source();
        Alias rendered = null;
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i).toAttribute();
            // the value and step lead the declared output, then the result's labels
            if (i >= 2 && constant == false) {
                Attribute label = declared.get(i - 2);
                if (PromqlLabels.isTagsLabel(label)) {
                    Expression rest = labels == null
                        ? VectorBinarySet.emptyLabels(source)
                        : new TimeSeriesMetadataFilter(labels.source(), labels, PromqlLabels.labelLiterals(labels.source(), named), true);
                    Alias labelContainer = new Alias(source, attr.name(), rest);
                    evals.add(labelContainer);
                    rendered = new Alias(
                        source,
                        attr.name(),
                        new ToString(source, labelContainer.toAttribute(), context.configuration()),
                        attr.id()
                    );
                    projected.set(i, labelContainer.toAttribute());
                    continue;
                }
                Expression value = labels == null
                    ? new Literal(source, null, attr.dataType())
                    : asType(
                        new TimeSeriesMetadataPath(
                            labels.source(),
                            labels,
                            Literal.keyword(labels.source(), PromqlLabels.labelName(label))
                        ),
                        attr.dataType(),
                        context
                    );
                Alias alias = new Alias(source, attr.name(), value, attr.id());
                evals.add(alias);
                projected.set(i, alias.toAttribute());
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
        plan = new Project(source(), plan, projected);
        if (rendered == null) {
            return plan;
        }
        plan = new Eval(source(), plan, List.of(rendered));
        // in the declared order: the Eval appends the rendered column
        var exposed = new ArrayList<NamedExpression>(projected.size());
        for (NamedExpression projection : projected) {
            exposed.add(projection.name().equals(rendered.name()) ? rendered.toAttribute() : projection.toAttribute());
        }
        return new Project(source(), plan, exposed);
    }

    /** A label's text as the declared label's type: a label read from the container is text, a dimension may not be. */
    private static Expression asType(Expression text, DataType type, TranslationContext context) {
        if (type == DataType.KEYWORD || type == DataType.NULL) {
            return text;
        }
        return EsqlDataTypeConverter.converterFunctionFactory(type).apply(text.source(), text, context.configuration());
    }

    /** Verifies the PromQL constructs translation supports. */
    public void verify(Failures failures, AnalyzerContext analyzer) {
        LogicalPlan p = promqlPlan();
        if (MetadataAttribute.isTimeSeriesAttributeName(valueColumnName) && PromqlLabels.getTagsLabel(p.output()) != null) {
            // The result's label container is exposed under this name; a value column under it would shadow it.
            failures.add(fail(this, "result column [{}] collides with the label container column", valueColumnName));
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
        var warnings = new ArrayList<String>();
        verifyTree(
            p,
            failures,
            warnings,
            analyzer.maxBranchCountPerMerge(),
            analyzer.maxBranchCountPerMergeLimitSource(),
            usesLabelContainer(analyzer)
        );
        if (failures.hasFailures() == false) {
            warnings.forEach(HeaderWarning::addWarning);
        }
    }

    private void verifyGroupingLabels(LogicalPlan plan, List<Attribute> groupings, Failures failures) {
        for (Attribute grouping : groupings) {
            String name = PromqlLabels.labelName(grouping.name());
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
        boolean typedLabelContainer
    ) {
        // Validate entire plan. Each contiguous `or` chain (top-level or nested) flattens to one UnionAll; reject
        // chains that exceed the UnionAll branch limit here rather than during translation.
        Set<VectorBinarySet> topLevelUnions = topLevelUnionChain(p);
        if (typedLabelContainer == false && p instanceof VectorBinarySet union && topLevelUnions.contains(union)) {
            verifyKeywordUnion(failures, union);
        }
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
        Holder<Boolean> root = new Holder<>(true);
        p.forEachDown(lp -> {
            switch (lp) {
                case Selector selector -> verifySelector(selector, failures);
                case AcrossSeriesAggregate agg -> {
                    verifyGroupingLabels(agg, agg.groupings(), failures);
                }
                case AcrossSeriesReduction reduction -> verifyGroupingLabels(reduction, reduction.groupings(), failures);
                case HistogramFunctionCall histogram -> {
                    LogicalPlan buckets = histogram.child();
                    // Regrouping by every label but `le` needs the labels as named columns. A reduction or a WITHOUT over
                    // opaque series hides them in the label container instead.
                    if (hasConcreteLabels(buckets) == false && (usesReduction(buckets) || usesWithoutGrouping(buckets))) {
                        failures.add(
                            fail(
                                histogram,
                                "{} over topk, bottomk, limitk, limit_ratio or a WITHOUT aggregate is not supported at this time "
                                    + "unless the input is first aggregated with BY [{}]",
                                histogram.functionName(),
                                histogram.sourceText()
                            )
                        );
                    } else if (readsNativeHistograms(buckets) == false
                        && selectsMappedMetric(buckets)
                        && inputLabel(buckets, HistogramFunctionCall.LE_LABEL) == null) {
                            // like Prometheus: an input without buckets evaluates to nothing; one without series has nothing to say
                            warnings.add(histogram.functionName() + ": input vector has no le label; no buckets to evaluate");
                        }
                }
                case MetadataManipulationFunction relabel -> verifyRelabelInputs(relabel, failures);
                case PromqlFunctionCall functionCall -> {
                    // ok: sort and sort_desc below the top level pass their input through; AddPromqlResultOrder orders the
                    // result where the order they give survives.
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
                            // materializes them from the label container/_timeseries (#157669). Default matching (including an unmatched
                            // op composed over a prior match) packs a label container signature for opaque sides and named keys
                            // for concrete ones - same joinKey path as concrete x opaque without nesting.
                            failures.add(fail(lp, "vector matching requires operands with concrete label sets [{}]", lp.sourceText()));
                        }
                    }
                    if (binaryOperator instanceof VectorBinaryComparison comparison
                        && comparison.match() == VectorMatch.NONE
                        && root.get() == false) {
                        failures.add(
                            fail(lp, "comparison operators are only supported at the top-level at this time [{}]", lp.sourceText())
                        );
                    }
                    if (binaryOperator instanceof VectorBinaryComparison comp) {
                        // Filter-mode comparisons finish inside translateOverSharedFrame (literal operand), as a step
                        // broadcast (a scalar computed from the source, on either side), or in translateAsLeftJoin (two
                        // vectors), like arithmetic.
                        if (comp.boolMode() == false && PromqlPlan.returnsScalar(comp.left()) && PromqlPlan.returnsScalar(comp.right())) {
                            failures.add(fail(comp, "Comparisons [{}] between scalars must use the BOOL modifier", comp.op()));
                        }
                        if (comp.filterMode()
                            && PromqlPlan.returnsScalar(comp.left())
                            && PromqlPlan.returnsScalar(comp.right()) == false
                            && comp.left() instanceof LiteralSelector == false) {
                            // A filter keeps the vector's samples where a scalar function on the left compares true; translation
                            // returns rows Prometheus does not until the typed label container translation lands.
                            failures.add(
                                fail(
                                    lp,
                                    "comparison filters with a non-literal scalar on the left are not supported at this time [{}]",
                                    lp.sourceText()
                                )
                            );
                        }
                    }
                    if ((binaryOperator.match().filter() == VectorMatch.Filter.ON
                        && binaryOperator.match().filterLabels().contains(LabelMatcher.NAME))
                        || binaryOperator.match().groupingLabels().contains(LabelMatcher.NAME)) {
                        // A series whose metric is a field stores no __name__ and reads it as absent, so on(__name__) would
                        // match the series of different metrics, which Prometheus keeps apart. Which series store it is
                        // known only from the data, so it is rejected for all.
                        // TODO: Match on __name__ once the metric name is a dimension value.
                        // https://github.com/elastic/elasticsearch/issues/146989
                        failures.add(fail(lp, "matching on __name__ is not supported at this time [{}]", lp.sourceText()));
                    }
                    if (binaryOperator instanceof VectorBinarySet setOp) {
                        verifySetOperator(failures, setOp, typedLabelContainer, topLevelUnions.contains(setOp));
                    }
                    if (hasSourceBackedExpression(binaryOperator.left()) && hasSourceBackedExpression(binaryOperator.right())) {
                        verifySourceBackedOperands(failures, binaryOperator, typedLabelContainer);
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

        verifyMetadataManipulationPlacement(p, null, failures, typedLabelContainer);
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
     * source its label container keeps. Null when the result has no such label.
     */
    private @Nullable Attribute inputLabel(LogicalPlan input, String name) {
        Attribute promoted = PromqlLabels.getLabel(
            input.output().stream().filter(attribute -> PromqlLabels.isTagsLabel(attribute) == false).toList(),
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

    /** Whether {@code input} selects a metric the source maps: an unmapped one has no series, with or without {@code le}. */
    private static boolean selectsMappedMetric(LogicalPlan input) {
        return input.anyMatch(
            node -> node instanceof Selector selector && selector.series().resolved() && selector.series().dataType() != DataType.NULL
        );
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
     * With typed {@code _timeseries}, an opaque label container is rewritten via {@code ts_set}/{@code ts_path} ({@code #157672}), so
     * bare calls, {@code without(...)}, reductions, and binary operands are admitted. Without a typed label container, only
     * placements that cannot leak a pre-replace label through a keyword label container remain:
     * <ul>
     *   <li>an enclosing {@code by(...)} aggregate (projects away every other label and any label container)</li>
     *   <li>a child that already has a concrete label set (no label container), e.g. {@code label_join(sum by (...) (...), ...)}
     *       at the top level or under {@code topk}</li>
     * </ul>
     *
     * @param consumer the nearest enclosing identity consumer for a relabel at this position, or {@code null} at the root
     * @param typed whether this query rewrites the label container with typed timeseries functions
     */
    private void verifyMetadataManipulationPlacement(LogicalPlan node, LogicalPlan consumer, Failures failures, boolean typed) {
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

    private void checkRelabelConsumer(MetadataManipulationFunction relabel, LogicalPlan consumer, Failures failures, boolean typed) {
        if (typed) {
            checkTypedRelabelOutput(relabel, consumer, failures);
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
        if (consumer instanceof VectorBinaryOperator) {
            failures.add(
                fail(
                    relabel,
                    "[{}] is only supported inside a `by(...)` aggregation, but was used as an operand of a binary operator [{}]",
                    name,
                    relabel.sourceText()
                )
            );
            return;
        }
        // No consumer (top-level) or a topk-style reduction: the relabel is supported over an identity that an enclosed
        // `by(...)` aggregation has already reduced to concrete columns. Overwriting one fails the query where two series end
        // up with one label set, as in Prometheus.
        String position = consumer instanceof AcrossSeriesReduction reduction
            ? "under [" + reduction.definition().name() + "]"
            : "as a top-level expression";
        LogicalPlan identitySource = identitySource(relabel.child());
        if (identitySource instanceof AcrossSeriesAggregate agg && agg.grouping() == AcrossSeriesAggregate.Grouping.BY) {
            // Compare by label name, not attribute name: a destination written as `labels.pod` names the same label as the
            // aggregation's `pod`, which is how the analyzer shadows it and how translation resolves it.
            String destination = PromqlLabels.labelName(relabel.destination().name());
            // On this path the derived label is not consumed by a grouping but becomes an output column of its own, so
            // it must be checked against the built-in output columns just like a `by(...)` label.
            checkBuiltInColumnCollision(relabel, destination, failures);
            return;
        }
        failures.add(
            fail(
                relabel,
                "[{}] is only supported inside a `by(...)` aggregation or over the result of one, but was used {} over {} [{}]",
                name,
                position,
                describeIdentitySource(identitySource),
                relabel.sourceText()
            )
        );
    }

    /**
     * A typed label container is rewritten in place, so a relabel may sit anywhere. Where its derived label is an
     * output column of its own - at the top level or under a topk-style reduction - it still must not collide with a
     * built-in output column.
     */
    private void checkTypedRelabelOutput(MetadataManipulationFunction relabel, LogicalPlan consumer, Failures failures) {
        // At the top level or under a topk-style reduction the derived label is an output column of its own, whatever the
        // label container representation.
        if (consumer == null || consumer instanceof AcrossSeriesReduction) {
            // Compare by label name, not attribute name: a destination written as `labels.pod` names the same label as the
            // aggregation's `pod`, which is how the analyzer shadows it and how translation resolves it.
            checkBuiltInColumnCollision(relabel, PromqlLabels.labelName(relabel.destination().name()), failures);
        }
    }

    /**
     * Rejects a label that would surface as an output column under the same name as one of the command's built-in output
     * columns - the step column or the value column - since the two would end up as duplicate output attributes and fail
     * in the optimizer rather than with a client error. {@code labelName} is the PromQL label name (see
     * {@link PromqlLabels#labelName}), so a passthrough-prefixed {@code labels.step} is caught as well.
     */
    private void checkBuiltInColumnCollision(LogicalPlan node, String labelName, Failures failures) {
        for (String builtIn : List.of(stepColumnName(), valueColumnName())) {
            if (builtIn.equals(labelName)) {
                failures.add(
                    fail(node, "label [{}] collides with the built-in [{}] output column [{}]", labelName, builtIn, node.sourceText())
                );
            }
        }
    }

    /**
     * The node that produces {@code plan}'s series identity: the nearest non-identity-transparent node (see
     * {@link PromqlPlan#isIdentityTransparent}) below, or {@code null} when the walk reaches raw series without passing one.
     */
    private static LogicalPlan identitySource(LogicalPlan plan) {
        LogicalPlan node = plan;
        while (node instanceof PromqlPlan promqlPlan) {
            if (promqlPlan.isIdentityTransparent() == false) {
                return node;
            }
            if (node.children().size() != 1) {
                return null;
            }
            node = node.children().get(0);
        }
        return null;
    }

    /**
     * A human-readable name for an {@link #identitySource} in a verification failure. Only called for the sources that
     * {@link #checkRelabelConsumer} rejects, so a {@code by(...)} aggregation never reaches this method.
     */
    private static String describeIdentitySource(LogicalPlan identitySource) {
        return switch (identitySource) {
            case null -> "raw (non-aggregated) series";
            case AcrossSeriesAggregate agg -> switch (agg.grouping()) {
                case WITHOUT -> "a `without(...)` aggregation";
                case NONE -> "an aggregation without a `by(...)` grouping";
                case BY -> throw new AssertionError("a relabel over a `by(...)` aggregation is accepted before reaching here");
            };
            case AcrossSeriesReduction reduction -> "[" + reduction.definition().name() + "]";
            case VectorBinaryOperator binaryOperator -> "a binary operator";
            // These are the only nodes for which PromqlPlan#isIdentityTransparent returns false, so identitySource never
            // yields anything else.
            default -> throw new AssertionError("unexpected identity source [" + identitySource.nodeName() + "]");
        };
    }

    /**
     * Set operators ({@code and}/{@code or}/{@code unless}).
     * <ul>
     *   <li>Scalar operands are illegal for set operators in PromQL itself; we mirror Prometheus' wording.</li>
     *   <li>{@code and} (INTERSECT) keeps left series that match on the right (default or {@code on}/{@code ignoring}).</li>
     *   <li>{@code unless} (SUBTRACT) keeps left series with no match on the right, by default matching or by
     *       {@code on}/{@code ignoring}. Operands whose label sets never match anti-join too, so the left operand comes
     *       back whole, as in Prometheus.</li>
     *   <li>{@code or} (UNION) unions its branches, keeping one series per step and label set without {@code __name__};
     *       with {@code on}/{@code ignoring} as {@code a or (b unless<M> a)}.</li>
     * </ul>
     * Each of them matches series on their labels without {@code __name__}, which a keyword label container cannot leave out: there
     * only a top-level chain of {@code or} without {@code on}/{@code ignoring} runs, as it did before a typed label container.
     */
    private static void verifySetOperator(Failures failures, VectorBinarySet setOp, boolean typedLabelContainer, boolean topLevelUnion) {
        if (PromqlPlan.returnsScalar(setOp.left()) || PromqlPlan.returnsScalar(setOp.right())) {
            failures.add(fail(setOp, "set operator \"{}\" not allowed in binary scalar expression", setOp.op().keyword()));
            return;
        }
        if (typedLabelContainer) {
            return;
        }
        if (setOp.op() != VectorBinarySet.SetOp.UNION) {
            failures.add(fail(setOp, "set operator [{}] requires a typed label container [{}]", setOp.op().keyword(), setOp.sourceText()));
        } else if (setOp.match() != VectorMatch.NONE) {
            failures.add(fail(setOp, "set operator [or] with on/ignoring requires a typed label container [{}]", setOp.sourceText()));
        } else if (topLevelUnion == false) {
            failures.add(fail(setOp, "set operator [or] below the top level requires a typed label container [{}]", setOp.sourceText()));
        }
    }

    /**
     * A top-level {@code or} on a keyword label container deduplicates series on their labels as the operands read them, which keep
     * {@code __name__} where the series store it and an operand does not drop it. Prometheus compares label sets without
     * {@code __name__}, so the two agree only when no pair of operands can hold series that differ in {@code __name__}
     * alone:
     * <ul>
     *   <li>operands that name their labels compare them as columns, a label one lacks as absent, as Prometheus does;</li>
     *   <li>an operand without labels, such as {@code vector(1)}, beside opaque ones that keep no stored {@code __name__};</li>
     *   <li>opaque operands that leave different labels out, or keep the name of the same metric, or all drop it.</li>
     * </ul>
     */
    private void verifyKeywordUnion(Failures failures, VectorBinarySet union) {
        List<LogicalPlan> branches = VectorBinarySet.flattenUnion(union);
        if (branches.stream().anyMatch(PromqlPlan::returnsScalar)) {
            return; // a scalar operand is illegal whatever the label container, see verifySetOperator
        }
        boolean storesName = storesMetricName();
        List<Set<String>> opaque = new ArrayList<>();
        List<LogicalPlan> opaqueBranches = new ArrayList<>();
        boolean labelledConcrete = false;
        boolean labellessConcrete = false;
        for (LogicalPlan branch : branches) {
            Set<String> excluded = PromqlLabels.excluded(branch);
            if (excluded != null) {
                opaque.add(excluded);
                opaqueBranches.add(branch);
            } else if (branch.output().stream().anyMatch(a -> PromqlLabels.isTagsLabel(a) == false)) {
                labelledConcrete = true;
            } else {
                labellessConcrete = true;
            }
        }
        if (opaque.isEmpty()) {
            return;
        }
        boolean keepsName = storesName && opaque.stream().anyMatch(excluded -> excluded.contains(LabelMatcher.NAME) == false);
        boolean agree = labelledConcrete == false && (labellessConcrete == false || keepsName == false);
        for (int i = 0; agree && i < opaque.size(); i++) {
            for (int j = i + 1; agree && j < opaque.size(); j++) {
                agree = keywordUnionAgrees(storesName, opaque.get(i), opaqueBranches.get(i), opaque.get(j), opaqueBranches.get(j));
            }
        }
        if (agree == false) {
            failures.add(fail(union, "set operator [or] between these operands requires a typed label container [{}]", union.sourceText()));
        }
    }

    private static boolean keywordUnionAgrees(
        boolean storesName,
        Set<String> left,
        LogicalPlan leftBranch,
        Set<String> right,
        LogicalPlan rightBranch
    ) {
        boolean leftKeeps = storesName && left.contains(LabelMatcher.NAME) == false;
        boolean rightKeeps = storesName && right.contains(LabelMatcher.NAME) == false;
        if (leftKeeps == false && rightKeeps == false) {
            return true;
        }
        Set<String> leftOthers = new HashSet<>(left);
        leftOthers.remove(LabelMatcher.NAME);
        Set<String> rightOthers = new HashSet<>(right);
        rightOthers.remove(LabelMatcher.NAME);
        if (leftOthers.equals(rightOthers) == false) {
            return true; // one leaves out a label the other keeps: their series never share a label set
        }
        if (leftKeeps != rightKeeps) {
            return false;
        }
        Set<String> metrics = new HashSet<>(selectedMetrics(leftBranch));
        metrics.addAll(selectedMetrics(rightBranch));
        return metrics.size() == 1;
    }

    /** The metrics the selectors of {@code plan} read. */
    private static Set<String> selectedMetrics(LogicalPlan plan) {
        Set<String> metrics = new HashSet<>();
        plan.forEachDown(Selector.class, selector -> {
            if (selector instanceof LiteralSelector == false) {
                metrics.add(selector.series() instanceof NamedExpression named ? named.name() : String.valueOf(selector.series()));
            }
        });
        return metrics;
    }

    /**
     * Whether some series may store the metric name as a {@code __name__} label, as Prometheus remote write does: a queried
     * index maps it.
     */
    private boolean storesMetricName() {
        return child().output().stream().anyMatch(field -> LabelMatcher.NAME.equals(PromqlLabels.labelName(field)));
    }

    /** The {@code or} operators of the chain at the root of {@code p}, which translate as one top-level union. */
    private static Set<VectorBinarySet> topLevelUnionChain(LogicalPlan p) {
        Set<VectorBinarySet> chain = new HashSet<>();
        collectUnionChain(p, chain);
        return chain;
    }

    private static void collectUnionChain(LogicalPlan p, Set<VectorBinarySet> chain) {
        if (p instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
            chain.add(setOp);
            collectUnionChain(setOp.left(), chain);
            collectUnionChain(setOp.right(), chain);
        }
    }

    private static boolean hasSourceBackedExpression(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof Selector && (p instanceof LiteralSelector) == false);
    }

    /**
     * Whether vector matching still needs every operand to name its labels up front. {@code on(...)} / {@code ignoring(...)}
     * declare the match keys; unresolved refs materialize them from an opaque {@code _timeseries}. Default matching -
     * including an unmatched op composed over a prior match - packs a label container signature for any opaque side (and named
     * keys from a concrete sibling), so opaque x concrete does not need labels named up front. Explicit {@code group_*}
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
        // Default matching: joinKey packs an opaque label container (and EMPTY_SIGNATURE on a labelless/concrete sibling).
        if (op.match() == VectorMatch.NONE) {
            return false;
        }
        // Both opaque (e.g. two {@code group_right} results): translateAsLeftJoin packs the label container like cross-metric default
        // matching.
        if (PromqlLabels.getTagsLabel(op.left().output()) != null && PromqlLabels.getTagsLabel(op.right().output()) != null) {
            return false;
        }
        return true;
    }

    /**
     * Verifies the operands of a binary operator whose both sides read source data.
     * <p>
     * Nested across-series, aggregated x raw, different offsets, and {@code scalar()} x labeled join or
     * step-broadcast ({@link EsqlCapabilities.Cap#PROMQL_BINARY_INDEPENDENT_JOIN}). Opaque
     * {@code topk}/{@code bottomk}/{@code limitk}/{@code limit_ratio} join under typed {@code _timeseries};
     * a keyword label container keeps the pre-typed rejection (cannot regroup those results). Concrete named-label
     * reductions always join.
     */
    private static void verifySourceBackedOperands(Failures failures, VectorBinaryOperator binaryOperator, boolean typedLabelContainer) {
        if (binaryOperator instanceof VectorBinarySet || binaryOperator.match() != VectorMatch.NONE) {
            return;
        }
        if (typedLabelContainer) {
            return;
        }
        LogicalPlan left = binaryOperator.left();
        LogicalPlan right = binaryOperator.right();
        if ((usesReduction(left) && joinableConcreteReduction(left) == false)
            || (usesReduction(right) && joinableConcreteReduction(right) == false)) {
            failures.add(
                fail(
                    binaryOperator,
                    "binary operations over topk, bottomk, limitk or limit_ratio are not supported at this time [{}]",
                    binaryOperator.sourceText()
                )
            );
        }
    }

    private static boolean usesReduction(LogicalPlan plan) {
        return plan.anyMatch(AcrossSeriesReduction.class::isInstance);
    }

    /**
     * A {@code topk}/{@code bottomk}/... whose output names at least one label (no label container). Those join on the named labels;
     * an unlabeled or opaque reduction cannot under a keyword label container (bind regrouping).
     */
    private static boolean joinableConcreteReduction(LogicalPlan plan) {
        return usesReduction(plan) && hasConcreteLabels(plan) && namedLabels(plan).isEmpty() == false;
    }

    private static List<String> namedLabels(LogicalPlan plan) {
        return PromqlLabels.labelNames(plan.output().stream().filter(a -> PromqlLabels.isTagsLabel(a) == false).toList());
    }

    private static boolean usesWithoutGrouping(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof AcrossSeriesAggregate agg && agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT);
    }

    private static boolean hasConcreteLabels(LogicalPlan plan) {
        return PromqlLabels.getTagsLabel(plan.output()) == null;
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
}
