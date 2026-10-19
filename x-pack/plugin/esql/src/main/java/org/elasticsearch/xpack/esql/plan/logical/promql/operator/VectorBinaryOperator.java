/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.hasTsMetadata;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.getType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.returnsScalar;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.intersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.subtract;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

public abstract sealed class VectorBinaryOperator extends BinaryPlan implements PromqlPlan permits VectorBinarySet, VectorBinaryComparison,
    VectorBinaryArithmetic {

    /**
     * Give the build operand fresh identities for self-joins. Rename its value because {@link InnerJoin#output()}
     * merges columns by name and would otherwise replace the probe value.
     */
    private record PlanRefresher(String valueName, String refreshedValueName, Map<NameId, NameId> mapper) {
        PlanRefresher(String valueName) {
            this(valueName, TemporaryNameGenerator.locallyUniqueTemporaryName(valueName), new HashMap<>());
        }

        private Expression refresh(Expression expression) {
            if (expression instanceof NamedExpression ne) {
                String name = ne.name().equals(valueName) ? refreshedValueName : ne.name();
                NameId id = mapper.computeIfAbsent(ne.id(), k -> new NameId());
                if (ne instanceof Attribute attribute) {
                    return attribute.withName(name).withId(id);
                }
                if (ne instanceof Alias alias) {
                    return alias.withName(name).withId(id);
                }
            }
            return expression;
        }

        IntermediateResult apply(IntermediateResult input) {
            return new IntermediateResult(
                input.plan().transformExpressionsDown(Expression.class, this::refresh),
                input.schema(),
                refresh(input.valueColumn()),
                (Attribute) refresh(input.step()),
                input.pendingFilter(),
                input.kind()
            );
        }
    }

    private final VectorMatch match;
    private final boolean dropMetricName;
    private final BinaryOp binaryOp;
    private List<Attribute> output;

    /** One side of the join */
    private record PreparedJoinInput(LogicalPlan plan, List<Attribute> keys) {}

    /**
     * Underlying binary operation (e.g. +, -, *, /, etc.) being performed
     * on the actual values of the vectors.
     */
    public interface BinaryOp {
        String name();

        ScalarFunctionFactory asFunction();
    }

    public interface ScalarFunctionFactory {
        Function create(Source source, Expression left, Expression right, Configuration configuration);
    }

    protected VectorBinaryOperator(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        VectorMatch match,
        boolean dropMetricName,
        BinaryOp binaryOp
    ) {
        super(source, left, right);
        this.match = match;
        this.dropMetricName = dropMetricName;
        this.binaryOp = binaryOp;
    }

    /** The declared vector matching; {@link VectorMatch#NONE} - never null - when the operator declares none. */
    public VectorMatch match() {
        return match;
    }

    public BinaryOp binaryOp() {
        return binaryOp;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = computeOutput();
        }
        return output;
    }

    private List<Attribute> computeOutput() {
        // Between an instant vector and a scalar,
        // the operator is applied to the value of every data sample in the vector.
        // Therefore, we're returning any grouping attributes (like those created for by (...) and _timeseries) from the vector.
        // If both the left and right are a scalar, this works, too
        // as both outputs will be empty (scalars don't have any grouping attributes).
        if (returnsScalar(left())) {
            return right().output();
        }
        if (returnsScalar(right())) {
            return left().output();
        }
        Set<String> outputLabels;
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        Set<String> leftLabels = extractLabelNames(leftAttrs);
        Set<String> rightLabels = extractLabelNames(rightAttrs);
        if (match.grouping() != Joining.NONE) {
            // Keep the many side's labels and copy only explicitly included labels from the one side.
            boolean manyRight = match.grouping() == Joining.RIGHT;
            List<Attribute> many = manyRight ? rightAttrs : leftAttrs;
            if (match.groupingLabels().isEmpty() && hasTsMetadata(many)) {
                // Matching reads named keys, but the result retains the many side's runtime identity.
                return many;
            }
            outputLabels = new HashSet<>(manyRight ? rightLabels : leftLabels);
            outputLabels.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
            }
        } else if (match.filter() == VectorMatch.Filter.ON) {
            outputLabels = new HashSet<>(match.filterLabels());
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.removeAll(match.filterLabels());
        } else if (hasTsMetadata(leftAttrs) && hasTsMetadata(rightAttrs) == false) {
            // A default one-to-one match proves equality of the complete label sets. The concrete
            // side therefore describes every successful result, even when it is the logical RHS.
            outputLabels = new HashSet<>(rightLabels);
        } else if (leftLabels.equals(rightLabels)) {
            return leftAttrs;
        } else {
            // Default matching between different label sets: a pair matches only where the labels one side lacks are
            // absent on the other side too (a Prometheus signature has no entry for an absent label), and like every
            // one-to-one match the result carries the left operand's labels.
            outputLabels = new HashSet<>(leftLabels);
        }

        if (dropMetricName) {
            outputLabels.remove(LabelMatcher.NAME);
        }

        List<Attribute> result = new ArrayList<>();
        for (String label : outputLabels) {
            Attribute attr = findAttribute(label, leftAttrs, rightAttrs);
            if (attr == null) {
                // Matching and grouping can request labels neither operand declares; translation materializes or null-fills them.
                attr = new ReferenceAttribute(source(), label, DataType.KEYWORD);
            }
            result.add(attr);
        }

        return result;
    }

    private HashSet<String> extractLabelNames(List<Attribute> attrs) {
        var labels = new HashSet<String>();
        for (var attr : attrs) {
            var name = attr.name();
            if (name.equals("value") == false) {
                labels.add(name);
            }
        }
        return labels;
    }

    private Attribute findAttribute(String name, List<Attribute> left, List<Attribute> right) {
        for (var attr : left) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        for (var attr : right) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        return null;
    }

    @Override
    public abstract VectorBinaryOperator replaceChildren(LogicalPlan newLeft, LogicalPlan newRight);

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o)) {
            VectorBinaryOperator that = (VectorBinaryOperator) o;
            return dropMetricName == that.dropMetricName && Objects.equals(match, that.match) && Objects.equals(binaryOp, that.binaryOp);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), match, dropMetricName, binaryOp);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PromQL plans should not be serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PromQL plans should not be serialized");
    }

    @Override
    public PromqlDataType returnType() {
        PromqlDataType leftType = getType(left());
        PromqlDataType rightType = getType(right());
        // scalar op scalar -> scalar; otherwise -> vector
        if (leftType == SCALAR && rightType == SCALAR) {
            return SCALAR;
        }
        return PromqlDataType.INSTANT_VECTOR;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Matches and merges two operands' series identities: a relabel below either operand feeds this boundary.
        return false;
    }

    /** Whether the operands can compose over a shared aggregate. */
    public boolean canTranslateAsAggregate() {
        if (returnsScalar(left()) || returnsScalar(right())) {
            return true;
        }
        return requiresHashJoin() == false && hasBinaryOperatorSubtree(left()) == false && hasBinaryOperatorSubtree(right()) == false;
    }

    /** Whether this operator's own matching rules require a join, independent of its descendants. */
    private boolean requiresHashJoin() {
        if (returnsScalar(left()) || returnsScalar(right())) {
            return false;
        }

        if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) {
            return true;
        }

        var left = left().output();
        var right = right().output();

        boolean leftHasTsMetadata = hasTsMetadata(left);
        boolean rightHasTsMetadata = hasTsMetadata(right);
        if (leftHasTsMetadata || rightHasTsMetadata) {
            return leftHasTsMetadata != rightHasTsMetadata;
        }

        return extractLabelNames(left).equals(extractLabelNames(right)) == false;
    }

    private static boolean hasBinaryOperatorSubtree(LogicalPlan plan) {
        // Check local matching rules only: calling canTranslateAsAggregate() here would repeatedly traverse the same subtrees.
        // Scalar and set operators need no matching join themselves, but their descendants can still contain one.
        return plan.anyMatch(
            p -> p instanceof VectorBinaryOperator vbo && vbo instanceof VectorBinarySet == false && vbo.requiresHashJoin()
        );
    }

    /** Translates vector matching as a join; compatible operands compose over a shared aggregate. */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (canTranslateAsAggregate()) {
            return translateAggregate(context);
        }
        return translateJoinOperator(context);
    }

    /** Composes a binary operator as an expression over the operands' shared aggregate. */
    private IntermediateResult translateAggregate(TranslationContext context) {
        IntermediateResult left = context.translate(left());
        Expression leftExpr = new ToDouble(left.value().source(), left.value());
        if (this instanceof VectorBinaryComparison comp && comp.filterMode()) {
            return left.with(left.plan(), left.schema(), leftExpr);
        }

        IntermediateResult right = context.translate(right());
        Expression rightExpr = new ToDouble(right.value().source(), right.value());
        Expression binaryExpr = binaryOp().asFunction().create(source(), leftExpr, rightExpr, context.configuration());

        LogicalPlan plan;
        Expression filter;
        if (left.kind().afterInitialAggregation && right.kind().afterInitialAggregation) {
            plan = mergeAggregateOperators(left, right);
            filter = null;
        } else {
            plan = left.kind().afterInitialAggregation ? left.plan() : right.plan();
            filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
        }
        TranslationSchema schema = left.schema().equals(TranslationSchema.EMPTY) == false ? left.schema() : right.schema();
        Kind kind = left.kind().afterInitialAggregation || right.kind().afterInitialAggregation
            ? Kind.AFTER_INITIAL_AGGREGATE
            : Kind.BEFORE_INITIAL_AGGREGATE;
        IntermediateResult result = new IntermediateResult(plan, schema, null, left.step(), filter, kind);
        return context.eval(result, binaryExpr);
    }

    /**
     * Translates a vector-matched join operator into an {@link InnerJoin}: each operand becomes an independent series
     * pipeline, joined on shared {@code step} + label keys, and the result value is computed on the joined rows.
     * The operands compile against the labels the join requires, like any other schema push-down: a required label
     * comes back as a concrete column wherever the operand can carry it, and a label the operand dropped stays
     * absent and null-fills at the join.
     * <p>
     * The block has three rules: how the sides are ordered (which operand probes, which builds and is re-identified), how
     * the fields are placed (each side's match key packed next to step, the build side's value and {@code group_x} labels
     * carried across, every result label bound to the operand carrying it or to null) and what is projected (the build
     * side down to its join fields, the result down to value, step and the schema's labels). The result exposes its step
     * column under the enclosing translation's step identity, and a label the match dropped null-fills rather than leaking
     * from an operand.
     */
    public IntermediateResult translateJoinOperator(TranslationContext context) {
        TranslationSchema schema = union(finite(namedLabels(output())), finite(context.required().labels()));
        if (hasTsMetadata(output())) {
            // Keep result identity separate from the join keys. Parent aggregations may request additional
            // metadata projections, but the full result identity must survive until those aggregations consume it.
            TranslationSchema identity = union(open(), context.required());
            schema = union(schema, dropMetricName ? subtract(identity, Set.of(LabelMatcher.NAME)) : identity);
        }
        TranslationSchema childSchema = schema;
        if (match.filter() == VectorMatch.Filter.ON) {
            childSchema = union(childSchema, finite(match.filterLabels()));
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            childSchema = union(childSchema, open(match.filterLabels()));
        }

        // Preserve each operand's series identity, including on the build side. Grouping only by the match keys
        // could hide duplicates that the join must reject.
        boolean leftHasTsMetadata = hasTsMetadata(left().output());
        boolean rightHasTsMetadata = hasTsMetadata(right().output());
        Set<String> residualSkip = null;
        if (match.filter() == VectorMatch.Filter.NONE && (leftHasTsMetadata || rightHasTsMetadata)) {
            assert leftHasTsMetadata != rightHasTsMetadata : "default matching requires at least one concrete label set";
            residualSkip = new TreeSet<>(namedLabels(leftHasTsMetadata ? right().output() : left().output()));
            childSchema = union(childSchema, finite(residualSkip));
            residualSkip.add(LabelMatcher.NAME);
            childSchema = union(childSchema, open(residualSkip));
        }
        IntermediateResult left = translateOperand(context, left(), childSchema, leftHasTsMetadata);
        IntermediateResult right = translateOperand(context, right(), childSchema, rightHasTsMetadata);
        if (residualSkip != null) {
            if (leftHasTsMetadata) {
                left = requireEmptyResidual(context, left, residualSkip);
            } else {
                right = requireEmptyResidual(context, right, residualSkip);
            }
        }

        // The probe carries the many side; the build must be unique per match key.
        // Without a group modifier both sides must be unique, so prefer the concrete side as the probe:
        // equality of the full signatures proves its labels also describe the raw operand.
        boolean probeRight = match.grouping() == Joining.RIGHT
            || (match.grouping() == Joining.NONE && match.filter() == VectorMatch.Filter.NONE && leftHasTsMetadata);
        IntermediateResult probe = probeRight ? right : left;
        IntermediateResult build = new PlanRefresher(context.cmd().valueColumnName()).apply(probeRight ? left : right);
        Expression leftValue = probeRight ? build.value() : probe.value();
        Expression rightValue = probeRight ? probe.value() : build.value();

        LogicalPlan join = emitInnerJoin(context, probe, build);
        List<NamedExpression> output = bindOutput(context, schema, output(), probe, build);
        return projectFinal(context, schema, leftValue, rightValue, probe.step(), join, output);
    }

    /** Named labels exclude the internal metadata identity, which is never itself a PromQL label. */
    private static List<String> namedLabels(List<Attribute> attributes) {
        return TranslationContext.mapFinite(
            attributes.stream().filter(a -> MetadataAttribute.isTimeSeriesAttributeName(a.name()) == false).toList()
        );
    }

    /** Finish each operand at its own series grain before matching; absent samples are not vector elements. */
    private IntermediateResult translateOperand(
        TranslationContext context,
        LogicalPlan operand,
        TranslationSchema required,
        boolean hasTsMetadata
    ) {
        IntermediateResult result = context.withRequired(hasTsMetadata ? union(required, open()) : required)
            .translateIntermediate(operand, new NameId(), new NameId());
        return result.with(
            new Filter(source(), result.plan(), new IsNotNull(source(), result.valueColumn())),
            result.schema(),
            result.value()
        );
    }

    /**
     * A runtime identity can equal a concrete one only when it contains no additional labels. The source metadata
     * loader emits the empty JSON object after excluding all known labels (and the metric name). Check it before
     * the build-key uniqueness check, so unrelated series cannot cause a false duplicate-key failure.
     * This uses source exclusions. A translation editing one current metadata column with {@code TimeSeriesUnset}
     * must derive the residual from that column instead, preserving the original result identity.
     */
    private IntermediateResult requireEmptyResidual(TranslationContext context, IntermediateResult input, Set<String> skip) {
        Attribute residual = context.metadataColumn(input, skip);
        assert residual != null : "default matching requires the residual identity " + skip;
        var condition = new Equals(source(), residual, Literal.keyword(source(), "{}"));
        return input.with(new Filter(source(), input.plan(), condition), input.schema(), input.value());
    }

    /**
     * The operator's value computed on the joined rows, then the finished table: value and step exposed under this
     * frame's identities, null-fills defined, comparison filter mode applied, and everything else projected away.
     */
    private IntermediateResult projectFinal(
        TranslationContext context,
        TranslationSchema schema,
        Expression leftValue,
        Expression rightValue,
        Attribute step,
        LogicalPlan join,
        List<NamedExpression> output
    ) {
        PromqlCommand cmd = context.cmd();
        Expression lhsExpr = new ToDouble(leftValue.source(), leftValue);
        Expression rhsExpr = new ToDouble(rightValue.source(), rightValue);
        Expression value = binaryOp().asFunction().create(source(), lhsExpr, rhsExpr, context.configuration());
        Expression filter = null;
        if (this instanceof VectorBinaryComparison comparison) {
            filter = comparison.filterMode() ? value : null;
            value = comparison.filterMode() ? lhsExpr : new ToDouble(value.source(), value);
        }
        // Expose the step under the enclosing frame's step identity so enclosing translations (union branches, parent
        // aggregates) resolve it by id, not just by name.
        Alias stepAlias = new Alias(step.source(), step.name(), step, context.stepAttr().id());
        Alias valueAlias = new Alias(source(), cmd.valueColumnName(), value, new NameId());
        List<Alias> definitions = new ArrayList<>(List.of(valueAlias, stepAlias));
        definitions.addAll(defined(output));
        LogicalPlan plan = new Eval(cmd.source(), join, definitions);
        if (filter != null) {
            plan = new Filter(source(), plan, filter);
        }
        List<NamedExpression> projected = new ArrayList<>(List.of(valueAlias.toAttribute(), stepAlias.toAttribute()));
        output.forEach(column -> projected.add(column.toAttribute()));
        plan = new Project(cmd.source(), plan, projected);

        return new IntermediateResult(plan, schema, valueAlias.toAttribute(), stepAlias.toAttribute(), null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    /** Join on step and the same ordered label keys on both sides. */
    private LogicalPlan emitInnerJoin(TranslationContext context, IntermediateResult probe, IntermediateResult build) {
        var labelsToMatchOn = new TreeSet<String>();
        if (match.filter() == VectorMatch.Filter.ON) {
            labelsToMatchOn.addAll(match.filterLabels());
        } else {
            labelsToMatchOn.addAll(probe.schema().labels());
            labelsToMatchOn.addAll(build.schema().labels());
            labelsToMatchOn.removeAll(match.filterLabels());
            labelsToMatchOn.remove(LabelMatcher.NAME);
        }

        // Only the build value and explicitly copied labels are added to the probe's output.
        var addToProbeSideOutput = new ArrayList<Attribute>();
        addToProbeSideOutput.add(build.valueColumn());
        for (var name : match.groupingLabels()) {
            var field = build.label(name);
            if (field != null) {
                addToProbeSideOutput.add(field);
            }
        }

        PreparedJoinInput buildInput = prepareJoinInput(context, build, List.copyOf(labelsToMatchOn));
        var buildProjection = new ArrayList<>(buildInput.keys());
        buildProjection.addAll(addToProbeSideOutput);

        PreparedJoinInput probeInput = prepareJoinInput(context, probe, List.copyOf(labelsToMatchOn));

        // one-to-one join rejects multiple probe matched rows to a (always unique) build row.
        boolean shouldRejectsProbeDuplicates = match.grouping() == Joining.NONE;
        Source source = context.cmd().source();
        return new InnerJoin(
            source,
            probeInput.plan(),
            new Project(source, buildInput.plan(), buildProjection),
            probeInput.keys(),
            buildInput.keys(),
            addToProbeSideOutput,
            shouldRejectsProbeDuplicates
        );
    }

    /** Define absent labels as null, then form one match key alongside step; an empty key joins on step alone. */
    private PreparedJoinInput prepareJoinInput(TranslationContext context, IntermediateResult input, List<String> keyLabels) {
        List<NamedExpression> key = new ArrayList<>();
        // Under ignoring, retain metadata projections for the labels outside the ignored set.
        if (match.filter() == VectorMatch.Filter.IGNORING) {
            TranslationSchema surviving = intersect(input.schema(), match.filterLabels());
            for (var skip : TranslationContext.finestFirst(surviving.skips())) {
                Attribute metadata = context.metadataColumn(input, skip);
                assert metadata != null : "invariant: metadata column " + skip + " must be carried by the operand";
                key.add(metadata);
            }
        }
        for (var name : keyLabels) {
            Attribute attribute = input.label(name);
            key.add(attribute != null ? attribute : TranslationContext.emitNullExpression(TranslationContext.mapToRef(name)));
        }
        if (key.isEmpty()) {
            return new PreparedJoinInput(input.plan(), List.of(input.step()));
        }

        Source source = context.cmd().source();
        List<Alias> nullFills = defined(key);
        LogicalPlan plan = nullFills.isEmpty() ? input.plan() : new Eval(source, input.plan(), nullFills);
        Attribute packed = new ReferenceAttribute(source, null, PackDims.PACKED_FIELD_NAME, DataType.KEYWORD);
        plan = new PackDims(source, plan, key.stream().map(NamedExpression::toAttribute).toList(), packed);
        return new PreparedJoinInput(plan, List.of(input.step(), packed));
    }

    /** The join result's label columns: every schema label bound to the operand carrying it, or to null. */
    private List<NamedExpression> bindOutput(
        TranslationContext context,
        TranslationSchema schema,
        List<Attribute> declared,
        IntermediateResult probe,
        IntermediateResult build
    ) {
        var output = new ArrayList<NamedExpression>();
        for (var skip : TranslationContext.finestFirst(schema.skips())) {
            var metadata = context.metadataColumn(probe, skip);
            assert metadata != null : "the result must retain the probe identity " + skip;
            output.add(metadata);
        }
        for (var name : schema.labels()) {
            // A label the match semantics dropped (e.g. on(...) narrowing) may still be required by an enclosing
            // translation; it must come back null rather than leak through from an operand.
            var declaredAttr = TranslationContext.find(declared, name);
            if ((dropMetricName && name.equals(LabelMatcher.NAME)) || (declaredAttr == null && hasTsMetadata(declared) == false)) {
                output.add(TranslationContext.emitNullExpression(TranslationContext.mapToRef(name)));
                continue;
            }
            // Null-fill under the operator's own attribute when the carrying operand lacks the label, so the command
            // projection binds it by identity.
            var attribute = match.groupingLabels().contains(name) ? build.label(name) : probe.label(name);
            output.add(
                attribute != null
                    ? attribute
                    : TranslationContext.emitNullExpression(declaredAttr != null ? declaredAttr : TranslationContext.mapToRef(name))
            );
        }
        return output;
    }

    /** The columns among {@code columns} defined inline (aliases) rather than carried by the plan. */
    private static List<Alias> defined(List<? extends NamedExpression> columns) {
        return columns.stream().filter(Alias.class::isInstance).map(Alias.class::cast).toList();
    }

    private static Set<String> groupingNames(Aggregate aggregate) {
        Set<String> names = new HashSet<>();
        for (Expression grouping : aggregate.groupings()) {
            if (grouping instanceof NamedExpression named) {
                names.add(named.name());
            }
        }
        return names;
    }

    /** Fold left and right aggregates into a single aggregate node. */
    private LogicalPlan mergeAggregateOperators(IntermediateResult left, IntermediateResult right) {
        var names = new TemporaryNameGenerator.Monotonic();
        var rightAgg = right.plan().collect(Aggregate.class).getFirst();
        Set<String> rightGroupingNames = groupingNames(rightAgg);

        var result = left.plan().transformDown(Aggregate.class, leftAgg -> {
            boolean groupingsCompatible = leftAgg.groupings().size() == rightAgg.groupings().size()
                && groupingNames(leftAgg).equals(rightGroupingNames);

            if (groupingsCompatible == false) {
                throw new VerificationException("binary operations between vectors with mismatched grouping keys are not yet supported");
            }

            var uniqueAggregates = new LinkedHashSet<Expression>();
            uniqueAggregates.addAll(withFilter(leftAgg.aggregates(), left.pendingFilter()));
            uniqueAggregates.addAll(withFilter(rightAgg.aggregates(), right.pendingFilter()));

            // Only the aggregate functions need fresh names: both operands define `value`. Grouping columns keep their
            // own names - the command projection finds a passthrough label (`labels.pod`) by its canonical name when the
            // analyzer bound the declared output to the bare attribute instead, and a renamed column would not map.
            var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
                if (e instanceof Alias a) {
                    return (NamedExpression) new Alias(a.source(), names.next(a.name()), a.child(), a.id());
                }
                return e;
            }).toList();

            return leftAgg.with(leftAgg.child(), leftAgg.groupings(), newAggregates);
        });

        var rightEvals = right.plan().collect(Eval.class);
        for (Eval eval : rightEvals.reversed()) {
            result = new Eval(eval.source(), result, eval.fields());
        }
        return result;
    }

}
