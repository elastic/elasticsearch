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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
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
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.DynamicColumnList;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.Static;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.getType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.any;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.nullAlias;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.ref;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

public abstract sealed class VectorBinaryOperator extends BinaryPlan implements PromqlPlan permits VectorBinarySet, VectorBinaryComparison,
    VectorBinaryArithmetic {

    private final VectorMatch match;
    private final boolean dropMetricName;
    private final BinaryOp binaryOp;
    private List<Attribute> output;

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
        this.match = Objects.requireNonNull(match, "match must be VectorMatch.NONE rather than null");
        this.dropMetricName = dropMetricName;
        this.binaryOp = binaryOp;
    }

    /** The declared vector matching; {@link VectorMatch#NONE} - never null - when the operator declares none. */
    public VectorMatch match() {
        return match;
    }

    public boolean dropMetricName() {
        return dropMetricName;
    }

    public BinaryOp binaryOp() {
        return binaryOp;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = computeOutputAttributes();
        }
        return output;
    }

    private List<Attribute> computeOutputAttributes() {
        // Between an instant vector and a scalar,
        // the operator is applied to the value of every data sample in the vector.
        // Therefore, we're returning any grouping attributes (like those created for by (...) and _timeseries) from the vector.
        // If both the left and right are a scalar, this works, too
        // as both outputs will be empty (scalars don't have any grouping attributes).
        if (PromqlPlan.returnsScalar(left())) {
            return right().output();
        }
        if (PromqlPlan.returnsScalar(right())) {
            return left().output();
        }
        Set<String> outputLabels;
        // Labels the translation guarantees as result columns even when no operand declares them as attributes (an
        // operand can be opaque - its identity packed into `_timeseries` - and the translation materializes or
        // null-fills the columns): the match keys named by on(...) and the group modifier labels. Ones that resolve
        // against neither operand are declared as synthesized references.
        Set<String> guaranteed = new HashSet<>();
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        Set<String> leftLabels = extractLabelNames(leftAttrs);
        Set<String> rightLabels = extractLabelNames(rightAttrs);
        if (match.grouping() == VectorMatch.Joining.LEFT) {
            // group_left keeps the left ("many") label set and copies only explicitly included labels from the right.
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.addAll(match.groupingLabels());
            guaranteed.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
                guaranteed.addAll(match.filterLabels());
            }
        } else if (match.grouping() == VectorMatch.Joining.RIGHT) {
            // group_right keeps the right ("many") label set and copies only explicitly included labels from the left.
            outputLabels = new HashSet<>(rightLabels);
            outputLabels.addAll(match.groupingLabels());
            guaranteed.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
                guaranteed.addAll(match.filterLabels());
            }
        } else if (match.filter() == VectorMatch.Filter.ON) {
            outputLabels = new HashSet<>(match.filterLabels());
            guaranteed.addAll(match.filterLabels());
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.removeAll(match.filterLabels());
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
            if (attr != null) {
                result.add(attr);
            } else if (guaranteed.contains(label)) {
                result.add(new ReferenceAttribute(source(), label, DataType.KEYWORD));
            }
        }

        return result;
    }

    /**
     * Whether both operands declare concrete label sets that differ by name. Prometheus default matching compares each
     * pair's actual label sets, so such operands cannot fold into one aggregate over shared grouping keys; they
     * translate as a join, which yields the empty vector unless the differing labels are null on both sides.
     */
    public boolean hasMismatchedLabelSets() {
        if (match != VectorMatch.NONE || PromqlPlan.returnsScalar(left()) || PromqlPlan.returnsScalar(right())) {
            return false;
        }
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        if (hasPackedLabels(leftAttrs) || hasPackedLabels(rightAttrs)) {
            return false;
        }
        return extractLabelNames(leftAttrs).equals(extractLabelNames(rightAttrs)) == false;
    }

    /** Whether the operand carries a packed {@code _timeseries} column, i.e. does not name every label it exposes. */
    private static boolean hasPackedLabels(List<Attribute> attrs) {
        return attrs.stream().anyMatch(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()));
    }

    private Set<String> extractLabelNames(List<Attribute> attrs) {
        Set<String> labels = new HashSet<>();
        for (Attribute attr : attrs) {
            String name = attr.name();
            if (name.equals("value") == false) {
                labels.add(name);
            }
        }
        return labels;
    }

    private Attribute findAttribute(String name, List<Attribute> left, List<Attribute> right) {
        for (Attribute attr : left) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        for (Attribute attr : right) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        return null;
    }

    // ---------- translation ----------

    /** Explicit vector matching translates as a join; other binary operators compose over the operands' shared frame. */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        if (match.filter() == VectorMatch.Filter.NONE && match.grouping() == Joining.NONE) {
            boolean scalarOperand = left().resolved() && getType(left()) == SCALAR || right().resolved() && getType(right()) == SCALAR;
            boolean nestedMatch = anyMatchVectorBinaryOperator(left()) || anyMatchVectorBinaryOperator(right());
            // Operands over one label set fold into a shared aggregate. Different concrete label sets match like
            // Prometheus does, pair by pair on the actual labels, which only the join expresses.
            if (scalarOperand || (nestedMatch == false && hasMismatchedLabelSets() == false)) {
                return translateFused(translation);
            }
        }
        return translateJoin(translation);
    }

    private static boolean anyMatchVectorBinaryOperator(LogicalPlan plan) {
        return plan.anyMatch(
            p -> p instanceof VectorBinaryOperator vbo
                && (vbo.match.filter() != VectorMatch.Filter.NONE || vbo.match.grouping() != Joining.NONE)
        );
    }

    /** Composes the operator as an expression over the operands' shared aggregate; the operands pass the requirement through. */
    private TranslationResult translateFused(TranslationContext translation) {
        // IN: required, on both operands
        TranslationConstraint required = translation.required();
        TranslationResult left = translation.translate(left(), required);
        Expression leftExpr = new ToDouble(left.value().source(), left.value());
        if (this instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
            return left.with(left.plan(), leftExpr);
        }
        TranslationResult right = translation.translate(right(), required);
        Expression rightExpr = new ToDouble(right.value().source(), right.value());
        Expression binaryExpr = binaryOp.asFunction().create(source(), leftExpr, rightExpr, translation.configuration());

        LogicalPlan plan;
        Expression filter;
        TranslationResult ir;
        if (left.kind().afterInitialAggregation && right.kind().afterInitialAggregation) {
            plan = fuse(left, right);
            ir = left;
            filter = null;
        } else {
            ir = getType(left()) != SCALAR ? left : getType(right()) != SCALAR ? right : left.kind().afterInitialAggregation ? left : right;
            plan = ir.plan();
            filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
        }
        Kind kind = left.kind().afterInitialAggregation || right.kind().afterInitialAggregation
            ? Kind.AFTER_INITIAL_AGGREGATE
            : Kind.BEFORE_INITIAL_AGGREGATE;
        // OUT: the vector operand's labels (left when both are)
        TranslationResult result = new TranslationResult(plan, ir.labels(), null, ir.step(), filter, kind);
        return translation.eval(result, binaryExpr);
    }

    /** Folds the left and right aggregates into a single plan. */
    private static LogicalPlan fuse(TranslationResult left, TranslationResult right) {
        var names = new TemporaryNameGenerator.Monotonic();
        var rightAgg = right.plan().collect(Aggregate.class).getFirst();
        List<Expression> rightGroupings = rightAgg.groupings();

        var result = left.plan().transformDown(Aggregate.class, leftAgg -> {
            List<Expression> leftGroupings = leftAgg.groupings();
            Set<String> leftGroupingNames = new HashSet<>();
            for (Expression grouping : leftGroupings) {
                if (grouping instanceof NamedExpression ne) {
                    leftGroupingNames.add(ne.name());
                }
            }
            Set<String> rightGroupingNames = new HashSet<>();
            for (Expression grouping : rightGroupings) {
                if (grouping instanceof NamedExpression ne) {
                    rightGroupingNames.add(ne.name());
                }
            }
            boolean groupingsCompatible = leftGroupings.size() == rightGroupings.size() && leftGroupingNames.equals(rightGroupingNames);

            if (groupingsCompatible == false) {
                throw new VerificationException("binary operations between vectors with mismatched grouping keys are not yet supported");
            }

            // Grouping columns match by name. Relation fields share one attribute on both sides, but a packing alias
            // (`_timeseries$__name__`) is minted per collapse: rebind the right side's keys to the left side's so the
            // fused aggregate carries each key once and every reference resolves against the surviving grouping.
            Map<String, Attribute> leftKeys = new HashMap<>();
            for (Expression grouping : leftAgg.groupings()) {
                if (grouping instanceof NamedExpression ne) {
                    leftKeys.put(ne.name(), ne.toAttribute());
                }
            }
            Map<NameId, Attribute> rightToLeft = new HashMap<>();
            for (Expression grouping : rightAgg.groupings()) {
                if (grouping instanceof NamedExpression ne && leftKeys.containsKey(ne.name())) {
                    rightToLeft.put(ne.toAttribute().id(), leftKeys.get(ne.name()));
                }
            }
            List<? extends Expression> rightAggregates = rightAgg.aggregates()
                .stream()
                .map(e -> e.transformUp(Attribute.class, a -> rightToLeft.getOrDefault(a.id(), a)))
                .toList();

            var uniqueAggregates = new LinkedHashSet<Expression>();
            uniqueAggregates.addAll(withFilter(leftAgg.aggregates(), left.pendingFilter()));
            uniqueAggregates.addAll(withFilter(rightAggregates, right.pendingFilter()));

            // Only the aggregate functions need fresh names: both operands define `value`. Grouping columns keep their
            // own names - the command projection finds a passthrough label (`labels.pod`) by its canonical name when the
            // analyzer bound the declared output to the bare attribute instead, and a renamed column would not map.
            var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
                if (e instanceof Alias a) {
                    return (NamedExpression) new Alias(a.source(), names.next(a.name()), a.child(), a.id());
                }
                return e;
            }).toList();

            return leftAgg.with(leftAgg.child(), leftGroupings, newAggregates);
        });

        var rightEvals = right.plan().collect(Eval.class);
        for (Eval eval : rightEvals.reversed()) {
            result = new Eval(eval.source(), result, eval.fields());
        }
        return result;
    }

    /**
     * Translates explicit vector matching as an {@link InnerJoin}: each operand becomes an independent series pipeline,
     * joined on shared {@code step} + label keys, and the result value is computed on the joined rows. The operands
     * compile against the labels the join requires, like any other requirement push-down: a required label comes back as
     * a concrete column wherever the operand can carry it, and a label the operand dropped stays absent and null-fills at
     * the join. The block has three rules: how the sides are ordered (which operand probes, which builds and is
     * re-identified), how the fields are placed (each side's match key packed next to step, the build side's value and
     * {@code group_x} labels carried across, every result label bound to the operand carrying it or to null) and what is
     * projected (the build side down to its join fields, the result down to value, step and the required labels).
     */
    private TranslationResult translateJoin(TranslationContext translation) {
        // TODO: revisit once expression like foo on(a,b) / bar is supported
        // IN: declared output labels + required names (operands have concrete label sets, so every label is named);
        // key on top: on (k) -> k; ignoring (i) -> any - i; none -> declared
        TranslationConstraint declared = union(of(PromqlLabels.labelNames(output())), of(translation.required().names()));
        TranslationConstraint in = switch (match.filter()) {
            case ON -> union(declared, of(match.filterLabels()));
            case IGNORING -> union(declared, sub(any(), of(match.filterLabels())));
            case NONE -> {
                assert hasPackedLabels(left().output()) == false && hasPackedLabels(right().output()) == false
                    : "[INVARIANT]: an unmatched join needs operands with concrete label sets [" + sourceText() + "]";
                yield declared;
            }
        };
        TranslationResult left = translation.translateOperand(left(), in);
        TranslationResult right = translation.translateOperand(right(), in);

        // Orientation: the probe side keeps its identities; the build side is re-identified so a self-join of
        // structurally identical operands has distinct attributes on each side.
        PromqlCommand cmd = translation.cmd();
        boolean probeRight = match.grouping() == Joining.RIGHT;
        TranslationResult probe = probeRight ? right : left;
        TranslationResult build = reidentify(cmd, probeRight ? left : right);
        Expression leftValue = probeRight ? build.value() : probe.value();
        Expression rightValue = probeRight ? probe.value() : build.value();

        LogicalPlan join = emitJoin(translation, probe, build, keyLabels(left, right));
        // OUT: every declared label bound to the operand carrying it, or null
        Output output = bindOutput(declared, probe, build);
        return bindResult(translation, leftValue, rightValue, probe.step(), join, output);
    }

    /** One side of the join: its plan with the key columns defined, and the fields the join matches on. */
    private record Input(LogicalPlan plan, List<Attribute> fields) {}

    /** The join result's label columns, and the ones defined as null rather than taken from an operand. */
    private record Output(Map<TranslationColumn, Attribute> labels, List<Alias> nullFills) {}

    /**
     * The labels both sides pack into the match key, in one shared order: the on(...) labels as written, otherwise the
     * union of the operands' labels minus the ignored ones, sorted by name. A side that lacks a key label packs null
     * there, so the key behaves like a Prometheus signature: a label absent on both sides does not discriminate, a label
     * present on one side only never matches. Operands over different label sets therefore evaluate to the empty
     * vector, and the order in which each operand declares its labels is irrelevant.
     */
    private List<String> keyLabels(TranslationResult left, TranslationResult right) {
        if (match.filter() == VectorMatch.Filter.ON) {
            return List.copyOf(match.filterLabels());
        }
        var names = new TreeSet<>(left.statics());
        names.addAll(right.statics());
        names.removeAll(match.filterLabels());
        return List.copyOf(names);
    }

    /**
     * The operator's value computed on the joined rows, then the finished table: value and step exposed under this
     * frame's identities, null-fills defined, comparison filter mode applied, and everything else projected away.
     */
    private TranslationResult bindResult(
        TranslationContext translation,
        Expression leftValue,
        Expression rightValue,
        Attribute step,
        LogicalPlan join,
        Output output
    ) {
        PromqlCommand cmd = translation.cmd();
        Expression lhsExpr = new ToDouble(leftValue.source(), leftValue);
        Expression rhsExpr = new ToDouble(rightValue.source(), rightValue);
        Expression value = binaryOp.asFunction().create(source(), lhsExpr, rhsExpr, translation.configuration());
        Expression filter = null;
        if (this instanceof VectorBinaryComparison comparison) {
            filter = comparison.filterMode() ? value : null;
            value = comparison.filterMode() ? lhsExpr : new ToDouble(value.source(), value);
        }
        // Expose the step under the enclosing frame's step identity so enclosing translations (union branches, parent
        // aggregates) resolve it by id, not just by name.
        Alias stepAlias = new Alias(step.source(), step.name(), step, translation.stepAttr().id());
        Alias valueAlias = new Alias(source(), cmd.valueColumnName(), value, new NameId());
        List<Alias> definitions = new ArrayList<>(List.of(valueAlias, stepAlias));
        definitions.addAll(output.nullFills());
        LogicalPlan plan = new Eval(cmd.source(), join, definitions);
        if (filter != null) {
            plan = new Filter(source(), plan, filter);
        }
        List<NamedExpression> projected = new ArrayList<>(List.of(valueAlias.toAttribute(), stepAlias.toAttribute()));
        projected.addAll(output.labels().values());
        plan = new Project(cmd.source(), plan, projected);

        return new TranslationResult(
            plan,
            output.labels(),
            valueAlias.toAttribute(),
            stepAlias.toAttribute(),
            null,
            Kind.AFTER_INITIAL_AGGREGATE
        );
    }

    /**
     * The build operand under fresh identities, so a self-join of structurally identical operands has distinct
     * attributes on each side. Its value column is also renamed: {@link InnerJoin#output()} merges output by NAME (see
     * {@code NamedExpressions#mergeOutputAttributes}), so a build-side column still called {@code value} would shadow
     * the probe side's value column that the operator's expression references.
     */
    private static TranslationResult reidentify(PromqlCommand cmd, TranslationResult input) {
        Map<NameId, NameId> ids = new HashMap<>();
        String valueName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        LogicalPlan plan = input.plan()
            .transformExpressionsDown(Expression.class, e -> reidExpr(renamed(e, cmd.valueColumnName(), valueName), ids));
        Expression value = reidExpr(renamed(input.valueColumn(), cmd.valueColumnName(), valueName), ids);
        Attribute step = (Attribute) reidExpr(input.step(), ids);
        var reidentified = new LinkedHashMap<TranslationColumn, Attribute>();
        input.labels().forEach((column, attribute) -> reidentified.put(column, (Attribute) reidExpr(attribute, ids)));
        return new TranslationResult(plan, reidentified, value, step, input.pendingFilter(), input.kind());
    }

    /** The inner join of the two operands on step plus the packed match key. */
    private LogicalPlan emitJoin(TranslationContext translation, TranslationResult probe, TranslationResult build, List<String> keyLabels) {
        PromqlCommand cmd = translation.cmd();
        Input probeInput = emitInput(translation, probe, keyLabels);
        Input buildInput = emitInput(translation, build, keyLabels);

        // The build side carries its join fields plus what the join adds: its value and the group_x labels. Neither can
        // already be a join field (the step, or the freshly packed key), so the two lists are disjoint.
        List<Attribute> added = addedFields(build);
        List<NamedExpression> projection = new ArrayList<>(buildInput.fields());
        projection.addAll(added);
        LogicalPlan buildPlan = new Project(cmd.source(), buildInput.plan(), projection);

        return new InnerJoin(
            cmd.source(),
            probeInput.plan(),
            buildPlan,
            probeInput.fields(),
            buildInput.fields(),
            added,
            match.grouping() == Joining.NONE
        );
    }

    /** The columns the join adds from the build side: its value and the labels a group_x modifier copies over. */
    private List<Attribute> addedFields(TranslationResult build) {
        List<Attribute> fields = new ArrayList<>();
        fields.add(build.valueColumn());
        for (String name : match.groupingLabels()) {
            Attribute field = build.label(name);
            if (field != null) {
                fields.add(field);
            }
        }
        return fields;
    }

    /**
     * One side's plan with its match key defined and packed next to step; step alone when the key is empty. The key is the
     * shared key labels, each as the operand's own column or a null where it lacks the label, plus the packings an opaque
     * operand carries that already exclude the ignored labels.
     */
    private Input emitInput(TranslationContext translation, TranslationResult input, List<String> keyLabels) {
        Source source = translation.cmd().source();
        // KEY: the shared key labels (null where lacking) + an opaque operand's packings
        TranslationResult keyed = translation.bind(input, of(keyLabels), source);
        var key = new LinkedHashMap<>(keyed.labels());
        if (match.filter() != VectorMatch.Filter.ON) {
            input.labels().forEach((column, attribute) -> {
                if (column instanceof DynamicColumnList packing && packing.except().containsAll(match.filterLabels())) {
                    key.put(column, attribute);
                }
            });
        }
        if (key.isEmpty()) {
            return new Input(keyed.plan(), List.of(input.step()));
        }
        List<Attribute> fields = keyed.with(keyed.plan(), key, keyed.value()).attributes();
        Attribute packed = new ReferenceAttribute(source, null, PackDims.PACKED_FIELD_NAME, DataType.KEYWORD);
        return new Input(new PackDims(source, keyed.plan(), fields, packed), List.of(input.step(), packed));
    }

    /** The join result's label columns: every required label bound to the operand carrying it, or to null. */
    private Output bindOutput(TranslationConstraint required, TranslationResult probe, TranslationResult build) {
        var columns = new LinkedHashMap<TranslationColumn, Attribute>();
        var nullFills = new ArrayList<Alias>();
        for (String name : required.names()) {
            // A label the match semantics dropped (e.g. on(...) narrowing) may still be required by an enclosing
            // translation; it must come back null rather than leak through from an operand.
            Attribute declaredAttr = PromqlLabels.find(output(), name);
            if (declaredAttr == null) {
                nullFills.add(nullAlias(ref(name)));
                columns.put(new Static(name), nullFills.getLast().toAttribute());
                continue;
            }
            Attribute attribute = match.groupingLabels().contains(name) ? build.label(name) : probe.label(name);
            if (attribute == null) {
                // Null-fill under the operator's own attribute when the carrying operand lacks the label, so the command
                // projection binds it by identity. The join may still carry that id from the other operand (a group_x
                // label the probe has and the build lacks): the definition deliberately shadows it, which is why these
                // null-fills are stated here rather than derived from what the join output lacks.
                nullFills.add(nullAlias(declaredAttr));
                attribute = nullFills.getLast().toAttribute();
            }
            columns.put(new Static(name), attribute);
        }
        // Operands are required to have concrete label sets, so the result names every label and carries no dynamic column.
        return new Output(columns, nullFills);
    }

    /** Renames an attribute or alias in a re-identification pass; other expressions pass through unchanged. */
    private static Expression renamed(Expression e, String from, String to) {
        if (e instanceof Attribute a && a.name().equals(from)) {
            return a.withName(to);
        }
        if (e instanceof Alias a && a.name().equals(from)) {
            return new Alias(a.source(), to, a.child(), a.id());
        }
        return e;
    }

    /** Re-ids a single attribute/alias (leaving other expressions untouched), reusing the shared map for consistency. */
    private static Expression reidExpr(Expression e, Map<NameId, NameId> ids) {
        if (e instanceof Attribute a) {
            return a.withId(ids.computeIfAbsent(a.id(), k -> new NameId()));
        }
        if (e instanceof Alias a) {
            return a.withId(ids.computeIfAbsent(a.id(), k -> new NameId()));
        }
        return e;
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
        // scalar op scalar → scalar; otherwise → vector
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
}
