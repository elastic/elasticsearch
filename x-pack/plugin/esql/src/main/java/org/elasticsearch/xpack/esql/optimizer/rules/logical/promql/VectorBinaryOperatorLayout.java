/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.Header;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.emitNullExpression;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.find;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.finestFirst;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.mapToRef;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

/**
 * The plan block of one vector binary operator with explicit matching, laid out from its two translated operands. The
 * translator translates the children and feeds them in through the fluent setters; {@link #result()} owns the three
 * rules of the block: how the sides are ordered (which operand probes, which builds and is re-identified), how the
 * fields are placed (each side's match key packed next to step, the build side's value and {@code group_x} labels
 * carried across, every result label bound to the operand carrying it or to null) and what is projected (the build
 * side down to its join fields, the result down to value, step and the header's labels).
 * <pre>
 * new VectorBinaryOperatorLayout(op).command(cmd).configuration(cfg).stepId(id).header(header).left(lhs).right(rhs).result()
 * </pre>
 */
final class VectorBinaryOperatorLayout {
    /** One side of the join: its plan with the key columns defined, and the fields the join matches on. */
    private record Input(LogicalPlan plan, List<Attribute> fields) {}

    private final VectorBinaryOperator op;
    private final VectorMatch match;
    /* The operator's declared output: which labels the match produces, and under which attributes. */
    private final List<Attribute> declared;

    private PromqlCommand cmd;
    private Configuration configuration;
    private NameId stepId;
    private Header header;
    private IntermediateResult left;
    private IntermediateResult right;

    VectorBinaryOperatorLayout(VectorBinaryOperator op) {
        this.op = op;
        match = op.match();
        declared = op.output();
    }

    VectorBinaryOperatorLayout command(PromqlCommand cmd) {
        this.cmd = cmd;
        return this;
    }

    VectorBinaryOperatorLayout configuration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    /** The enclosing translation's step identity: the result exposes its step column under this id. */
    VectorBinaryOperatorLayout stepId(NameId stepId) {
        this.stepId = stepId;
        return this;
    }

    /** The labels the result exposes; a label the match dropped null-fills rather than leaking from an operand. */
    VectorBinaryOperatorLayout header(Header header) {
        this.header = header;
        return this;
    }

    VectorBinaryOperatorLayout left(IntermediateResult left) {
        this.left = left;
        return this;
    }

    VectorBinaryOperatorLayout right(IntermediateResult right) {
        this.right = right;
        return this;
    }

    /** The table the operator produces: value and step columns plus the header's labels, one row per matched pair. */
    IntermediateResult result() {
        assert cmd != null && configuration != null && stepId != null && header != null && left != null && right != null
            : "invariant: every input must be set before laying out " + op.sourceText();
        // Orientation: the probe side keeps its identities; the build side is re-identified so a self-join of
        // structurally identical operands has distinct attributes on each side.
        boolean probeRight = match.grouping() == Joining.RIGHT;
        IntermediateResult probe = probeRight ? right : left;
        IntermediateResult build = reidentify(probeRight ? left : right);
        Expression leftValue = probeRight ? build.value() : probe.value();
        Expression rightValue = probeRight ? probe.value() : build.value();

        LogicalPlan join = emitJoin(probe, build, keyLabels());
        List<NamedExpression> output = bindOutput(probe, build);
        return bindResult(leftValue, rightValue, probe.step(), join, output);
    }

    /**
     * The labels both sides pack into the match key, in one shared order: the on(...) labels as written, otherwise the
     * union of the operands' labels minus the ignored ones, sorted by name. A side that lacks a key label packs null
     * there, so the key behaves like a Prometheus signature: a label absent on both sides does not discriminate, a label
     * present on one side only never matches. Operands over different label sets therefore evaluate to the empty
     * vector, and the order in which each operand declares its labels is irrelevant.
     */
    private List<String> keyLabels() {
        if (match.filter() == VectorMatch.Filter.ON) {
            return List.copyOf(match.filterLabels());
        }
        var names = new TreeSet<>(left.header().labels());
        names.addAll(right.header().labels());
        names.removeAll(match.filterLabels());
        return List.copyOf(names);
    }

    /**
     * The operator's value computed on the joined rows, then the finished table: value and step exposed under this
     * frame's identities, null-fills defined, comparison filter mode applied, and everything else projected away.
     */
    private IntermediateResult bindResult(
        Expression leftValue,
        Expression rightValue,
        Attribute step,
        LogicalPlan join,
        List<NamedExpression> output
    ) {
        Expression lhsExpr = new ToDouble(leftValue.source(), leftValue);
        Expression rhsExpr = new ToDouble(rightValue.source(), rightValue);
        Expression value = op.binaryOp().asFunction().create(op.source(), lhsExpr, rhsExpr, configuration);
        Expression filter = null;
        if (op instanceof VectorBinaryComparison comparison) {
            filter = comparison.filterMode() ? value : null;
            value = comparison.filterMode() ? lhsExpr : new ToDouble(value.source(), value);
        }
        // Expose the step under the enclosing frame's step identity so enclosing translations (union branches, parent
        // aggregates) resolve it by id, not just by name.
        Alias stepAlias = new Alias(step.source(), step.name(), step, stepId);
        Alias valueAlias = new Alias(op.source(), cmd.valueColumnName(), value, new NameId());
        List<Alias> definitions = new ArrayList<>(List.of(valueAlias, stepAlias));
        definitions.addAll(defined(output));
        LogicalPlan plan = new Eval(cmd.source(), join, definitions);
        if (filter != null) {
            plan = new Filter(op.source(), plan, filter);
        }
        List<NamedExpression> projected = new ArrayList<>(List.of(valueAlias.toAttribute(), stepAlias.toAttribute()));
        output.forEach(column -> projected.add(column.toAttribute()));
        plan = new Project(cmd.source(), plan, projected);

        return new IntermediateResult(plan, header, valueAlias.toAttribute(), stepAlias.toAttribute(), null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    /**
     * The build operand under fresh identities, so a self-join of structurally identical operands has distinct
     * attributes on each side. Its value column is also renamed: {@link InnerJoin#output()} merges output by NAME (see
     * {@code NamedExpressions#mergeOutputAttributes}), so a build-side column still called {@code value} would shadow
     * the probe side's value column that the operator's expression references.
     */
    private IntermediateResult reidentify(IntermediateResult input) {
        Map<NameId, NameId> ids = new HashMap<>();
        String valueName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        LogicalPlan plan = input.plan()
            .transformExpressionsDown(Expression.class, e -> reidExpr(renamed(e, cmd.valueColumnName(), valueName), ids));
        Expression value = reidExpr(renamed(input.valueColumn(), cmd.valueColumnName(), valueName), ids);
        Attribute step = (Attribute) reidExpr(input.step(), ids);
        return new IntermediateResult(plan, input.header(), value, step, input.pendingFilter(), input.kind());
    }

    /** The inner join of the two operands on step plus the packed match key. */
    private LogicalPlan emitJoin(IntermediateResult probe, IntermediateResult build, List<String> keyLabels) {
        Input probeInput = emitInput(probe, keyLabels);
        Input buildInput = emitInput(build, keyLabels);

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
    private List<Attribute> addedFields(IntermediateResult build) {
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

    /** One side's plan with its match key defined and packed next to step; step alone when the key is empty. */
    private Input emitInput(IntermediateResult input, List<String> keyLabels) {
        List<NamedExpression> key = joinKey(input, keyLabels);
        List<Alias> nullFills = defined(key);
        LogicalPlan plan = nullFills.isEmpty() ? input.plan() : new Eval(cmd.source(), input.plan(), nullFills);
        if (key.isEmpty()) {
            return new Input(plan, List.of(input.step()));
        }
        List<Attribute> keyColumns = key.stream().map(NamedExpression::toAttribute).toList();
        Attribute packed = new ReferenceAttribute(cmd.source(), null, PackDims.PACKED_FIELD_NAME, DataType.KEYWORD);
        return new Input(new PackDims(cmd.source(), plan, keyColumns, packed), List.of(input.step(), packed));
    }

    /**
     * The operand's match key columns: its packed columns surviving the ignored labels (an opaque operand under
     * ignoring), then the shared key labels, each as the operand's own column or a null where it lacks the label.
     */
    private List<NamedExpression> joinKey(IntermediateResult input, List<String> keyLabels) {
        var key = new ArrayList<NamedExpression>();
        if (match.filter() != VectorMatch.Filter.ON) {
            Header surviving = input.header().intersect(match.filterLabels());
            for (Set<String> skip : finestFirst(surviving.skips())) {
                Attribute packed = input.packed(skip);
                assert packed != null : "invariant: packing " + skip + " must be carried by the operand";
                key.add(packed);
            }
        }
        for (String name : keyLabels) {
            Attribute attribute = input.label(name);
            key.add(attribute != null ? attribute : emitNullExpression(mapToRef(name)));
        }
        return key;
    }

    /** The join result's label columns: every header label bound to the operand carrying it, or to null. */
    private List<NamedExpression> bindOutput(IntermediateResult probe, IntermediateResult build) {
        var output = new ArrayList<NamedExpression>();
        for (String name : header.labels()) {
            // A label the match semantics dropped (e.g. on(...) narrowing) may still be required by an enclosing
            // translation; it must come back null rather than leak through from an operand.
            Attribute declaredAttr = find(declared, name);
            if (declaredAttr == null) {
                output.add(emitNullExpression(mapToRef(name)));
                continue;
            }
            // Null-fill under the operator's own attribute when the carrying operand lacks the label, so the command
            // projection binds it by identity.
            Attribute attribute = match.groupingLabels().contains(name) ? build.label(name) : probe.label(name);
            output.add(attribute != null ? attribute : emitNullExpression(declaredAttr));
        }
        return output;
    }

    /** The columns among {@code columns} defined inline (aliases) rather than carried by the plan. */
    private static List<Alias> defined(List<? extends NamedExpression> columns) {
        return columns.stream().filter(Alias.class::isInstance).map(Alias.class::cast).toList();
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
}
