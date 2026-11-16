/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Score;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Captures one original MEMBER-bearing WHERE conjunct at its own row's binding site. Its earlier-stage set is
 * immutable: scalar simplification must not change when backward search evaluates the conjunct or counts its work.
 * The child output stays public; condition-only inputs never restore columns removed by later projections.
 */
public final class SequenceCondition extends UnaryPlan implements PostAnalysisVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "SequenceCondition",
        SequenceCondition::new
    );

    /** Separates a scalar expression's protected input identity from its canonical stage-local source identity. */
    public record Input(Attribute reference, int stage, Attribute source) implements Writeable {
        public Input {
            Objects.requireNonNull(reference);
            Objects.requireNonNull(source);
            if (stage < 0 || reference.id().equals(source.id()) || reference.dataType() != source.dataType()) {
                throw new IllegalArgumentException("invalid SEQUENCE condition input binding");
            }
        }

        private Input(StreamInput in) throws IOException {
            this(in.readNamedWriteable(Attribute.class), in.readVInt(), in.readNamedWriteable(Attribute.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(reference);
            out.writeVInt(stage);
            out.writeNamedWriteable(source);
        }
    }

    private final int ownStage;
    private final Expression condition;
    private final List<Integer> referencedStages;
    private final List<Input> inputs;

    /** Keeps each moved conjunct and its original stage dependencies distinct through ordinary scalar rewrites. */
    public SequenceCondition(
        Source source,
        LogicalPlan child,
        int ownStage,
        Expression condition,
        List<Integer> referencedStages,
        List<Input> inputs
    ) {
        super(source, child);
        if (ownStage < 1 || referencedStages.isEmpty()) {
            throw new IllegalArgumentException("a SEQUENCE condition must reference an earlier stage");
        }
        int previous = -1;
        for (int stage : referencedStages) {
            if (stage <= previous || stage >= ownStage) {
                throw new IllegalArgumentException("SEQUENCE condition stages must be distinct, sorted and earlier than their own stage");
            }
            previous = stage;
        }
        Set<NameId> ids = new HashSet<>();
        for (Input input : inputs) {
            if ((input.stage() != ownStage && referencedStages.contains(input.stage()) == false)
                || ids.add(input.reference().id()) == false) {
                throw new IllegalArgumentException("invalid or duplicate SEQUENCE condition input");
            }
        }
        this.ownStage = ownStage;
        this.condition = Objects.requireNonNull(condition);
        this.referencedStages = List.copyOf(referencedStages);
        this.inputs = List.copyOf(inputs);
    }

    private SequenceCondition(StreamInput in) throws IOException {
        this(
            readSource(in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readVInt(),
            in.readNamedWriteable(Expression.class),
            in.readCollectionAsList(StreamInput::readVInt),
            in.readCollectionAsList(Input::new)
        );
    }

    private static Source readSource(StreamInput in) throws IOException {
        Sequence.checkPlanSupported(in.getTransportVersion());
        return Source.readFrom((PlanStreamInput) in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Sequence.checkPlanSupported(out.getTransportVersion());
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeVInt(ownStage);
        out.writeNamedWriteable(condition);
        out.writeCollection(referencedStages, StreamOutput::writeVInt);
        out.writeCollection(inputs);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<SequenceCondition> info() {
        return NodeInfo.create(this, SequenceCondition::new, child(), ownStage, condition, referencedStages, inputs);
    }

    @Override
    public SequenceCondition replaceChild(LogicalPlan child) {
        return new SequenceCondition(source(), child, ownStage, condition, referencedStages, inputs);
    }

    public int ownStage() {
        return ownStage;
    }

    public Expression condition() {
        return condition;
    }

    public List<Integer> referencedStages() {
        return referencedStages;
    }

    public List<Input> inputs() {
        return inputs;
    }

    /** Optimized-away leaves retain scheduling provenance but demand no stored value or loadable source field. */
    public List<Input> activeInputs() {
        AttributeSet used = condition.references();
        return inputs.stream().filter(input -> used.contains(input.reference())).toList();
    }

    @Override
    protected AttributeSet computeReferences() {
        var references = AttributeSet.builder();
        activeInputs().stream().filter(input -> input.stage() == ownStage).forEach(input -> references.add(input.source()));
        // Before the parent binds protected inputs, ordinary WHERE attributes still need normal resolution and discovery.
        Set<NameId> bound = new HashSet<>();
        inputs.forEach(input -> bound.add(input.reference().id()));
        for (Attribute attribute : condition.references()) {
            if (attribute instanceof SequenceMatchOutput.MemberReference == false && bound.contains(attribute.id()) == false) {
                references.add(attribute);
            }
        }
        return references.build();
    }

    @Override
    public boolean expressionsResolved() {
        if (condition.resolved() == false) {
            return false;
        }
        Set<NameId> bound = new HashSet<>();
        inputs.forEach(input -> bound.add(input.reference().id()));
        return condition.references().stream().allMatch(attribute -> bound.contains(attribute.id()));
    }

    /** Binds earlier public outputs incrementally while ordinary analysis resolves the row at this WHERE. */
    public SequenceCondition resolve(List<LogicalPlan> stages) {
        if (child().resolved() == false || expressionsResolved()) {
            return this;
        }
        Map<NameId, Input> bindings = new LinkedHashMap<>();
        inputs.forEach(input -> bindings.put(input.reference().id(), input));
        Expression resolved = condition.transformUp(Attribute.class, attribute -> {
            if (bindings.containsKey(attribute.id())) {
                return attribute;
            }
            final Attribute source;
            final int stage;
            if (attribute instanceof SequenceMatchOutput.MemberReference member) {
                SequenceStage earlier = (SequenceStage) stages.get(member.stage());
                if (earlier.resolved() == false) {
                    return member;
                }
                List<Attribute> matches = earlier.output()
                    .stream()
                    .filter(a -> a.synthetic() == false && a.name().equals(member.field()))
                    .toList();
                if (matches.size() != 1) {
                    return member.withUnresolvedMessage(
                        "SEQUENCE stage ["
                            + (member.stage() + 1)
                            + "] has no unique final output column ["
                            + member.field()
                            + "]; MEMBER reads the stage's final KEEP, DROP, RENAME and EVAL output. Expose ["
                            + member.field()
                            + "] in stage ["
                            + (member.stage() + 1)
                            + "] final output: add it to that stage's KEEP, undo its DROP, or use the renamed column in MEMBER"
                    );
                }
                source = matches.getFirst();
                stage = member.stage();
            } else {
                if (attribute.resolved() == false) {
                    return attribute;
                }
                List<Attribute> matches = child().output().stream().filter(a -> a.id().equals(attribute.id())).toList();
                if (matches.size() != 1 || matches.getFirst().dataType() != attribute.dataType()) {
                    throw new IllegalArgumentException("SEQUENCE condition has an invalid own-row input [" + attribute.name() + "]");
                }
                source = matches.getFirst();
                stage = ownStage;
            }
            Attribute reference = SequenceSchema.reference(source, attribute.source(), attribute.name(), new NameId(), true);
            bindings.put(reference.id(), new Input(reference, stage, source));
            return reference;
        });
        List<Input> resolvedInputs = new ArrayList<>(bindings.values());
        return condition.equals(resolved) && inputs.equals(resolvedInputs)
            ? this
            : new SequenceCondition(source(), child(), ownStage, resolved, referencedStages, resolvedInputs);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        Filter.checkFilterConditionDataType(condition, failures);
        condition.forEachDown(expression -> {
            if (expression instanceof FullTextFunction || expression instanceof Score) {
                failures.add(fail(expression, "Full-text and scoring functions are not supported in a SEQUENCE MEMBER condition"));
            }
        });
    }

    @Override
    public boolean skipTelemetry() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), ownStage, condition, referencedStages, inputs);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
            || obj instanceof SequenceCondition other
                && child().equals(other.child())
                && ownStage == other.ownStage
                && condition.equals(other.condition)
                && referencedStages.equals(other.referencedStages)
                && inputs.equals(other.inputs);
    }
}
