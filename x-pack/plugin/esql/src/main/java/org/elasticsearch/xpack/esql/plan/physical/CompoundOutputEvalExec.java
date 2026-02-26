/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Abstract base class for physical plans that produce compound outputs from a single input.
 */
public abstract class CompoundOutputEvalExec extends UnaryExec
    implements
        EstimatesRowSize,
        CompoundOutputEvaluator.OutputFieldsCollectorProvider {

    /**
     * The input by which the evaluation is performed.
     */
    protected final Expression input;

    /**
     * A list of the output field names expected by the evaluation function that corresponds to the concrete subclass.
     * From all fields that can actually be returned by the evalating function, this list defines the ones that should be used to
     * popolate the {@link #outputFieldAttributes} list. The entries of this list are not guaranteed to be exactly equal to the
     * output fields returned by the evaluating function. In case of a mismatch, the missing fields will be populated with null values.
     * The {@link #outputFieldAttributes} entries ARE guaranteed to be equivalent to the keys of this list in order, type, and count.
     * Names in the {@link #outputFieldAttributes} list are also corresponding the keys of this list, but they are prefixed with a common
     * prefix.
     */
    private final List<String> outputFieldNames;

    /**
     * The output columns of this command. Fully corresponding to the entries of {@link #outputFieldNames} in order, types, and count.
     * Names are also corresponding, though not equivalent as they would have a common prefix added to them.
     */
    private final List<Attribute> outputFieldAttributes;

    public CompoundOutputEvalExec(
        Source source,
        PhysicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        super(source, child);
        this.input = input;
        this.outputFieldNames = outputFieldNames;
        this.outputFieldAttributes = outputFieldAttributes;
    }

    public CompoundOutputEvalExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readCollectionAsList(StreamInput::readString),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(input);
        out.writeStringCollection(outputFieldNames);
        out.writeNamedWriteableCollection(outputFieldAttributes);
    }

    /**
     * Creates a new instance of the specific {@link CompoundOutputEvalExec} subclass with the provided parameters.
     */
    public abstract CompoundOutputEvalExec createNewInstance(
        Source source,
        PhysicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFields
    );

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(outputFieldAttributes, child().output());
    }

    @Override
    protected AttributeSet computeReferences() {
        return input.references();
    }

    public Expression input() {
        return input;
    }

    public List<String> outputFieldNames() {
        return outputFieldNames;
    }

    public List<Attribute> outputFieldAttributes() {
        return outputFieldAttributes;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, outputFieldAttributes);
        return this;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return createNewInstance(source(), newChild, input, outputFieldNames, outputFieldAttributes);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, this::createNewInstance, child(), input, outputFieldNames, outputFieldAttributes);
    }

    protected abstract boolean innerEquals(CompoundOutputEvalExec other);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        CompoundOutputEvalExec that = (CompoundOutputEvalExec) o;
        return Objects.equals(input, that.input)
            && Objects.equals(outputFieldNames, that.outputFieldNames)
            && Objects.equals(outputFieldAttributes, that.outputFieldAttributes)
            && innerEquals(that);
    }

    protected abstract int innerHashCode();

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, outputFieldNames, outputFieldAttributes, innerHashCode());
    }
}
