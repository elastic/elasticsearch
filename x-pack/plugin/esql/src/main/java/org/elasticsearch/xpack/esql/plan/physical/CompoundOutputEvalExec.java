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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.CompoundOutputFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Abstract base class for physical plans that produce compound outputs from a single input.
 */
public abstract class CompoundOutputEvalExec extends UnaryExec implements EstimatesRowSize {

    /**
     * The input by which the evaluation is performed.
     */
    protected final Expression input;

    /**
     * An ordered map of the output fields expected by the {@link CompoundOutputFunction} that corresponds the concrete subclass.
     * From the fields that are actually returned by {@link CompoundOutputFunction#evaluate(String)}, this list defines the ones that
     * should be used to popolate the {@link #outputFields} list. The key-set of this map is not guaranteed to be exactly equal to the
     * output fields returned by the function. In case of a mismatch, the missing fields will be populated with null values.
     * The {@link #outputFields} entries ARE guaranteed to be equivalent to the keys of this map in order, type, and count. Names in the
     * {@link #outputFields} list are also corresponding the keys of this map, but they are prefixed with a common prefix.
     */
    private final Map<String, DataType> functionOutputFields;

    /**
     * The output columns of this command. Fully corresponding to the keys of {@link #functionOutputFields} in order, types, and count.
     * Names are also corresponding, though not equivalent as they would have a common prefix added to them.
     */
    private final List<Attribute> outputFields;

    protected final CompoundOutputFunction function;

    protected CompoundOutputEvalExec(
        Source source,
        PhysicalPlan child,
        Expression input,
        Map<String, DataType> functionOutputFields,
        List<Attribute> outputFields,
        CompoundOutputFunction function
    ) {
        super(source, child);
        if (functionOutputFields instanceof LinkedHashMap == false) {
            throw new IllegalArgumentException("functionOutputFields must be an ordered map");
        }
        if (functionOutputFields.size() != outputFields.size()) {
            throw new IllegalArgumentException("functionOutputFields and outputFields must have the same size");
        }
        this.input = input;
        this.functionOutputFields = functionOutputFields;
        this.outputFields = List.copyOf(outputFields);
        this.function = function;
    }

    protected CompoundOutputEvalExec(StreamInput in, CompoundOutputFunction function) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readOrderedMap(StreamInput::readString, i -> i.readEnum(DataType.class)),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            function
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(input);
        out.writeMap(functionOutputFields, StreamOutput::writeString, StreamOutput::writeEnum);
        out.writeNamedWriteableCollection(outputFields);
    }

    /**
     * Creates a new instance of the specific {@link CompoundOutputEvalExec} subclass with the provided parameters.
     */
    public abstract CompoundOutputEvalExec createNewInstance(
        Source source,
        PhysicalPlan child,
        Expression input,
        Map<String, DataType> functionOutputFields,
        List<Attribute> outputFields
    );

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(outputFields, child().output());
    }

    @Override
    protected AttributeSet computeReferences() {
        return input.references();
    }

    public Expression input() {
        return input;
    }

    public Map<String, DataType> getFunctionOutputFields() {
        return functionOutputFields;
    }

    public List<Attribute> outputFields() {
        return outputFields;
    }

    public CompoundOutputFunction function() {
        return function;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, outputFields);
        return this;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return createNewInstance(source(), newChild, input, functionOutputFields, outputFields);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, this::createNewInstance, child(), input, functionOutputFields, outputFields);
    }

    protected abstract boolean configOptionsEqual(CompoundOutputEvalExec other);

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
            && Objects.equals(functionOutputFields, that.functionOutputFields)
            && Objects.equals(outputFields, that.outputFields)
            && Objects.equals(function, that.function)
            && configOptionsEqual(that);
    }

    protected abstract int configOptionsHashCode();

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, functionOutputFields, outputFields, function, configOptionsHashCode());
    }
}
