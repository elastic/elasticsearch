/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Base class for logical plan nodes that make a single evaluation on a single input expression and produce multiple output columns.
 * <p>
 * <b>NOTE:</b> The construction of the initial instance of the {@link CompoundOutputEval} subclass computes the output attributes based on
 * the specific evaluator's output columns and the provided prefix. Therefore, it should be used only when the node is first created.
 * In order to ensure this, there is no constructor that makes this computation directly. Instead, the initial instance creation should be
 * done through a static method like {@code createInitialInstance(...)} that makes use of the static {@link #computeOutputAttributes}
 * method.
 * Any subsequent instance construction, such as deserialization, regeneration with new names, or child replacement, should use the
 * constructor that directly accepts the output fields.
 */
public abstract class CompoundOutputEval<T extends CompoundOutputEval<T>> extends UnaryPlan
    implements
        TelemetryAware,
        GeneratingPlan<CompoundOutputEval<T>>,
        PostAnalysisVerificationAware,
        Streaming {

    /**
     * The input by which the evaluation is performed.
     */
    protected final Expression input;

    /**
     * A list of the output field names expected by the evaluation function that corresponds to the concrete subclass.
     * The output field names are NOT equivalent to the names of the corresponding output attributes, which would have a common prefix
     * added to them.
     * See {@link #computeOutputAttributes} for the conversion from function output fields to output attributes.
     * We must keep the original list by which the output fields are computed to propagate it so to ensure they are fully in sync, even if
     * the eventual computation is executed on a data node different from the one where the plan is created.
     */
    private final List<String> outputFieldNames;

    /**
     * The output columns of this command. Fully corresponding to the attributes in the {@link #outputFieldNames} list in order and count.
     * Names are also corresponding, though not equivalent as they would have a common prefix added to them.
     * See {@link #computeOutputAttributes} for the conversion from function output fields to output attributes.
     */
    private final List<Attribute> outputFieldAttributes;

    /**
     * This constructor directly accepts the output fields. It should be used for deserialization, regeneration with new names,
     * child replacement, or other scenarios where the output fields are already known.
     *
     * @param source                    the source information
     * @param child                     the child logical plan
     * @param input                     the input expression
     * @param outputFieldAttributes     the output attributes
     */
    protected CompoundOutputEval(
        Source source,
        LogicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        super(source, child);
        if (outputFieldNames.size() != outputFieldAttributes.size()) {
            throw new IllegalArgumentException("functionOutputFields and outputFields must have the same size");
        }
        this.input = input;
        this.outputFieldNames = outputFieldNames;
        this.outputFieldAttributes = outputFieldAttributes;
    }

    /**
     * This constructor is used for the deserialization of a {@link CompoundOutputEval} instance from a {@link StreamInput}.
     * Subclasses should call this constructor from their own deserialization constructor.
     *
     * @param in the input stream to read from
     * @throws IOException if an I/O error occurs
     */
    protected CompoundOutputEval(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
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
     * Computes the output attributes based on the provided output columns and prefix.
     *
     * @param outputColumns         the output columns by which the output attributes should be named and typed
     * @param outputFieldPrefix     the prefix to be used for the output field names
     * @param source                the source information for the attributes
     * @return a list of computed output attributes
     */
    protected static List<Attribute> computeOutputAttributes(
        final LinkedHashMap<String, Class<?>> outputColumns,
        final String outputFieldPrefix,
        final Source source
    ) {
        return outputColumns.entrySet()
            .stream()
            .map(
                entry -> (Attribute) new ReferenceAttribute(
                    source,
                    null,
                    outputFieldPrefix + "." + entry.getKey(),
                    DataType.fromJavaType(entry.getValue()),
                    Nullability.TRUE,
                    null,
                    false
                )
            )
            .toList();
    }

    /**
     * Creates a new instance of the specific {@link CompoundOutputEval} subclass with the provided parameters.
     * Subclasses should call their corresponding constructor with the provided arguments and the concrete evaluator instance.
     *
     * @param source                    the source information
     * @param child                     the child logical plan
     * @param input                     the input expression
     * @param outputFieldNames          the output field names, used for the computation of the output attributes
     * @param outputFieldAttributes     the output attributes
     * @return a new instance of the specific {@link CompoundOutputEval} subclass
     */
    public abstract T createNewInstance(
        Source source,
        LogicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    );

    public Expression getInput() {
        return input;
    }

    public List<String> outputFieldNames() {
        return outputFieldNames;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return outputFieldAttributes;
    }

    /**
     * By explicitly returning the references of the {@link #input} expression, we implicitly exclude the generated fields from the
     * references that require resolution.
     * @return only the input expression references
     */
    @Override
    protected AttributeSet computeReferences() {
        return input.references();
    }

    @Override
    public T withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);

        List<Attribute> renamedFields = new ArrayList<>(newNames.size());
        for (int i = 0; i < newNames.size(); i++) {
            Attribute oldAttribute = outputFieldAttributes.get(i);
            String newName = newNames.get(i);
            if (oldAttribute.name().equals(newName)) {
                renamedFields.add(oldAttribute);
            } else {
                renamedFields.add(oldAttribute.withName(newName).withId(new NameId()));
            }
        }

        return createNewInstance(source(), child(), input, outputFieldNames, renamedFields);
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(generatedAttributes(), child().output());
    }

    @Override
    public T replaceChild(LogicalPlan newChild) {
        return createNewInstance(source(), newChild, input, outputFieldNames, outputFieldAttributes);
    }

    @Override
    public boolean expressionsResolved() {
        return input.resolved();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, this::createNewInstance, child(), input, outputFieldNames, outputFieldAttributes);
    }

    protected abstract int innerHashCode();

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, innerHashCode(), outputFieldNames, outputFieldAttributes, getClass());
    }

    protected abstract boolean innerEquals(CompoundOutputEval<?> other);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (super.equals(obj) == false) {
            return false;
        }
        CompoundOutputEval<?> other = (CompoundOutputEval<?>) obj;
        return Objects.equals(input, other.input)
            && Objects.equals(outputFieldNames, other.outputFieldNames)
            && Objects.equals(outputFieldAttributes, other.outputFieldAttributes)
            && Objects.equals(this.getClass(), other.getClass())
            && innerEquals(other);
    }
}
