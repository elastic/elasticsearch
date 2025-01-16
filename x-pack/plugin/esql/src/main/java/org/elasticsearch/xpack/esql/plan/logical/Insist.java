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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.InsistParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public class Insist extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "INSIST", Insist::new);

    private final InsistParameters parameters;

    public Insist(Source source, InsistParameters parameters, LogicalPlan child) {
        super(source, child);
        this.parameters = parameters;
    }

    private @Nullable List<Attribute> lazyOutput = null;

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = computeOutput();
        }
        return lazyOutput;
    }

    private List<Attribute> computeOutput() {
        var result = new ArrayList<>(child().output());
        OptionalInt index = CollectionUtils.findIndex(child().output(), c -> c.name().equals(parameters.identifier()));
        index.ifPresentOrElse(i -> {
            var field = ((FieldAttribute) child().output().get(i)).field();
            result.set(i, new FieldAttribute(source(), parameters.identifier(), PotentiallyUnmappedEsField.fromField(field)));
        },
            () -> result.add(
                new FieldAttribute(source(), parameters.identifier(), PotentiallyUnmappedEsField.fromStandalone(parameters.identifier()))
            )
        );
        return result;
    }

    public InsistParameters parameters() {
        return parameters;
    }

    @Override
    public Insist replaceChild(LogicalPlan newChild) {
        return new Insist(source(), parameters, newChild);
    }

    @Override
    public String commandName() {
        return "INSIST";
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Insist::new, parameters(), child());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeWriteable(parameters());
        out.writeNamedWriteable(child());
    }

    private Insist(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), InsistParameters.readFrom(in), in.readNamedWriteable(LogicalPlan.class));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + 31 * parameters.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && ((Insist) obj).parameters.equals(parameters);
    }
}
