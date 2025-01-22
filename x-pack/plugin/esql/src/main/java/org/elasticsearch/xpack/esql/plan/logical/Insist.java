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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

public class Insist extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "INSIST", Insist::new);

    private final String insistIdentifier;

    public Insist(Source source, String insistIdentifier, LogicalPlan child) {
        super(source, child);
        this.insistIdentifier = insistIdentifier;
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
        OptionalInt index = CollectionUtils.findIndex(child().output(), c -> c.name().equals(insistIdentifier));
        index.ifPresentOrElse(i -> {
            var field = ((FieldAttribute) child().output().get(i)).field();
            result.set(i, new FieldAttribute(source(), insistIdentifier, PotentiallyUnmappedEsField.fromField(field)));
        }, () -> result.add(new FieldAttribute(source(), insistIdentifier, PotentiallyUnmappedEsField.fromStandalone(insistIdentifier))));
        return result;
    }

    public String getInsistIdentifier() {
        return insistIdentifier;
    }

    @Override
    public Insist replaceChild(LogicalPlan newChild) {
        return new Insist(source(), insistIdentifier, newChild);
    }

    @Override
    public String commandName() {
        return "INSIST";
    }

    @Override
    public boolean expressionsResolved() {
        return computeOutput().stream().allMatch(Attribute::resolved);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Insist::new, insistIdentifier, child());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(insistIdentifier);
        out.writeNamedWriteable(child());
    }

    private Insist(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readString(), in.readNamedWriteable(LogicalPlan.class));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), Objects.hashCode(insistIdentifier));
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && ((Insist) obj).insistIdentifier.equals(insistIdentifier);
    }
}
