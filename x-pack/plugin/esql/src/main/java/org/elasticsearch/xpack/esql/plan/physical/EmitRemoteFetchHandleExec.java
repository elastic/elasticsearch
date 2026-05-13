/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Replaces the local-only {@code _doc} column with a transport-safe remote fetch handle before the final exchange.
 */
public class EmitRemoteFetchHandleExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EmitRemoteFetchHandleExec",
        EmitRemoteFetchHandleExec::new
    );

    private final Attribute sourceAttribute;
    private final Attribute handleAttribute;
    private List<Attribute> lazyOutput;

    public EmitRemoteFetchHandleExec(Source source, PhysicalPlan child, Attribute sourceAttribute, Attribute handleAttribute) {
        super(source, child);
        this.sourceAttribute = sourceAttribute;
        this.handleAttribute = handleAttribute;
    }

    private EmitRemoteFetchHandleExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(sourceAttribute);
        out.writeNamedWriteable(handleAttribute);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<EmitRemoteFetchHandleExec> info() {
        return NodeInfo.create(this, EmitRemoteFetchHandleExec::new, child(), sourceAttribute, handleAttribute);
    }

    @Override
    public EmitRemoteFetchHandleExec replaceChild(PhysicalPlan newChild) {
        return new EmitRemoteFetchHandleExec(source(), newChild, sourceAttribute, handleAttribute);
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(sourceAttribute);
    }

    public Attribute sourceAttribute() {
        return sourceAttribute;
    }

    public Attribute handleAttribute() {
        return handleAttribute;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = new ArrayList<>(child().output().size());
            for (Attribute attribute : child().output()) {
                lazyOutput.add(attribute.equals(sourceAttribute) ? handleAttribute : attribute);
            }
        }
        return lazyOutput;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), sourceAttribute, handleAttribute);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EmitRemoteFetchHandleExec other = (EmitRemoteFetchHandleExec) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(sourceAttribute, other.sourceAttribute)
            && Objects.equals(handleAttribute, other.handleAttribute);
    }
}
