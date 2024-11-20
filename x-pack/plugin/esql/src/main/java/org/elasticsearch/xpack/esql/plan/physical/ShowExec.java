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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ShowExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ShowExec",
        ShowExec::new
    );

    private final List<Attribute> attributes;
    private final List<List<Object>> values;

    public ShowExec(Source source, List<Attribute> attributes, List<List<Object>> values) {
        super(source);
        this.attributes = attributes;
        this.values = values;
    }

    @SuppressWarnings("unchecked")
    private ShowExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            (List<List<Object>>) in.readGenericValue()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(output());
        out.writeGenericValue(values());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ShowExec::new, attributes, values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes, values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ShowExec other && Objects.equals(attributes, other.attributes) && Objects.equals(values, other.values);
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    public List<List<Object>> values() {
        return values;
    }
}
