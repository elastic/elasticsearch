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
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.NestedFlatten;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Physical form of {@link NestedFlatten}: appends one multivalued column per child field, one position per parent.
 */
public final class NestedFlattenExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "NestedFlattenExec",
        NestedFlattenExec::new
    );

    private final String path;
    private final List<Attribute> expanded;
    private final List<String> paths;

    public NestedFlattenExec(Source source, PhysicalPlan child, String path, List<Attribute> expanded, List<String> paths) {
        super(source, child);
        this.path = path;
        this.expanded = List.copyOf(expanded);
        this.paths = List.copyOf(paths);
    }

    private NestedFlattenExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readString(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        NestedFlatten.checkVersion(out);
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(path);
        out.writeNamedWriteableCollection(expanded);
        out.writeStringCollection(paths);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String path() {
        return path;
    }

    public List<Attribute> expanded() {
        return expanded;
    }

    public List<String> paths() {
        return paths;
    }

    @Override
    protected AttributeSet computeReferences() {
        // The operator reads child documents through the parent _doc, which is not one of its output columns.
        return NestedExpand.docAttributes(child().output());
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(expanded, child().output());
    }

    @Override
    protected NodeInfo<NestedFlattenExec> info() {
        return NodeInfo.create(this, NestedFlattenExec::new, child(), path, expanded, paths);
    }

    @Override
    public NestedFlattenExec replaceChild(PhysicalPlan newChild) {
        return new NestedFlattenExec(source(), newChild, path, expanded, paths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), path, expanded, paths);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NestedFlattenExec other = (NestedFlattenExec) obj;
        return child().equals(other.child()) && path.equals(other.path) && expanded.equals(other.expanded) && paths.equals(other.paths);
    }
}
