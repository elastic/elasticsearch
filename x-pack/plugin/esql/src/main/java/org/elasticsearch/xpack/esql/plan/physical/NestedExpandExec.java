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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Physical form of {@link NestedExpand}: appends one column per child field and repeats each parent once per object. */
public final class NestedExpandExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "NestedExpandExec",
        NestedExpandExec::new
    );

    private final List<Attribute> metadata;
    private final String path;
    private final Set<String> indices;
    private final List<Attribute> expanded;
    private final List<String> paths;
    private final List<Set<String>> mappedIndices;

    public NestedExpandExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> metadata,
        String path,
        Set<String> indices,
        List<Attribute> expanded,
        List<String> paths,
        List<Set<String>> mappedIndices
    ) {
        super(source, child);
        this.metadata = List.copyOf(metadata);
        this.path = path;
        this.indices = Set.copyOf(indices);
        this.expanded = List.copyOf(expanded);
        this.paths = List.copyOf(paths);
        this.mappedIndices = mappedIndices.stream().map(Set::copyOf).toList();
    }

    private NestedExpandExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readString(),
            in.readCollectionAsImmutableSet(StreamInput::readString),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList(),
            in.readCollectionAsList(stream -> stream.readCollectionAsImmutableSet(StreamInput::readString))
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        NestedExpand.checkVersion(out);
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(metadata);
        out.writeString(path);
        out.writeStringCollection(indices);
        out.writeNamedWriteableCollection(expanded);
        out.writeStringCollection(paths);
        out.writeCollection(mappedIndices, StreamOutput::writeStringCollection);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public List<Attribute> metadata() {
        return metadata;
    }

    public String path() {
        return path;
    }

    public Set<String> indices() {
        return indices;
    }

    public List<Attribute> expanded() {
        return expanded;
    }

    public List<String> paths() {
        return paths;
    }

    public List<Set<String>> mappedIndices() {
        return mappedIndices;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(metadata);
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> output = new ArrayList<>(child().output());
        output.addAll(expanded);
        return output;
    }

    @Override
    protected NodeInfo<NestedExpandExec> info() {
        return NodeInfo.create(this, NestedExpandExec::new, child(), metadata, path, indices, expanded, paths, mappedIndices);
    }

    @Override
    public NestedExpandExec replaceChild(PhysicalPlan newChild) {
        return new NestedExpandExec(source(), newChild, metadata, path, indices, expanded, paths, mappedIndices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), metadata, path, indices, expanded, paths, mappedIndices);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NestedExpandExec other = (NestedExpandExec) obj;
        return child().equals(other.child())
            && metadata.equals(other.metadata)
            && path.equals(other.path)
            && indices.equals(other.indices)
            && expanded.equals(other.expanded)
            && paths.equals(other.paths)
            && mappedIndices.equals(other.mappedIndices);
    }
}
