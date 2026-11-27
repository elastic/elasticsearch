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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.NestedContainerField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Physical form of {@link NestedExpand}: appends one column per child field and repeats each parent once per object.
 */
public final class NestedExpandExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "NestedExpandExec",
        NestedExpandExec::new
    );

    private final String path;
    private final List<Attribute> expanded;
    private final List<String> paths;
    @Nullable
    private final Attribute container;

    public NestedExpandExec(Source source, PhysicalPlan child, String path, List<Attribute> expanded, List<String> paths) {
        this(source, child, path, expanded, paths, null);
    }

    /** {@code container} carries the child documents when they are not read through the row's {@code _doc}. */
    public NestedExpandExec(
        Source source,
        PhysicalPlan child,
        String path,
        List<Attribute> expanded,
        List<String> paths,
        @Nullable Attribute container
    ) {
        super(source, child);
        this.path = path;
        this.expanded = List.copyOf(expanded);
        this.paths = List.copyOf(paths);
        this.container = container;
    }

    private NestedExpandExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readString(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList(),
            in.getTransportVersion().supports(NestedContainerField.VERSION) ? in.readOptionalNamedWriteable(Attribute.class) : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        NestedExpand.checkVersion(out);
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(path);
        out.writeNamedWriteableCollection(expanded);
        out.writeStringCollection(paths);
        if (out.getTransportVersion().supports(NestedContainerField.VERSION)) {
            out.writeOptionalNamedWriteable(container);
        } else if (container != null) {
            throw new IOException("NESTED_EXPAND over a lookup index requires nested container transport support");
        }
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

    /** The column that carries the child documents, or {@code null} when they are read through the row's {@code _doc}. */
    @Nullable
    public Attribute container() {
        return container;
    }

    @Override
    protected AttributeSet computeReferences() {
        // The operator reads child documents through the parent _doc, which is not one of its output columns.
        return container != null ? AttributeSet.of(container) : NestedExpand.docAttributes(child().output());
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> input = container == null
            ? child().output()
            : child().output().stream().filter(attribute -> attribute.id().equals(container.id()) == false).toList();
        return mergeOutputAttributes(expanded, input);
    }

    @Override
    protected NodeInfo<NestedExpandExec> info() {
        return NodeInfo.create(this, NestedExpandExec::new, child(), path, expanded, paths, container);
    }

    @Override
    public NestedExpandExec replaceChild(PhysicalPlan newChild) {
        return new NestedExpandExec(source(), newChild, path, expanded, paths, container);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), path, expanded, paths, container);
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
            && path.equals(other.path)
            && expanded.equals(other.expanded)
            && paths.equals(other.paths)
            && Objects.equals(container, other.container);
    }
}
