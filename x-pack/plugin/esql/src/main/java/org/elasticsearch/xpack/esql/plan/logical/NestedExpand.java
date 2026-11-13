/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Turns each parent row into one row per object of a mapped nested field, repeating the parent columns.
 * A parent without objects keeps one row with null child columns, as MV_EXPAND does. The parser only knows the
 * field name; analysis binds the private source metadata, the mapped indices and one column per child field.
 */
public final class NestedExpand extends UnaryPlan implements SortAgnostic {
    /** Ships with NESTED in the same unreleased change, so it shares that transport version. */
    public static final TransportVersion VERSION = NestedReduction.VERSION;
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "NestedExpand",
        NestedExpand::new
    );

    @Nullable
    private final NamedExpression target;
    private final List<Attribute> metadata;
    @Nullable
    private final String path;
    private final Set<String> indices;
    private final List<Attribute> expanded;
    private final List<String> paths;
    private final List<Set<String>> mappedIndices;

    private List<Attribute> output;

    /** The parsed, unbound command. */
    public NestedExpand(Source source, LogicalPlan child, NamedExpression target) {
        this(source, child, target, List.of(), null, Set.of(), List.of(), List.of(), List.of());
    }

    public NestedExpand(
        Source source,
        LogicalPlan child,
        @Nullable NamedExpression target,
        List<Attribute> metadata,
        @Nullable String path,
        Set<String> indices,
        List<Attribute> expanded,
        List<String> paths,
        List<Set<String>> mappedIndices
    ) {
        super(source, child);
        this.target = target;
        this.metadata = List.copyOf(metadata);
        this.path = path;
        this.indices = Set.copyOf(indices);
        this.expanded = List.copyOf(expanded);
        this.paths = List.copyOf(paths);
        this.mappedIndices = mappedIndices.stream().map(Set::copyOf).toList();
    }

    private NestedExpand(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            null,
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readString(),
            in.readCollectionAsImmutableSet(StreamInput::readString),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList(),
            in.readCollectionAsList(stream -> stream.readCollectionAsImmutableSet(StreamInput::readString))
        );
    }

    /** Reject older peers before writing the expansion. */
    public static void checkVersion(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(VERSION) == false) {
            throw new IOException("NESTED_EXPAND requires nested expansion transport support");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkVersion(out);
        if (bound() == false) {
            throw new IllegalStateException("NESTED_EXPAND must be bound before serialization");
        }
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

    public boolean bound() {
        return path != null;
    }

    /** The field the user named; null once bound. */
    @Nullable
    public NamedExpression target() {
        return target;
    }

    /** The nested path the user named, before or after binding. */
    public String fieldName() {
        return bound() ? path : target.name();
    }

    /** Private {@code _source} and {@code _index} of the parent row. */
    public List<Attribute> metadata() {
        return metadata;
    }

    public String path() {
        return path;
    }

    /** Indices where {@link #path()} is a mapped nested field. */
    public Set<String> indices() {
        return indices;
    }

    /** One column per child field, named by its full path. */
    public List<Attribute> expanded() {
        return expanded;
    }

    /** Each child field's path relative to the nested object. */
    public List<String> paths() {
        return paths;
    }

    /** Indices where each child field is mapped. */
    public List<Set<String>> mappedIndices() {
        return mappedIndices;
    }

    @Override
    public boolean expressionsResolved() {
        return bound();
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(metadata);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            List<Attribute> result = new ArrayList<>();
            for (Attribute attribute : child().output()) {
                // The expanded collection stops being a nested scope: its children are ordinary columns now.
                if (bound() == false || expandedScope(attribute) == false) {
                    result.add(attribute);
                }
            }
            result.addAll(expanded);
            output = result;
        }
        return output;
    }

    private boolean expandedScope(Attribute attribute) {
        return attribute instanceof FieldAttribute field
            && field.field() instanceof NestedScopeField scope
            && (scope.path().equals(path) || scope.path().startsWith(path + "."));
    }

    @Override
    public NestedExpand replaceChild(LogicalPlan newChild) {
        return new NestedExpand(source(), newChild, target, metadata, path, indices, expanded, paths, mappedIndices);
    }

    @Override
    protected NodeInfo<NestedExpand> info() {
        return NodeInfo.create(this, NestedExpand::new, child(), target, metadata, path, indices, expanded, paths, mappedIndices);
    }

    @Override
    public boolean skipTelemetry() {
        // Unadvertised. A FeatureMetric would change the _xpack/usage shape; add one before advertising the command.
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), target, metadata, path, indices, expanded, paths, mappedIndices);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        NestedExpand other = (NestedExpand) obj;
        return Objects.equals(target, other.target)
            && metadata.equals(other.metadata)
            && Objects.equals(path, other.path)
            && indices.equals(other.indices)
            && expanded.equals(other.expanded)
            && paths.equals(other.paths)
            && mappedIndices.equals(other.mappedIndices);
    }
}
