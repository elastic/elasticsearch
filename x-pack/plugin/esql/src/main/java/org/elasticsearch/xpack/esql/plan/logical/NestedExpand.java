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

/**
 * Turns each parent row into one row per object of a mapped nested field, repeating the parent columns.
 * A parent without objects keeps one row with null child columns, as MV_EXPAND does. The parser only knows the
 * field name; analysis binds the private source metadata, the mapped indices and one column per child field.
 * With {@link #flatten()} ({@code NESTED_FLATTEN}) each parent keeps one row instead, and each child column holds the
 * values of all of its objects.
 */
public final class NestedExpand extends UnaryPlan implements SortAgnostic {
    /** Ships with NESTED in the same unreleased change, so it shares that transport version. */
    public static final TransportVersion VERSION = NestedReduction.VERSION;
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "NestedExpand",
        NestedExpand::new
    );

    private final boolean flatten;
    @Nullable
    private final NamedExpression target;
    @Nullable
    private final String path;
    private final List<Attribute> expanded;
    private final List<String> paths;

    private List<Attribute> output;

    /** The parsed, unbound command. */
    public NestedExpand(Source source, LogicalPlan child, boolean flatten, NamedExpression target) {
        this(source, child, flatten, target, null, List.of(), List.of());
    }

    public NestedExpand(
        Source source,
        LogicalPlan child,
        boolean flatten,
        @Nullable NamedExpression target,
        @Nullable String path,
        List<Attribute> expanded,
        List<String> paths
    ) {
        super(source, child);
        this.flatten = flatten;
        this.target = target;
        this.path = path;
        this.expanded = List.copyOf(expanded);
        this.paths = List.copyOf(paths);
    }

    private NestedExpand(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readBoolean(),
            null,
            in.readString(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList()
        );
    }

    /** Reject older peers before writing the expansion. */
    public static void checkVersion(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(VERSION) == false) {
            throw new IOException("NESTED_EXPAND and NESTED_FLATTEN require nested expansion transport support");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkVersion(out);
        if (bound() == false) {
            throw new IllegalStateException(commandName() + " must be bound before serialization");
        }
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeBoolean(flatten);
        out.writeString(path);
        out.writeNamedWriteableCollection(expanded);
        out.writeStringCollection(paths);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    /** One row per parent with multivalued child columns, rather than one row per object. */
    public boolean flatten() {
        return flatten;
    }

    public String commandName() {
        return commandName(flatten);
    }

    public static String commandName(boolean flatten) {
        return flatten ? "NESTED_FLATTEN" : "NESTED_EXPAND";
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

    public String path() {
        return path;
    }

    /** One column per child field, named by its full path. */
    public List<Attribute> expanded() {
        return expanded;
    }

    /** Each child field's path relative to the nested object. */
    public List<String> paths() {
        return paths;
    }

    @Override
    public boolean expressionsResolved() {
        return bound();
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
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
        return new NestedExpand(source(), newChild, flatten, target, path, expanded, paths);
    }

    @Override
    protected NodeInfo<NestedExpand> info() {
        return NodeInfo.create(this, NestedExpand::new, child(), flatten, target, path, expanded, paths);
    }

    @Override
    public boolean skipTelemetry() {
        // Unadvertised. A FeatureMetric would change the _xpack/usage shape; add one before advertising the command.
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), flatten, target, path, expanded, paths);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        NestedExpand other = (NestedExpand) obj;
        return flatten == other.flatten
            && Objects.equals(target, other.target)
            && Objects.equals(path, other.path)
            && expanded.equals(other.expanded)
            && paths.equals(other.paths);
    }
}
