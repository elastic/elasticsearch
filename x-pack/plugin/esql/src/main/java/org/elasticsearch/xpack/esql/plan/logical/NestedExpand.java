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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Turns each parent row into one row per object of a mapped nested field, repeating the parent columns. A parent
 * without objects keeps one row with null child columns, as {@code MV_EXPAND} does. The parser only knows the field
 * name; analysis binds the nested path and one column per child field.
 */
public final class NestedExpand extends UnaryPlan implements SortAgnostic, DocPreserving {
    public static final TransportVersion VERSION = TransportVersion.fromName("esql_nested_expand");
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "NestedExpand",
        NestedExpand::new
    );

    @Nullable
    private final NamedExpression target;
    @Nullable
    private final String path;
    private final List<Attribute> expanded;
    private final List<String> paths;

    private volatile List<Attribute> output;

    /** The parsed, unbound command. */
    public NestedExpand(Source source, LogicalPlan child, NamedExpression target) {
        this(source, child, target, null, List.of(), List.of());
    }

    public NestedExpand(
        Source source,
        LogicalPlan child,
        @Nullable NamedExpression target,
        @Nullable String path,
        List<Attribute> expanded,
        List<String> paths
    ) {
        super(source, child);
        this.target = target;
        this.path = path;
        this.expanded = List.copyOf(expanded);
        this.paths = List.copyOf(paths);
    }

    private NestedExpand(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            null,
            in.readString(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList()
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
        out.writeString(path);
        out.writeNamedWriteableCollection(expanded);
        out.writeStringCollection(paths);
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
        return docAttributes(child().output());
    }

    /**
     * Whether {@code attribute} is one of the child columns a {@link NestedExpand} in {@code plan} produces.
     */
    public static boolean expands(LogicalPlan plan, Attribute attribute) {
        return plan.anyMatch(
            p -> p instanceof NestedExpand expand && expand.expanded().stream().anyMatch(a -> a.id().equals(attribute.id()))
        );
    }

    public static AttributeSet docAttributes(List<Attribute> attributes) {
        List<Attribute> docs = new ArrayList<>();
        for (Attribute attribute : attributes) {
            if (EsQueryExec.isDocAttribute(attribute)) {
                docs.add(attribute);
            }
        }
        return AttributeSet.of(docs);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            List<Attribute> result = new ArrayList<>();
            for (Attribute attribute : child().output()) {
                // The expanded collection's child fields leave the private nested schema as ordinary columns. Other
                // collections keep theirs, so a later NESTED_EXPAND can expand them.
                if (expandedScope(attribute) == false) {
                    result.add(attribute);
                }
            }
            output = mergeOutputAttributes(expanded, result);
        }
        return output;
    }

    private boolean expandedScope(Attribute attribute) {
        return attribute instanceof FieldAttribute field
            && field.field() instanceof NestedScopeField scope
            && (bound() == false || scope.path().equals(path) || scope.path().startsWith(path + "."));
    }

    @Override
    public NestedExpand replaceChild(LogicalPlan newChild) {
        return new NestedExpand(source(), newChild, target, path, expanded, paths);
    }

    @Override
    protected NodeInfo<NestedExpand> info() {
        return NodeInfo.create(this, NestedExpand::new, child(), target, path, expanded, paths);
    }

    @Override
    public boolean skipTelemetry() {
        // Unadvertised. A FeatureMetric would change the _xpack/usage shape; add one before advertising the command.
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), target, path, expanded, paths);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        NestedExpand other = (NestedExpand) obj;
        return Objects.equals(target, other.target)
            && Objects.equals(path, other.path)
            && expanded.equals(other.expanded)
            && paths.equals(other.paths);
    }
}
