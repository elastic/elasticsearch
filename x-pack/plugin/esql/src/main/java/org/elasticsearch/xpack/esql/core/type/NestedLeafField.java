/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A column named like a leaf under a nested object. It holds every child value of one parent, so it is not single-valued.
 * The values are read from the child documents by {@code NestedChildren}; a parent-document loader would see none of them.
 */
public final class NestedLeafField extends EsField {
    public static final TransportVersion VERSION = TransportVersion.fromName("esql_nested_flattened_leaves");

    private final String path;
    private final String relative;
    private final EsField leaf;

    public NestedLeafField(EsField leaf, String path, String relative) {
        super(
            leaf.getName(),
            leaf.getDataType(),
            Map.of(),
            leaf.isAggregatable(),
            leaf.isAlias(),
            leaf.getTimeSeriesFieldType(),
            leaf.isPassthrough(),
            false
        );
        this.leaf = leaf;
        this.path = path;
        this.relative = relative;
    }

    private NestedLeafField(EsField leaf, String path, String relative, DataType type) {
        super(
            leaf.getName(),
            type,
            Map.of(),
            leaf.isAggregatable(),
            leaf.isAlias(),
            leaf.getTimeSeriesFieldType(),
            leaf.isPassthrough(),
            false
        );
        this.leaf = leaf;
        this.path = path;
        this.relative = relative;
    }

    public NestedLeafField(StreamInput in) throws IOException {
        this(read(in));
    }

    private NestedLeafField(Serialized serialized) {
        this(serialized.leaf, serialized.path, serialized.relative);
    }

    private record Serialized(EsField leaf, String path, String relative) {}

    private static Serialized read(StreamInput in) throws IOException {
        EsField leaf = EsField.readFrom(in);
        PlanStreamInput plan = (PlanStreamInput) in;
        return new Serialized(leaf, plan.readCachedString(), plan.readCachedString());
    }

    /** The nested collection whose child documents hold the values. */
    public String path() {
        return path;
    }

    /** The field's path inside {@link #path()}. */
    public String relative() {
        return relative;
    }

    /** The field ordinary resolution would make from the same occurrences. */
    public EsField leaf() {
        return leaf;
    }

    public static boolean isLeaf(FieldAttribute attribute) {
        return attribute.field() instanceof NestedLeafField;
    }

    @Override
    public EsField withProperties(Map<String, EsField> newProperties) {
        return this;
    }

    @Override
    public EsField withDataType(DataType type) {
        return type == getDataType() ? this : new NestedLeafField(leaf, path, relative, type);
    }

    @Override
    public Exact getExactInfo() {
        // The values live on the child documents. A query against the parent index would see none of them.
        return new Exact(false, "nested field [" + getName() + "] is read from its child documents");
    }

    @Override
    public EsField getExactField() {
        return this;
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        leaf.writeTo(out);
        PlanStreamOutput plan = (PlanStreamOutput) out;
        plan.writeCachedString(path);
        plan.writeCachedString(relative);
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "NestedLeafField";
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && Objects.equals(path, ((NestedLeafField) obj).path)
            && Objects.equals(relative, ((NestedLeafField) obj).relative)
            && Objects.equals(leaf, ((NestedLeafField) obj).leaf);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, relative, leaf);
    }
}
