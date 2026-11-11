/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.FieldSemantics;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@code DESCRIBE} returns one row per column of its input: what the column is, where it comes from,
 * and what the mappings say about it.
 * <p>
 * It is resolved during analysis into a {@link LocalRelation} and reads no data, so it costs a field
 * caps call and works after any command. Unlike {@code LIMIT 0}, the result is a table that the rest of
 * the query can filter, and it carries the semantics that field caps reports and ES|QL otherwise drops.
 * The node never escapes the coordinator.
 */
public class Describe extends UnaryPlan implements TelemetryAware {

    private static final List<Column> COLUMNS = List.of(
        new Column("name", DataType.KEYWORD),
        new Column("type", DataType.KEYWORD),
        new Column("role", DataType.KEYWORD),
        new Column("field", DataType.KEYWORD),
        new Column("origin", DataType.KEYWORD),
        new Column("metric_type", DataType.KEYWORD),
        new Column("unit", DataType.KEYWORD),
        new Column("description", DataType.KEYWORD)
    );

    private record Column(String name, DataType type) {}

    private final List<Attribute> output;

    public Describe(Source source, LogicalPlan child) {
        this(source, child, COLUMNS.stream().map(c -> (Attribute) new ReferenceAttribute(source, null, c.name(), c.type())).toList());
    }

    private Describe(Source source, LogicalPlan child, List<Attribute> output) {
        super(source, child);
        this.output = output;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public Describe replaceChild(LogicalPlan newChild) {
        return new Describe(source(), newChild, output);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<Describe> info() {
        return NodeInfo.create(this, Describe::new, child());
    }

    @Override
    public String telemetryLabel() {
        return "DESCRIBE";
    }

    /**
     * Describes the resolved child as a local table.
     *
     * @param semantics      the semantics of the mapped fields the child's columns read, keyed by attribute id
     * @param datasetColumns the dataset columns the child's columns read, keyed by attribute id
     * @param units          the unit of every column whose unit is known, keyed by attribute id
     */
    public LocalRelation describe(Map<NameId, FieldSemantics> semantics, Map<NameId, Attribute> datasetColumns, Map<NameId, String> units) {
        List<Attribute> columns = child().output().stream().filter(a -> a.synthetic() == false).toList();
        ElementType[] elementTypes = new ElementType[COLUMNS.size()];
        Block.Builder[] builders = new Block.Builder[COLUMNS.size()];
        try {
            for (int i = 0; i < builders.length; i++) {
                elementTypes[i] = COLUMNS.get(i).type() == DataType.BOOLEAN ? ElementType.BOOLEAN : ElementType.BYTES_REF;
                builders[i] = elementTypes[i].newBlockBuilder(columns.size(), PlannerUtils.NON_BREAKING_BLOCK_FACTORY);
            }
            for (Attribute column : columns) {
                FieldSemantics field = semantics.get(column.id());
                Attribute datasetColumn = datasetColumns.get(column.id());
                FieldSemantics.Role role = field != null ? field.role() : isMetadata(column) ? FieldSemantics.Role.METADATA : null;
                String metricType = field == null ? null : field.metricType();
                Object[] row = new Object[] {
                    column.name(),
                    column.dataType().outputType(),
                    role == null ? null : role.label(),
                    field != null ? field.field() : datasetColumn != null ? datasetColumn.name() : null,
                    origin(column, field, datasetColumn),
                    metricType,
                    units.get(column.id()),
                    field == null ? null : field.description() };
                for (int i = 0; i < row.length; i++) {
                    Object value = row[i] instanceof String s ? new BytesRef(s) : row[i];
                    BlockUtils.appendValue(builders[i], value, elementTypes[i]);
                }
            }
            Block[] blocks = new Block[builders.length];
            for (int i = 0; i < builders.length; i++) {
                blocks[i] = builders[i].build();
            }
            return new LocalRelation(source(), output, LocalSupplier.of(new Page(columns.size(), blocks)));
        } finally {
            Releasables.closeExpectNoException(builders);
        }
    }

    private static String origin(Attribute column, @Nullable FieldSemantics field, @Nullable Attribute datasetColumn) {
        if (isMetadata(column)) {
            return "metadata";
        }
        if (column instanceof UnsupportedAttribute) {
            return "unsupported";
        }
        if (column instanceof FieldAttribute) {
            return field == null ? "unmapped" : "mapped";
        }
        if (datasetColumn != null) {
            return datasetColumn.id().equals(column.id()) ? "dataset" : "renamed";
        }
        return field == null ? "computed" : "renamed";
    }

    private static boolean isMetadata(Attribute column) {
        return column instanceof MetadataAttribute || column instanceof ExternalMetadataAttribute;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

}
