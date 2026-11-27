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
import org.elasticsearch.xpack.esql.analysis.ColumnDescription;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@code DESCRIBE} returns one row per column of its input: what the column is, where it comes from,
 * and what the mappings say about it.
 * <p>
 * Analysis computes the {@link LocalRelation} that describes its input and replaces the node with it once
 * the whole query is verified, so the input is verified like any other query. It reads no data, so it costs
 * a field caps call and works after any command. Unlike {@code LIMIT 0}, the result is a table that the rest of
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
        new Column("interval", DataType.KEYWORD),
        new Column("time_zone", DataType.KEYWORD),
        new Column("description", DataType.KEYWORD)
    );

    private record Column(String name, DataType type) {}

    private final List<Attribute> output;
    @Nullable
    private final LocalRelation table;

    public Describe(Source source, LogicalPlan child) {
        this(source, child, null);
    }

    public Describe(Source source, LogicalPlan child, @Nullable LocalRelation table) {
        this(
            source,
            child,
            table != null
                ? table.output()
                : COLUMNS.stream().map(c -> (Attribute) new ReferenceAttribute(source, null, c.name(), c.type())).toList(),
            table
        );
    }

    private Describe(Source source, LogicalPlan child, List<Attribute> output, @Nullable LocalRelation table) {
        super(source, child);
        this.output = output;
        this.table = table;
    }

    /**
     * The table that describes the child, once analysis computed it.
     */
    @Nullable
    public LocalRelation table() {
        return table;
    }

    public Describe withTable(LocalRelation newTable) {
        return new Describe(source(), child(), output, newTable);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public Describe replaceChild(LogicalPlan newChild) {
        return new Describe(source(), newChild, output, table);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<Describe> info() {
        return NodeInfo.create(this, Describe::new, child(), table);
    }

    @Override
    public String telemetryLabel() {
        return "DESCRIBE";
    }

    /**
     * Describes the resolved child as a local table.
     *
     * @param descriptions the description of every column of the child, keyed by attribute id
     */
    public LocalRelation describe(Map<NameId, ColumnDescription> descriptions) {
        List<Attribute> columns = child().output().stream().filter(a -> a.synthetic() == false).toList();
        ElementType[] elementTypes = new ElementType[COLUMNS.size()];
        Block.Builder[] builders = new Block.Builder[COLUMNS.size()];
        try {
            for (int i = 0; i < builders.length; i++) {
                elementTypes[i] = COLUMNS.get(i).type() == DataType.BOOLEAN ? ElementType.BOOLEAN : ElementType.BYTES_REF;
                builders[i] = elementTypes[i].newBlockBuilder(columns.size(), PlannerUtils.NON_BREAKING_BLOCK_FACTORY);
            }
            for (Attribute column : columns) {
                ColumnDescription description = descriptions.get(column.id());
                Object[] row = new Object[] {
                    column.name(),
                    column.dataType().outputType(),
                    description.role() == null ? null : description.role().label(),
                    description.field(),
                    description.origin(),
                    description.metricType(),
                    description.units(),
                    description.interval(),
                    description.timeZone(),
                    description.descriptions() };
                for (int i = 0; i < row.length; i++) {
                    append(builders[i], row[i], elementTypes[i]);
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

    /**
     * Appends a value, or a list of values as one multi-valued position.
     */
    private static void append(Block.Builder builder, @Nullable Object value, ElementType type) {
        if (value instanceof List<?> values && values.size() > 1) {
            builder.beginPositionEntry();
            for (Object v : values) {
                BlockUtils.appendValue(builder, toBytesRef(v), type);
            }
            builder.endPositionEntry();
        } else if (value instanceof List<?> values) {
            BlockUtils.appendValue(builder, values.isEmpty() ? null : toBytesRef(values.get(0)), type);
        } else {
            BlockUtils.appendValue(builder, toBytesRef(value), type);
        }
    }

    private static Object toBytesRef(@Nullable Object value) {
        return value instanceof String s ? new BytesRef(s) : value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(table, ((Describe) obj).table);
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
