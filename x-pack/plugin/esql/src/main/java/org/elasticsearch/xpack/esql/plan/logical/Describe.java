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
 * what the mappings say about it, and how to use it in ES|QL.
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
        new Column("description", DataType.KEYWORD),
        new Column("searchable", DataType.BOOLEAN),
        new Column("aggregatable", DataType.BOOLEAN),
        new Column("usage", DataType.KEYWORD)
    );

    private record Column(String name, DataType type) {}

    /** Stands for the column in a {@code usage} hint. */
    static final String FIELD_PLACEHOLDER = "<field>";

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
     * @param semantics the semantics of the mapped fields the child's columns read, keyed by attribute id
     */
    public LocalRelation describe(Map<NameId, FieldSemantics> semantics) {
        // The output of a STATS is a table of answers, not a time series to aggregate per series again.
        boolean timeSeries = child().anyMatch(p -> p instanceof EsRelation r && r.indexMode().isTsdb())
            && child().anyMatch(p -> p instanceof Aggregate) == false;
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
                FieldSemantics.Role role = field != null ? field.role()
                    : column instanceof MetadataAttribute ? FieldSemantics.Role.METADATA
                    : null;
                String metricType = field == null ? null : field.metricType();
                Object[] row = new Object[] {
                    column.name(),
                    column.dataType().outputType(),
                    role == null ? null : role.label(),
                    field == null ? null : field.field(),
                    origin(column, field),
                    metricType,
                    field == null ? null : field.unit(),
                    field == null ? null : field.description(),
                    field == null ? null : field.searchable(),
                    field == null ? null : field.aggregatable(),
                    usage(role, metricType, timeSeries) };
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

    private static String origin(Attribute column, @Nullable FieldSemantics field) {
        if (column instanceof MetadataAttribute) {
            return "metadata";
        }
        if (column instanceof UnsupportedAttribute) {
            return "unsupported";
        }
        if (column instanceof FieldAttribute) {
            return field == null ? "unmapped" : "mapped";
        }
        return field == null ? "computed" : "renamed";
    }

    /**
     * How to use the column in ES|QL. This is the part of the output an agent acts on: it names the functions that are
     * correct for the column, not just what it is. There is a hint only when the mapping declares the role (a dimension,
     * a counter or a gauge, and the time of a time series) or the type settles it (text, vectors, metadata); a role
     * inferred from the type alone, such as a number that may be a measure or an identifier, gets none.
     * <p>
     * The hint names the column {@value #FIELD_PLACEHOLDER} rather than by its name, so it is the same for every
     * column of a kind and {@code DESCRIBE | STATS COUNT(*) BY role, metric_type, usage} summarizes how to query
     * a whole index in a few rows.
     */
    @Nullable
    static String usage(@Nullable FieldSemantics.Role role, @Nullable String metricType, boolean timeSeries) {
        String name = FIELD_PLACEHOLDER;
        if (role != null) {
            switch (role) {
                case TIME:
                    return timeSeries ? "BY TBUCKET(" + name + ", <interval>)" : null;
                case DIMENSION:
                    return "BY " + name + "; WHERE " + name + " == <value>; identifies a time series";
                case METRIC:
                    if ("counter".equals(metricType)) {
                        return timeSeries
                            ? "STATS SUM(RATE(" + name + ")) or SUM(INCREASE(" + name + ")); never SUM over time"
                            : "use TS, then RATE(" + name + ") or INCREASE(" + name + "); raw values are cumulative";
                    }
                    if ("gauge".equals(metricType)) {
                        return timeSeries
                            ? "STATS AVG(LAST_OVER_TIME(" + name + ")) or MAX(MAX_OVER_TIME(" + name + ")); never SUM over time"
                            : "STATS AVG(" + name + "), MIN, MAX; do not SUM across time";
                    }
                    return null;
                case TEXT:
                    return "WHERE MATCH(" + name + ", \"<text>\"); not groupable";
                case SEMANTIC:
                    return "WHERE MATCH(" + name + ", \"<natural language>\")";
                case VECTOR:
                    return "WHERE KNN(" + name + ", <vector>)";
                case METADATA:
                    return "FROM <index> METADATA " + name;
                case ATTRIBUTE, NUMERIC, OTHER, MIXED:
                    return null;
            }
        }
        return null;
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
