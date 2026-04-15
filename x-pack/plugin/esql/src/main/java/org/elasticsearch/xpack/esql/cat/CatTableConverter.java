/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.cat;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Table;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts a {@link Table} (as built by {@link CatDataResolver}) into a columnar {@link Page}
 * suitable for use in a {@link org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation}.
 *
 * <p>{@link CatDataResolver} stores raw Java values in table cells — no pre-stringification.
 * This converter handles type coercion per column:
 * <ul>
 *   <li>{@code null} → ES|QL null in the block</li>
 *   <li>{@link String} → {@link BytesRef} for KEYWORD columns</li>
 *   <li>{@link Number} → narrowed/widened to the target primitive type</li>
 * </ul>
 */
public final class CatTableConverter {

    private CatTableConverter() {}

    /**
     * Infers the schema of {@code table} by inspecting the first non-null value in each column.
     *
     * <p>Type mapping: {@link Integer} → INTEGER, {@link Long} → LONG, {@link Double} → DOUBLE,
     * {@link Boolean} → BOOLEAN, anything else (or all-null column) → KEYWORD.
     *
     * @param table the table as built by {@link CatDataResolver}
     * @return ordered list of attributes matching the table's column order
     */
    public static List<Attribute> inferSchema(Table table) {
        Map<String, List<Table.Cell>> columnMap = table.getAsMap();
        List<Attribute> schema = new ArrayList<>(table.getHeaders().size());
        for (Table.Cell header : table.getHeaders()) {
            String name = header.value.toString();
            DataType type = inferType(columnMap.get(name));
            schema.add(new ReferenceAttribute(Source.EMPTY, name, type));
        }
        return schema;
    }

    private static DataType inferType(List<Table.Cell> cells) {
        if (cells == null) {
            return DataType.KEYWORD;
        }
        for (Table.Cell cell : cells) {
            if (cell.value == null) {
                continue;
            }
            return switch (cell.value) {
                case Integer ignored -> DataType.INTEGER;
                case Long ignored -> DataType.LONG;
                case Double ignored -> DataType.DOUBLE;
                case Boolean ignored -> DataType.BOOLEAN;
                default -> DataType.KEYWORD;
            };
        }
        return DataType.KEYWORD; // all-null or empty column
    }

    /**
     * Converts {@code table} to a {@link Page} whose blocks are ordered to match {@code schema}.
     *
     * @param table  the data returned by a CAT resolver; may be empty (0 rows)
     * @param schema the attributes defining column names and types
     * @return a {@link Page} with one block per attribute
     */
    public static Page toPage(Table table, List<Attribute> schema) {
        int numRows = table.getRows().size();
        if (numRows == 0) {
            return new Page(0);
        }

        Map<String, List<Table.Cell>> columnMap = table.getAsMap();
        BlockFactory factory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[schema.size()];
        boolean success = false;
        try {
            for (int i = 0; i < schema.size(); i++) {
                Attribute attr = schema.get(i);
                List<Table.Cell> cells = columnMap.get(attr.name());
                blocks[i] = buildBlock(factory, cells, attr.dataType(), numRows);
            }
            success = true;
            return new Page(blocks);
        } finally {
            if (success == false) {
                for (Block b : blocks) {
                    if (b != null) b.close();
                }
            }
        }
    }

    private static Block buildBlock(BlockFactory factory, List<Table.Cell> cells, DataType dataType, int numRows) {
        return switch (dataType) {
            case KEYWORD, TEXT -> buildKeywordBlock(factory, cells, numRows);
            case INTEGER -> buildIntBlock(factory, cells, numRows);
            case LONG -> buildLongBlock(factory, cells, numRows);
            case DOUBLE -> buildDoubleBlock(factory, cells, numRows);
            case BOOLEAN -> buildBooleanBlock(factory, cells, numRows);
            default -> throw new IllegalArgumentException("Unsupported CAT column type: " + dataType);
        };
    }

    private static Block buildKeywordBlock(BlockFactory factory, List<Table.Cell> cells, int numRows) {
        try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(numRows)) {
            for (Table.Cell cell : cells) {
                if (cell.value == null) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(cell.value.toString()));
                }
            }
            return builder.build();
        }
    }

    private static Block buildIntBlock(BlockFactory factory, List<Table.Cell> cells, int numRows) {
        try (IntBlock.Builder builder = factory.newIntBlockBuilder(numRows)) {
            for (Table.Cell cell : cells) {
                if (cell.value == null) {
                    builder.appendNull();
                } else if (cell.value instanceof Number n) {
                    builder.appendInt(n.intValue());
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private static Block buildLongBlock(BlockFactory factory, List<Table.Cell> cells, int numRows) {
        try (LongBlock.Builder builder = factory.newLongBlockBuilder(numRows)) {
            for (Table.Cell cell : cells) {
                if (cell.value == null) {
                    builder.appendNull();
                } else if (cell.value instanceof Number n) {
                    builder.appendLong(n.longValue());
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private static Block buildDoubleBlock(BlockFactory factory, List<Table.Cell> cells, int numRows) {
        try (DoubleBlock.Builder builder = factory.newDoubleBlockBuilder(numRows)) {
            for (Table.Cell cell : cells) {
                if (cell.value == null) {
                    builder.appendNull();
                } else if (cell.value instanceof Number n) {
                    builder.appendDouble(n.doubleValue());
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private static Block buildBooleanBlock(BlockFactory factory, List<Table.Cell> cells, int numRows) {
        try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(numRows)) {
            for (Table.Cell cell : cells) {
                if (cell.value == null) {
                    builder.appendNull();
                } else if (cell.value instanceof Boolean b) {
                    builder.appendBoolean(b);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }
}
