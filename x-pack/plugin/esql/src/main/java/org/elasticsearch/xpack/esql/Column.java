/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;

/**
 * Represents a column from a table provided in an ESQL query request.
 * <p>
 * A column encapsulates both the data type and the actual values stored in a block.
 * This record is releasable and writeable for efficient memory management and serialization.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Creating a column with integer values
 * Block intBlock = BlockFactory.getNonBreakingInstance().newIntArrayVector(new int[]{1, 2, 3}).asBlock();
 * Column column = new Column(DataType.INTEGER, intBlock);
 *
 * // Accessing column properties
 * DataType type = column.type();
 * Block values = column.values();
 *
 * // Always close the column to release resources
 * try (Column col = column) {
 *     // Use the column
 * }
 * }</pre>
 *
 * @param type the data type of the column values
 * @param values the block containing the actual column values
 */
public record Column(DataType type, Block values) implements Releasable, Writeable {
    public Column {
        assert PlannerUtils.toElementType(type) == values.elementType();
    }

    /**
     * Constructs a column by deserializing from a block stream input.
     * <p>
     * This constructor reads the data type name and block values from the stream,
     * reconstructing the column from its serialized form.
     * </p>
     *
     * @param in the block stream input to read from
     * @throws IOException if an I/O error occurs during deserialization
     */
    public Column(BlockStreamInput in) throws IOException {
        this(DataType.fromTypeName(in.readString()), Block.readTypedBlock(in));
    }

    /**
     * Serializes this column to a stream output.
     * <p>
     * Writes the data type name followed by the block values in their typed format.
     * This method is used for network serialization and persistence.
     * </p>
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type.typeName());
        Block.writeTypedBlock(values, out);
    }

    /**
     * Releases the resources held by this column.
     * <p>
     * This method closes the underlying block values, freeing any native memory
     * or resources. It's critical to call this method (or use try-with-resources)
     * to prevent memory leaks.
     * </p>
     * <p>
     * This implementation expects no exceptions during closure. Any exceptions
     * that occur will be logged but not propagated.
     * </p>
     */
    @Override
    public void close() {
        Releasables.closeExpectNoException(values);
    }
}
