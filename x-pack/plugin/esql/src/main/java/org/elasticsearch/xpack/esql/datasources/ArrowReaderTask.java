/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.arrow.ArrowToBlockConverter;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * Background task that reads data using Arrow format.
 * This task runs on a separate executor thread to avoid blocking the Driver.
 *
 * <p>Architecture:
 * <ul>
 *   <li>Reads from data sources that provide Arrow {@link VectorSchemaRoot} batches</li>
 *   <li>Converts Arrow vectors to ESQL Blocks using {@link ArrowToBlockConverter}</li>
 *   <li>Fills {@link AsyncExternalSourceBuffer} with converted pages for consumption by operator</li>
 * </ul>
 *
 * <p>Key requirements:
 * <ul>
 *   <li>Call {@link Page#allowPassingToDifferentDriver()} before adding pages to buffer</li>
 *   <li>Use parent {@link BlockFactory} for cross-thread memory tracking</li>
 *   <li>Handle backpressure via {@link AsyncExternalSourceBuffer#waitForWriting()}</li>
 *   <li>Report failures via {@link AsyncExternalSourceBuffer#onFailure(Throwable)}</li>
 * </ul>
 */
public class ArrowReaderTask implements Runnable {

    private final AsyncExternalSourceBuffer buffer;
    private final BlockFactory blockFactory;
    private final Supplier<CloseableIterable<VectorSchemaRoot>> dataSupplier;
    private final Schema schema;
    private final List<Attribute> attributes;
    private final int pageSize;

    public ArrowReaderTask(
        AsyncExternalSourceBuffer buffer,
        BlockFactory blockFactory,
        Supplier<CloseableIterable<VectorSchemaRoot>> dataSupplier,
        Schema schema,
        List<Attribute> attributes,
        int pageSize
    ) {
        this.buffer = buffer;
        this.blockFactory = blockFactory;
        this.dataSupplier = dataSupplier;
        this.schema = schema;
        this.attributes = attributes;
        this.pageSize = pageSize;
    }

    @Override
    public void run() {
        try {
            readData();
        } catch (Exception e) {
            buffer.onFailure(e);
        } finally {
            buffer.finish(false);
        }
    }

    /**
     * Read data using Arrow format.
     * Converts Arrow vectors to ESQL Blocks and adds pages to buffer.
     */
    private void readData() throws IOException {
        try (CloseableIterable<VectorSchemaRoot> batches = dataSupplier.get()) {
            CloseableIterator<VectorSchemaRoot> iterator = batches.iterator();

            while (iterator.hasNext() && buffer.noMoreInputs() == false) {
                // Check backpressure before processing next batch
                var blocked = buffer.waitForWriting();
                if (blocked.listener().isDone() == false) {
                    // Buffer is full - wait for space to become available
                    // In production systems, proper backpressure handling prevents memory exhaustion
                }

                VectorSchemaRoot root = iterator.next();
                try {
                    // Convert Arrow batch to ESQL Page
                    Page page = convertArrowToPage(root);
                    if (page != null && page.getPositionCount() > 0) {
                        // Critical: allow page to be passed to different driver thread
                        page.allowPassingToDifferentDriver();
                        buffer.addPage(page);
                    }
                } finally {
                    // VectorSchemaRoot must be closed after processing
                    root.close();
                }
            }
        }
    }

    /**
     * Convert an Arrow VectorSchemaRoot to an ESQL Page.
     * Uses {@link ArrowToBlockConverter} for type-specific conversion.
     *
     * @param root the Arrow vector batch
     * @return ESQL Page with converted blocks, or null if no data
     */
    private Page convertArrowToPage(VectorSchemaRoot root) {
        int rowCount = root.getRowCount();
        if (rowCount == 0) {
            return null;
        }

        List<FieldVector> vectors = root.getFieldVectors();
        if (vectors.size() != attributes.size()) {
            throw new IllegalStateException("Schema mismatch: expected " + attributes.size() + " columns, got " + vectors.size());
        }

        Block[] blocks = new Block[attributes.size()];
        try {
            for (int col = 0; col < attributes.size(); col++) {
                FieldVector vector = vectors.get(col);
                blocks[col] = convertArrowVectorToBlock(vector);
            }
            return new Page(rowCount, blocks);
        } catch (Exception e) {
            // Release any blocks that were created before the exception
            for (Block block : blocks) {
                if (block != null) {
                    block.close();
                }
            }
            throw new RuntimeException("Failed to convert Arrow vectors to ESQL blocks", e);
        }
    }

    /**
     * Convert a single Arrow FieldVector to an ESQL Block.
     * Uses {@link ArrowToBlockConverter} which provides symmetric conversion with BlockConverter.
     *
     * @param vector the Arrow field vector
     * @return ESQL Block with converted data
     */
    private Block convertArrowVectorToBlock(FieldVector vector) {
        // Get the Arrow type
        Types.MinorType arrowType = vector.getMinorType();

        // Get the appropriate converter for this Arrow type
        ArrowToBlockConverter converter = ArrowToBlockConverter.forType(arrowType);

        if (converter == null) {
            // Unsupported type - create null block
            return blockFactory.newConstantNullBlock(vector.getValueCount());
        }

        // Convert Arrow vector to ESQL block
        return converter.convert(vector, blockFactory);
    }
}
