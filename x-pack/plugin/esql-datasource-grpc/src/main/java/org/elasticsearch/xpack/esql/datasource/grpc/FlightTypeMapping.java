/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.arrow.ArrowToEsql;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps between Apache Arrow types and ESQL types.
 * Handles both schema conversion (Arrow Field to ESQL Attribute) and
 * data conversion (Arrow FieldVector to ESQL Block).
 */
final class FlightTypeMapping {

    private FlightTypeMapping() {}

    static List<Attribute> toAttributes(Schema schema) {
        List<Attribute> attributes = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            var mapping = ArrowToEsql.forField(field);
            if (mapping == null) {
                throw new IllegalArgumentException("Unsupported Arrow vector type: " + field.getType());
            }
            attributes.add(new ReferenceAttribute(Source.EMPTY, field.getName(), mapping.dataType()));
        }
        return attributes;
    }

    static <V extends ValueVector> V transfer(V vector, BlockFactory blockFactory) {
        var tp = vector.getTransferPair(blockFactory.arrowAllocator());
        tp.transfer();
        @SuppressWarnings("unchecked")
        var result = (V) tp.getTo();
        return result;
    }

    static Block toBlock(FieldVector flightVector, int rowCount, BlockFactory blockFactory) {
        // Trim the vector to the expected size (doesn't shrink buffers)
        if (flightVector.getValueCount() > rowCount) {
            flightVector.setValueCount(rowCount);
        }

        // FlightClient creates a child allocator. We need to transfer vectors to the block factory's allocator
        // since blocks live longer than the FlightClient and its allocator.
        try (var vector = transfer(flightVector, blockFactory)) {
            var mapping = ArrowToEsql.forField(flightVector.getField());
            if (mapping == null) {
                throw new IllegalArgumentException("Unsupported Arrow vector type: " + vector.getField().getType());
            }
            return mapping.convert(vector, blockFactory);
        }
    }
}
