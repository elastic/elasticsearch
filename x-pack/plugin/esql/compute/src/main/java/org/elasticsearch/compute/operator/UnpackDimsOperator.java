/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.Arrays;

public class UnpackDimsOperator extends AbstractPageMappingOperator {

    public record Factory(int packedChannel, ElementType[] types) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new UnpackDimsOperator(driverContext, packedChannel, types);
        }

        @Override
        public String describe() {
            return "UnpackDimsOperator[packed=" + packedChannel + ", types=" + Arrays.toString(types) + "]";
        }
    }

    private final DriverContext driverContext;
    private final int packedChannel;
    private final ElementType[] types;

    public UnpackDimsOperator(DriverContext driverContext, int packedChannel, ElementType[] types) {
        this.driverContext = driverContext;
        this.packedChannel = packedChannel;
        this.types = types;
    }

    @Override
    protected Page process(Page page) {
        BytesRefBlock packedBlock = page.getBlock(packedChannel);
        BytesRefVector packedVector = packedBlock.asVector();
        if (packedVector == null) {
            throw new IllegalStateException("expected a packed BytesRefVector; got [" + packedBlock + "]");
        }
        if (types.length == 1) {
            return page.appendBlock(DimsPacker.unpackSingleColumn(driverContext, packedVector, types[0]));
        }
        return page.appendBlocks(DimsPacker.unpackMultiColumns(driverContext, packedVector, types));
    }

    @Override
    public String toString() {
        return "UnpackDimsOperator[packed=" + packedChannel + ", types=" + Arrays.toString(types) + "]";
    }
}
