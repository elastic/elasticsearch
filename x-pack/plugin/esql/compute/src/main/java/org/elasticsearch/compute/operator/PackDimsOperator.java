/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.Arrays;

public class PackDimsOperator extends AbstractPageMappingOperator {

    public record Factory(int[] channels) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new PackDimsOperator(driverContext, channels);
        }

        @Override
        public String describe() {
            return "PackDimsOperator[channels=" + Arrays.toString(channels) + "]";
        }
    }

    private final DriverContext driverContext;
    private final int[] channels;

    public PackDimsOperator(DriverContext driverContext, int[] channels) {
        this.driverContext = driverContext;
        this.channels = channels;
    }

    @Override
    protected Page process(Page page) {
        if (channels.length == 1) {
            return page.appendBlock(DimsPacker.packSingleColumn(driverContext, page.getBlock(channels[0])).asBlock());
        }
        Block[] blocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            blocks[i] = page.getBlock(channels[i]);
        }
        return page.appendBlock(DimsPacker.packMultiColumns(driverContext, blocks).asBlock());
    }

    @Override
    public String toString() {
        return "PackDimsOperator[channels=" + Arrays.toString(channels) + "]";
    }
}
