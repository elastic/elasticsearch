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

    /**
     * @param emptyAsAbsent pack a zero-length value as absent, as PromQL reads an empty label
     */
    public record Factory(int[] channels, boolean emptyAsAbsent) implements OperatorFactory {
        public Factory(int[] channels) {
            this(channels, false);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new PackDimsOperator(driverContext, channels, emptyAsAbsent);
        }

        @Override
        public String describe() {
            return "PackDimsOperator[channels=" + Arrays.toString(channels) + (emptyAsAbsent ? ", emptyAsAbsent" : "") + "]";
        }
    }

    private final DriverContext driverContext;
    private final int[] channels;
    private final boolean emptyAsAbsent;

    public PackDimsOperator(DriverContext driverContext, int[] channels) {
        this(driverContext, channels, false);
    }

    public PackDimsOperator(DriverContext driverContext, int[] channels, boolean emptyAsAbsent) {
        this.driverContext = driverContext;
        this.channels = channels;
        this.emptyAsAbsent = emptyAsAbsent;
    }

    @Override
    protected Page process(Page page) {
        if (channels.length == 1) {
            return page.appendBlock(DimsPacker.packSingleColumn(driverContext, page.getBlock(channels[0]), emptyAsAbsent).asBlock());
        }
        Block[] blocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            blocks[i] = page.getBlock(channels[i]);
        }
        return page.appendBlock(DimsPacker.packMultiColumns(driverContext, blocks, emptyAsAbsent).asBlock());
    }

    @Override
    public String toString() {
        return "PackDimsOperator[channels=" + Arrays.toString(channels) + (emptyAsAbsent ? ", emptyAsAbsent" : "") + "]";
    }
}
