/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Augments output Pages with constant blocks for virtual partition columns.
 * The FormatReader produces pages with only data columns; this injector inserts
 * partition-value constant blocks at the correct positions to match the full output schema.
 */
final class VirtualColumnInjector {

    private final List<Attribute> fullOutput;
    private final Map<String, Object> partitionValues;
    private final BlockFactory blockFactory;
    private final int[] dataColumnIndices;
    private final int[] partitionColumnIndices;

    VirtualColumnInjector(
        List<Attribute> fullOutput,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        BlockFactory blockFactory
    ) {
        Check.isTrue(fullOutput != null && fullOutput.isEmpty() == false, "fullOutput cannot be null or empty");
        Check.notNull(partitionColumnNames, "partitionColumnNames cannot be null");
        Check.notNull(blockFactory, "blockFactory cannot be null");
        this.fullOutput = fullOutput;
        this.partitionValues = partitionValues != null ? partitionValues : Map.of();
        this.blockFactory = blockFactory;

        List<Integer> dataIdxList = new ArrayList<>();
        List<Integer> partIdxList = new ArrayList<>();
        for (int i = 0; i < fullOutput.size(); i++) {
            if (partitionColumnNames.contains(fullOutput.get(i).name())) {
                partIdxList.add(i);
            } else {
                dataIdxList.add(i);
            }
        }
        this.dataColumnIndices = toIntArray(dataIdxList);
        this.partitionColumnIndices = toIntArray(partIdxList);
    }

    boolean hasPartitionColumns() {
        return partitionColumnIndices.length > 0;
    }

    List<String> dataColumnNames() {
        List<String> names = new ArrayList<>(dataColumnIndices.length);
        for (int idx : dataColumnIndices) {
            names.add(fullOutput.get(idx).name());
        }
        return names;
    }

    Page inject(Page dataPage) {
        if (partitionColumnIndices.length == 0) {
            return dataPage;
        }

        int positions = dataPage.getPositionCount();
        Block[] blocks = new Block[fullOutput.size()];

        int dataBlockIdx = 0;
        for (int idx : dataColumnIndices) {
            blocks[idx] = dataPage.getBlock(dataBlockIdx++);
        }

        for (int idx : partitionColumnIndices) {
            Attribute attr = fullOutput.get(idx);
            Object value = partitionValues.get(attr.name());
            blocks[idx] = createConstantBlock(attr, value, positions);
        }

        return new Page(positions, blocks);
    }

    private static int[] toIntArray(List<Integer> list) {
        int[] result = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    private Block createConstantBlock(Attribute attr, Object value, int positions) {
        return switch (value) {
            case Integer intVal -> blockFactory.newConstantIntBlockWith(intVal, positions);
            case Long longVal -> blockFactory.newConstantLongBlockWith(longVal, positions);
            case Double doubleVal -> blockFactory.newConstantDoubleBlockWith(doubleVal, positions);
            case Boolean boolVal -> blockFactory.newConstantBooleanBlockWith(boolVal, positions);
            case null -> blockFactory.newConstantBytesRefBlockWith(new BytesRef(""), positions);
            default -> blockFactory.newConstantBytesRefBlockWith(new BytesRef(value.toString()), positions);
        };
    }
}
