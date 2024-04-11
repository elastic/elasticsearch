/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BytesRefArray;
$else$
import org.apache.lucene.util.RamUsageEstimator;
$endif$
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
$if(long)$
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.$Type$Block;
$else$
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.IntVector;
$endif$
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * {@link EnrichResultBuilder} for $Type$s.
 * This class is generated. Edit `X-EnrichResultBuilder.java.st` instead.
 */
final class EnrichResultBuilderFor$Type$ extends EnrichResultBuilder {
$if(BytesRef)$
    private final BytesRefArray bytes; // shared between all cells
$endif$
    private ObjectArray<$if(BytesRef)$int$else$$type$$endif$[]> cells;

    EnrichResultBuilderFor$Type$(BlockFactory blockFactory, int channel, int totalPositions) {
        super(blockFactory, channel, totalPositions);
        this.cells = blockFactory.bigArrays().newObjectArray(totalPositions);
$if(BytesRef)$
        BytesRefArray bytes = null;
        try {
            bytes = new BytesRefArray(totalPositions * 3L, blockFactory.bigArrays());
            this.bytes = bytes;
        } finally {
            if (bytes == null) {
                this.cells.close();
            }
        }
$endif$
    }

    @Override
    void addInputPage(IntVector positions, Page page) {
        $Type$Block block = page.getBlock(channel);
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        for (int i = 0; i < positions.getPositionCount(); i++) {
            int valueCount = block.getValueCount(i);
            if (valueCount == 0) {
                continue;
            }
            int cellPosition = positions.getInt(i);
            final var oldCell = cells.get(cellPosition);
            final var newCell = extendCell(oldCell, valueCount);
            cells.set(cellPosition, newCell);
            int dstIndex = oldCell != null ? oldCell.length : 0;
            adjustBreaker(RamUsageEstimator.sizeOf(newCell) - (oldCell != null ? RamUsageEstimator.sizeOf(oldCell) : 0));
            int firstValueIndex = block.getFirstValueIndex(i);
$if(BytesRef)$
            int bytesOrd = Math.toIntExact(bytes.size());
            for (int v = 0; v < valueCount; v++) {
                scratch = block.getBytesRef(firstValueIndex + v, scratch);
                bytes.append(scratch);
                newCell[dstIndex + v] = bytesOrd + v;
            }
$else$
            for (int v = 0; v < valueCount; v++) {
                newCell[dstIndex + v] = block.get$Type$(firstValueIndex + v);
            }
$endif$
        }
    }

    private $if(BytesRef)$int$else$$type$$endif$[] extendCell($if(BytesRef)$int$else$$type$$endif$[] oldCell, int newValueCount) {
        if (oldCell == null) {
            return new $if(BytesRef)$int$else$$type$$endif$[newValueCount];
        } else {
            return Arrays.copyOf(oldCell, oldCell.length + newValueCount);
        }
    }

    @Override
    Block build() {
        try ($Type$Block.Builder builder = blockFactory.new$Type$BlockBuilder(totalPositions)) {
$if(BytesRef)$
            BytesRef scratch = new BytesRef();
$endif$
            for (int i = 0; i < totalPositions; i++) {
                final var cell = cells.get(i);
                if (cell == null) {
                    builder.appendNull();
                    continue;
                }
                if (cell.length > 1) {
                    builder.beginPositionEntry();
                }
                // TODO: sort and dedup
                for (var v : cell) {
$if(BytesRef)$
                    builder.appendBytesRef(bytes.get(v, scratch));
$else$
                    builder.append$Type$(v);
$endif$
                }
                if (cell.length > 1) {
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    @Override
    public void close() {
        Releasables.close($if(BytesRef)$bytes, $endif$cells, super::close);
    }
}
