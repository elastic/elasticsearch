/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Conversion of {@code geotile} blocks to an Arrow struct with {@code zoom:uint8}, {@code x:uint32} and {@code y:uint32} fields.
 * <p>
 * A geotile can be null but its fields never are, so the children are declared non-nullable and have no validity buffer.
 * Null geotiles have their fields set to zero.
 * </p>
 */
class GeoTileArrowFormatter extends BlockArrowFormatter {

    private enum Component {
        ZOOM("zoom", Types.MinorType.UINT1, Byte.BYTES),
        X("x", Types.MinorType.UINT4, Integer.BYTES),
        Y("y", Types.MinorType.UINT4, Integer.BYTES);

        private final String fieldName;
        private final Types.MinorType minorType;
        private final int width;

        Component(String fieldName, Types.MinorType minorType, int width) {
            this.fieldName = fieldName;
            this.minorType = minorType;
            this.width = width;
        }
    }

    GeoTileArrowFormatter(DataType esqlType) {
        super(esqlType, Types.MinorType.STRUCT);
    }

    @Override
    public Field arrowField(String name) {
        Field superField = super.arrowField(name);
        var children = new ArrayList<Field>();
        for (Component component : Component.values()) {
            children.add(new Field(component.fieldName, new FieldType(false, component.minorType.getType(), null), null));
        }
        return new Field(name, superField.getFieldType(), children);
    }

    @Override
    public void convert(
        Block b,
        boolean asList,
        List<ArrowFieldNode> nodes,
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        List<Releasable> releasables
    ) {
        addSimpleFieldNodes(b, asList, nodes);
        int valueCount = arrowValueCount(b, asList);
        for (int i = 0; i < Component.values().length; i++) {
            nodes.add(new ArrowFieldNode(valueCount, 0));
        }
        if (asList) {
            addListOffsets(bufs, bufWriters, b);
        }
        // Struct validity. Empty if in a list, as the list carries the nulls.
        accumulateVectorValidity(bufs, bufWriters, b, asList);

        for (Component component : Component.values()) {
            // Non-nullable child: empty validity buffer
            addEmptyValidity(bufs, bufWriters);

            long byteCount = (long) valueCount * component.width;
            bufs.add(dummyArrowBuf(byteCount));
            bufWriters.add(out -> {
                if (b.areAllValuesNull()) {
                    // Fast path: all values are null, write zeroed child slots directly
                    return writeZeroes(out, byteCount);
                }
                LongBlock block = (LongBlock) b;
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (asList == false) {
                            writeComponent(out, component, 0L);
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        writeComponent(out, component, block.getLong(valueIndex));
                    }
                }
                return byteCount;
            });
        }
    }

    private static void writeComponent(RecyclerBytesStreamOutput out, Component component, long geotile) throws IOException {
        switch (component) {
            case ZOOM -> out.writeByte((byte) GeoTileUtils.getZoom(geotile));
            case X -> out.writeIntLE(GeoTileUtils.getXTile(geotile));
            case Y -> out.writeIntLE(GeoTileUtils.getYTile(geotile));
        }
    }
}
