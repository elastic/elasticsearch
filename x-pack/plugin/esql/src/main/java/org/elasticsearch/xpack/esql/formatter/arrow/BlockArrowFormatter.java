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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for stateless formatters that serialize ES|QL {@link Block}s into Arrow IPC record-batch buffers.
 * <p>
 * Formatters avoid Arrow off-heap allocation by pairing size-only dummy {@link ArrowBuf}s (used by Arrow's
 * {@code MessageSerializer} to compute IPC metadata offsets) with deferred {@link BufWriter} closures that stream
 * block bytes directly into the HTTP response buffer. Any temporary resources created during conversion are registered
 * in {@code releasables} and closed once the batch has been written.
 */
abstract class BlockArrowFormatter {

    static final String ESQL_TYPE_METADATA = "elastic:type";

    private static final byte[] ZERO_BYTES = new byte[4096];

    private final FieldType fieldType;

    protected BlockArrowFormatter(DataType esqlType, Types.MinorType minorType) {
        this(esqlType, minorType, Map.of());
    }

    protected BlockArrowFormatter(DataType esqlType, Types.MinorType minorType, Map<String, String> extraMetadata) {
        // Add the exact ESQL type as field metadata
        var meta = new HashMap<>(extraMetadata);
        meta.put(ESQL_TYPE_METADATA, esqlType.outputType());
        this.fieldType = new FieldType(true, minorType.getType(), null, meta);
    }

    public Field arrowField(String name) {
        return new Field(name, fieldType, null);
    }

    /**
     * Whether this formatter always encodes its values as an Arrow list, even when its blocks contain only single values.
     */
    public boolean alwaysList() {
        return false;
    }

    /**
     * Whether this block requires its column to be encoded as an Arrow list. The column is a list if any of its blocks
     * returns {@code true}, and the remaining blocks may then not be checked. This is decided before the schema is sent,
     * since the schema can't change once streaming has started.
     * <p>
     * Formatters that can't encode lists must override this to return {@code false}, rejecting blocks that actually
     * contain multivalued positions.
     *
     * @param columnName the column name, for error messages
     * @param block a block of that column
     */
    public boolean isMultivalued(String columnName, Block block) {
        return block.mayHaveMultivaluedFields();
    }

    /**
     * Implementation of {@link #isMultivalued} for types whose values are always single-valued, and that can't be
     * encoded as Arrow lists: returns {@code false}, or throws if the block actually has multivalued positions.
     * <p>
     * {@link Block#mayHaveMultivaluedFields()} can report false positives, which must not reject valid values, so this
     * checks {@link Block#doesHaveMultivaluedFields()}.
     *
     * @param columnName the column name, for error messages
     * @param esqlType the type of the column, for error messages
     * @param block a block of that column
     */
    protected static boolean forbidMultivalued(String columnName, DataType esqlType, Block block) {
        if (block.doesHaveMultivaluedFields()) {
            throw new IllegalArgumentException(
                "Column ["
                    + columnName
                    + "] of ES|QL type ["
                    + esqlType.outputType()
                    + "] has multivalued fields, which the Arrow format does not support for this type"
            );
        }
        return false;
    }

    public interface BufWriter {
        long write(RecyclerBytesStreamOutput out) throws IOException;
    }

    /**
     * Add Arrow field nodes and convert a block into Arrow buffers.
     *
     * @param block the ESQL block
     * @param asList whether to encode values as an Arrow list. The encoding is fixed for the column even when a page
     *               contains only single values or nulls.
     * @param nodes Arrow field nodes describing value and null counts
     * @param bufs arrow buffers, used to track sizes
     * @param bufWriters buffer writers, that will do the actual work of writing the data
     * @param releasables resources retained until the deferred buffer writers have completed
     */
    public abstract void convert(
        Block block,
        boolean asList,
        List<ArrowFieldNode> nodes,
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        List<Releasable> releasables
    );

    /**
     * Add Arrow field nodes for a non-nested type (either a single vector node, or a list node plus its value vector node).
     */
    protected static void addSimpleFieldNodes(Block block, boolean asList, List<ArrowFieldNode> nodes) {
        nodes.add(new ArrowFieldNode(block.getPositionCount(), block.nullValuesCount()));
        if (asList) {
            // Value vector, does not contain nulls.
            nodes.add(new ArrowFieldNode(block.getTotalValueCount(), 0));
        }
    }

    protected static Field nonNullableListField(String name, Types.MinorType itemType) {
        return new Field(
            name,
            new FieldType(false, Types.MinorType.LIST.getType(), null),
            List.of(new Field("$data$", new FieldType(false, itemType.getType(), null), null))
        );
    }

    protected static void addEmptyValidity(List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
        bufs.add(dummyArrowBuf(0));
        bufWriters.add(out -> 0);
    }

    protected static void addNonNullableListBuffers(
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        int[] offsets,
        int itemBytes,
        BufWriter valuesWriter
    ) {
        addEmptyValidity(bufs, bufWriters);
        long offsetsBytes = (long) offsets.length * Integer.BYTES;
        bufs.add(dummyArrowBuf(offsetsBytes));
        bufWriters.add(out -> {
            for (int offset : offsets) {
                out.writeIntLE(offset);
            }
            return offsetsBytes;
        });
        addEmptyValidity(bufs, bufWriters);
        long valuesBytes = (long) offsets[offsets.length - 1] * itemBytes;
        bufs.add(dummyArrowBuf(valuesBytes));
        bufWriters.add(out -> {
            long written = valuesWriter.write(out);
            assert written == valuesBytes : "Expected " + valuesBytes + " bytes, wrote " + written;
            return written;
        });
    }

    /** Maps Arrow struct items to block value indexes, skipping null positions when a column is multivalued. */
    protected static int[] itemIndexes(Block block, boolean asList) {
        int positions = block.getPositionCount();
        if (asList == false) {
            int[] indexes = new int[positions];
            for (int position = 0; position < positions; position++) {
                indexes[position] = block.isNull(position) ? -1 : block.getFirstValueIndex(position);
            }
            return indexes;
        }
        int[] indexes = new int[block.getTotalValueCount()];
        int item = 0;
        for (int position = 0; position < positions; position++) {
            int first = block.getFirstValueIndex(position);
            for (int i = 0; i < block.getValueCount(position); i++) {
                indexes[item++] = first + i;
            }
        }
        assert item == indexes.length;
        return indexes;
    }

    // Create a dummy ArrowBuf used for size accounting purposes.
    protected static ArrowBuf dummyArrowBuf(long size) {
        return new ArrowBuf(null, null, 0, 0).writerIndex(size);
    }

    // Length in bytes of a validity buffer
    protected static int bitSetLength(int totalValues) {
        return (int) ((totalValues + 7L) / 8);
    }

    protected static int arrowValueCount(Block block, boolean asList) {
        return asList ? block.getTotalValueCount() : block.getPositionCount();
    }

    protected static void accumulateVectorValidity(List<ArrowBuf> bufs, List<BufWriter> bufWriters, Block b, boolean asList) {
        // For list encoding, validities are output in the parent Arrow List buffer. The values themselves do not contain nulls.
        if (asList || b.mayHaveNulls() == false) {
            // Arrow IPC allows a compact form for "all true" validities using an empty buffer.
            addEmptyValidity(bufs, bufWriters);
            return;
        }

        int valueCount = b.getPositionCount();
        bufs.add(dummyArrowBuf(bitSetLength(valueCount)));
        bufWriters.add(out -> {
            if (b.areAllValuesNull()) {
                return writeAllFalseValidity(out, valueCount);
            } else {
                return writeValidities(out, b);
            }
        });
    }

    protected static long writeAllFalseValidity(RecyclerBytesStreamOutput out, int valueCount) {
        return writeZeroes(out, bitSetLength(valueCount));
    }

    protected static long writeValidities(RecyclerBytesStreamOutput out, Block block) {
        int positionCount = block.getPositionCount();
        BitSet bits = new BitSet(positionCount);
        for (int i = 0; i < positionCount; i++) {
            // isNull uses position indices, not value indices.
            if (block.isNull(i) == false) {
                bits.set(i);
            }
        }
        return writeBitSet(out, bits, positionCount);
    }

    protected static long writeBitSet(RecyclerBytesStreamOutput out, BitSet bits, int bitCount) {
        byte[] bytes = bits.toByteArray();
        out.writeBytes(bytes, 0, bytes.length);

        // toByteArray will return bytes up to the last bit set. It may therefore
        // have a length lower than what is needed to actually store bitCount bits.
        int expectedLength = bitSetLength(bitCount);
        writeZeroes(out, expectedLength - bytes.length);

        return expectedLength;
    }

    protected static long writeZeroes(RecyclerBytesStreamOutput out, long byteCount) {
        long remaining = byteCount;
        while (remaining > 0) {
            int chunk = (int) Math.min(remaining, ZERO_BYTES.length);
            out.writeBytes(ZERO_BYTES, 0, chunk);
            remaining -= chunk;
        }
        return byteCount;
    }

    protected static void addListOffsets(List<ArrowBuf> bufs, List<BufWriter> bufWriters, Block block) {
        // Add validity buffer
        accumulateVectorValidity(bufs, bufWriters, block, false);

        // Add offsets buffer
        long bufferLen = (long) Integer.BYTES * (block.getPositionCount() + 1L);

        bufs.add(dummyArrowBuf(bufferLen));
        bufWriters.add(out -> {
            int offset = 0;
            out.writeIntLE(offset);
            for (int position = 0; position < block.getPositionCount(); position++) {
                offset = Math.addExact(offset, block.getValueCount(position));
                out.writeIntLE(offset);
            }

            return bufferLen;
        });
    }
}
