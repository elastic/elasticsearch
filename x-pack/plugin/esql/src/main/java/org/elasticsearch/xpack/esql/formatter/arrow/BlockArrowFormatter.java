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
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlock;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.ToDoubleFunction;

public abstract class BlockArrowFormatter {

    public static final String ESQL_TYPE_METADATA = "elastic:type";

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

    // Block.nullValuesCount was more efficient but was removed in https://github.com/elastic/elasticsearch/pull/108916
    protected int nullValuesCount(Block block) {
        if (block.mayHaveNulls() == false) {
            return 0;
        }

        if (block.areAllValuesNull()) {
            return block.getPositionCount();
        }

        int count = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                count++;
            }
        }
        return count;
    }

    public interface BufWriter {
        long write(RecyclerBytesStreamOutput out) throws IOException;
    }

    /**
     * Add Arrow field nodes for this block. Must be called before {@link #convert}.
     * Override for nested types that contribute multiple nodes (e.g., struct).
     */
    public void addFieldNodes(Block block, boolean asList, List<ArrowFieldNode> nodes) {
        if (asList) {
            // List node
            nodes.add(new ArrowFieldNode(block.getPositionCount(), nullValuesCount(block)));
            // Value vector, does not contain nulls.
            nodes.add(new ArrowFieldNode(block.getTotalValueCount(), 0));
        } else {

            nodes.add(new ArrowFieldNode(block.getPositionCount(), nullValuesCount(block)));
        }
    }

    /**
     * Convert a block into Arrow buffers.
     * @param block the ESQL block
     * @param asList whether to encode values as an Arrow list. The encoding is fixed for the column even when a page
     *               contains only single values or nulls.
     * @param bufs arrow buffers, used to track sizes
     * @param bufWriters buffer writers, that will do the actual work of writing the data
     * @param releasables resources retained until the deferred buffer writers have completed
     */
    public abstract void convert(
        Block block,
        boolean asList,
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        List<Releasable> releasables
    );

    /**
     * Encodes dense vectors as Arrow lists with {@code float32} elements. Byte and bit vector elements have already been
     * widened to floats by the field loader.
     */
    public static class AsDenseVector extends BlockArrowFormatter {

        public AsDenseVector(DataType esqlType) {
            super(esqlType, Types.MinorType.FLOAT4);
        }

        @Override
        public boolean alwaysList() {
            return true;
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            FloatBlock block = (FloatBlock) b;
            assert asList;
            addListOffsets(bufs, bufWriters, block);
            accumulateVectorValidity(bufs, bufWriters, block, true);

            long byteSize = (long) Float.BYTES * block.getTotalValueCount();
            bufs.add(dummyArrowBuf(byteSize));
            bufWriters.add(out -> {
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeIntLE(Float.floatToRawIntBits(block.getFloat(valueIndex)));
                    }
                }
                return byteSize;
            });
        }
    }

    /**
     * Conversion of Double blocks
     */
    public static class AsFloat64 extends BlockArrowFormatter {

        public AsFloat64(DataType esqlType) {
            super(esqlType, Types.MinorType.FLOAT8);
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            DoubleBlock block = (DoubleBlock) b;

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            bufs.add(dummyArrowBuf(vectorByteSize(block, asList)));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                int count = arrowValueCount(block, asList);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (asList == false) {
                            out.writeDoubleLE(0);
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeDoubleLE(block.getDouble(valueIndex));
                    }
                }
                return (long) count * Double.BYTES;
            });
        }

        private static int vectorByteSize(DoubleBlock b, boolean asList) {
            return Double.BYTES * arrowValueCount(b, asList);
        }
    }

    /**
     * Conversion of Int blocks
     */
    public static class AsInt32 extends BlockArrowFormatter {

        public AsInt32(DataType esqlType) {
            super(esqlType, Types.MinorType.INT);
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            IntBlock block = (IntBlock) b;

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            bufs.add(dummyArrowBuf(vectorByteSize(block, asList)));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                int count = arrowValueCount(block, asList);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (asList == false) {
                            out.writeIntLE(0);
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeIntLE(block.getInt(valueIndex));
                    }
                }
                return (long) count * Integer.BYTES;
            });
        }

        private static int vectorByteSize(Block b, boolean asList) {
            return Integer.BYTES * arrowValueCount(b, asList);
        }
    }

    /**
     * Conversion of Long blocks
     */
    public static class AsInt64 extends BlockArrowFormatter {
        public AsInt64(DataType esqlType) {
            this(esqlType, Types.MinorType.BIGINT);
        }

        protected AsInt64(DataType esqlType, Types.MinorType minorType) {
            super(esqlType, minorType);
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            LongBlock block = (LongBlock) b;

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            bufs.add(dummyArrowBuf(vectorByteSize(block, asList)));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                int count = arrowValueCount(block, asList);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (asList == false) {
                            out.writeLongLE(0);
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeLongLE(arrowValue(block.getLong(valueIndex)));
                    }
                }
                return (long) count * Long.BYTES;
            });
        }

        protected long arrowValue(long blockValue) {
            return blockValue;
        }

        private static int vectorByteSize(LongBlock b, boolean asList) {
            return Long.BYTES * arrowValueCount(b, asList);
        }
    }

    /**
     * Conversion of {@code unsigned_long} blocks, which store values with the sign bit flipped so that they sort as signed
     * longs. Arrow's {@code UInt64} expects the plain unsigned bits.
     */
    public static class AsFlippedSignInt64 extends AsInt64 {
        public AsFlippedSignInt64(DataType esqlType) {
            super(esqlType, Types.MinorType.UINT8);
        }

        @Override
        protected long arrowValue(long blockValue) {
            return blockValue ^ NumericUtils.TWOS_COMPLEMENT_BITMASK;
        }
    }

    /**
     * Conversion of Boolean blocks
     */
    public static class AsBoolean extends BlockArrowFormatter {
        public AsBoolean(DataType esqlType) {
            super(esqlType, Types.MinorType.BIT);
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            BooleanBlock block = (BooleanBlock) b;

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            bufs.add(dummyArrowBuf(vectorByteSize(block, asList)));
            bufWriters.add(out -> {
                int count = arrowValueCount(block, asList);
                BitSet bits = new BitSet();

                // Only set the bits that are true, writeBitSet will take
                // care of adding zero bytes if needed.
                int outputIndex = 0;
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (asList == false) {
                            outputIndex++;
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        if (block.getBoolean(valueIndex)) {
                            bits.set(outputIndex);
                        }
                        outputIndex++;
                    }
                }

                return BlockArrowFormatter.writeBitSet(out, bits, count);
            });
        }

        private static int vectorByteSize(BooleanBlock b, boolean asList) {
            return BlockArrowFormatter.bitSetLength(arrowValueCount(b, asList));
        }
    }

    /**
     * Conversion of ByteRef blocks
     */
    public static class BytesReFormatter extends BlockArrowFormatter {

        public BytesReFormatter(DataType esqlType, Types.MinorType minorType) {
            super(esqlType, minorType);
        }

        protected BytesReFormatter(DataType esqlType, Types.MinorType minorType, Map<String, String> extraMetadata) {
            super(esqlType, minorType, extraMetadata);
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            BytesRefBlock block = (BytesRefBlock) b;

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            // Offsets vector
            bufs.add(dummyArrowBuf(offsetvectorByteSize(block, asList)));

            bufWriters.add(out -> writeOffsets(out, block, asList));

            // Data vector
            bufs.add(BlockArrowFormatter.dummyArrowBuf(dataVectorByteSize(block)));

            bufWriters.add(out -> writeData(out, block));
        }

        protected BytesRef getBytesRef(BytesRefBlock block, int valueIndex, BytesRef scratch) {
            return block.getBytesRef(valueIndex, scratch);
        }

        private static int offsetvectorByteSize(BytesRefBlock block, boolean asList) {
            return Integer.BYTES * (arrowValueCount(block, asList) + 1);
        }

        private long writeOffsets(RecyclerBytesStreamOutput out, BytesRefBlock block, boolean asList) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            BytesRef scratch = new BytesRef();
            int offset = 0;
            out.writeIntLE(offset);
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    if (asList == false) {
                        out.writeIntLE(offset);
                    }
                    continue;
                }
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    // FIXME: add a ByteRefsVector.getLength(position): there are some cases
                    // where getBytesRef will allocate, which isn't needed here.
                    offset += getBytesRef(block, valueIndex, scratch).length;
                    out.writeIntLE(offset);
                }
            }
            return offsetvectorByteSize(block, asList);
        }

        private long writeData(RecyclerBytesStreamOutput out, BytesRefBlock block) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            BytesRef scratch = new BytesRef();
            long startPosition = out.position();
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    continue;
                }
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    BytesRef value = getBytesRef(block, valueIndex, scratch);
                    out.write(value.bytes, value.offset, value.length);
                }
            }
            return out.position() - startPosition;
        }

        private int dataVectorByteSize(BytesRefBlock block) {
            // TODO we can probably get the length from the vector without all this sum
            int length = 0;
            BytesRef scratch = new BytesRef();
            for (int position = 0; position < block.getPositionCount(); position++) {
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    length += getBytesRef(block, valueIndex, scratch).length;
                }
            }
            return length;
        }
    }

    /**
     * Conversion of ByteRefs where each value is itself converted to a different format.
     */
    public static class TransformedBytesRef extends BytesReFormatter {

        private final BiFunction<BytesRef, BytesRef, BytesRef> valueConverter;

        /**
         *
         * @param esqlType ESQL data type
         * @param minorType Arrow type
         * @param valueConverter a function that takes (value, scratch) input parameters and returns the transformed value
         */
        public TransformedBytesRef(DataType esqlType, Types.MinorType minorType, BiFunction<BytesRef, BytesRef, BytesRef> valueConverter) {
            super(esqlType, minorType);
            this.valueConverter = valueConverter;
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            BytesRefBlock transformed = transformValues((BytesRefBlock) b);
            releasables.add(transformed);
            super.convert(transformed, asList, bufs, bufWriters, releasables);
        }

        private BytesRefBlock transformValues(BytesRefBlock block) {
            try (BytesRefBlock.Builder builder = block.blockFactory().newBytesRefBlockBuilder(block.getPositionCount())) {
                BytesRef scratch = new BytesRef();
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        builder.appendNull();
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    if (end - start == 1) {
                        convertAndAppend(builder, block, start, scratch);
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        convertAndAppend(builder, block, valueIndex, scratch);
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        private void convertAndAppend(BytesRefBlock.Builder builder, BytesRefBlock block, int valueIndex, BytesRef scratch) {
            BytesRef bytes = block.getBytesRef(valueIndex, scratch);
            if (bytes.length != 0) {
                bytes = valueConverter.apply(bytes, scratch);
            }
            builder.appendBytesRef(bytes);
        }
    }

    public static class AsVarChar extends BytesReFormatter {
        public AsVarChar(DataType esqlType) {
            super(esqlType, Types.MinorType.VARCHAR);
        }
    }

    public static class AsVarBinary extends BytesReFormatter {
        public AsVarBinary(DataType esqlType) {
            super(esqlType, Types.MinorType.VARBINARY);
        }
    }

    /**
     * Geometries stored as WKB in a {@code BytesRefBlock}, exposed as a variable-size binary vector tagged with the
     * <a href="https://geoarrow.org/extension-types.html#wkb">{@code geoarrow.wkb}</a> Arrow extension type.
     */
    public static class AsGeoArrowWkb extends BytesReFormatter {
        public static final String GEOARROW_WKB = "geoarrow.wkb";

        /**
         * GeoArrow extension metadata for geographic types: the CRS is {@code OGC:CRS84} (WGS 84, longitude/latitude order),
         * given as an authority code.
         */
        private static final String GEO_CRS_METADATA = "{\"crs\":\"OGC:CRS84\",\"crs_type\":\"authority_code\"}";

        public AsGeoArrowWkb(DataType esqlType) {
            super(esqlType, Types.MinorType.VARBINARY, extensionMetadata(esqlType));
        }

        /**
         * Geo types declare the CRS as lon/lat. Cartesian types omit the {@code crs}, which GeoArrow defines as "the
         * producer does not have any information about the CRS".
         */
        private static Map<String, String> extensionMetadata(DataType esqlType) {
            boolean geographic = switch (esqlType) {
                case GEO_POINT, GEO_SHAPE -> true;
                case CARTESIAN_POINT, CARTESIAN_SHAPE -> false;
                default -> throw new IllegalArgumentException("Not a WKB geometry type: " + esqlType);
            };
            var metadata = new HashMap<String, String>();
            metadata.put(ExtensionType.EXTENSION_METADATA_KEY_NAME, GEOARROW_WKB);
            if (geographic) {
                metadata.put(ExtensionType.EXTENSION_METADATA_KEY_METADATA, GEO_CRS_METADATA);
            }
            return metadata;
        }
    }

    /**
     * Conversion of LongRangeBlocks (DATE_RANGE) to an Arrow struct with "from" and "to" timestamp[ms] fields.
     */
    public static class AsDateRange extends BlockArrowFormatter {

        public AsDateRange(DataType esqlType) {
            super(esqlType, Types.MinorType.STRUCT);
        }

        @Override
        public Field arrowField(String name) {
            Field superField = super.arrowField(name);
            var fromField = new Field("from", new FieldType(true, Types.MinorType.TIMESTAMPMILLI.getType(), null), null);
            var toField = new Field("to", new FieldType(true, Types.MinorType.TIMESTAMPMILLI.getType(), null), null);
            return new Field(name, superField.getFieldType(), List.of(fromField, toField));
        }

        @Override
        public void addFieldNodes(Block block, boolean asList, List<ArrowFieldNode> nodes) {
            int count = block.getPositionCount();
            if (asList) {
                int valueCount = block.getTotalValueCount();
                nodes.add(new ArrowFieldNode(count, nullValuesCount(block)));  // list
                nodes.add(new ArrowFieldNode(valueCount, 0));                  // struct (items have no independent nulls)
                nodes.add(new ArrowFieldNode(valueCount, 0));                  // from
                nodes.add(new ArrowFieldNode(valueCount, 0));                  // to
            } else {
                nodes.add(new ArrowFieldNode(count, nullValuesCount(block)));
                if (block.areAllValuesNull()) {
                    nodes.add(new ArrowFieldNode(count, count));
                    nodes.add(new ArrowFieldNode(count, count));
                } else {
                    LongRangeBlock rangeBlock = (LongRangeBlock) block;
                    nodes.add(new ArrowFieldNode(count, nullValuesCount(rangeBlock.getFromBlock())));
                    nodes.add(new ArrowFieldNode(count, nullValuesCount(rangeBlock.getToBlock())));
                }
            }
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            LongRangeBlock block = (LongRangeBlock) b;
            int count = block.getPositionCount();

            if (asList) {
                int valueCount = block.getTotalValueCount();
                addListOffsets(bufs, bufWriters, block);
                // Struct validity: empty — items inside a list entry are never null in ES|QL
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);
                // from child: empty validity + int64 data for all value slots
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);
                bufs.add(dummyArrowBuf(valueCount * Long.BYTES));
                bufWriters.add(out -> {
                    LongBlock fromBlock = block.getFromBlock();
                    writeMultivaluedLongs(out, fromBlock);
                    return (long) valueCount * Long.BYTES;
                });
                // to child: empty validity + int64 data for all value slots
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);
                bufs.add(dummyArrowBuf(valueCount * Long.BYTES));
                bufWriters.add(out -> {
                    LongBlock toBlock = block.getToBlock();
                    writeMultivaluedLongs(out, toBlock);
                    return (long) valueCount * Long.BYTES;
                });
            } else {
                // Struct validity bitmap
                accumulateVectorValidity(bufs, bufWriters, block, false);
                if (block.areAllValuesNull()) {
                    // ConstantNullBlock: getFromBlock()/getToBlock() return `this`, and getLong() throws.
                    // Write all-null validity and zeroed data for both children.
                    addAllNullChildBuffers(bufs, bufWriters, count);
                    addAllNullChildBuffers(bufs, bufWriters, count);
                } else {
                    addChildBuffers(bufs, bufWriters, count, block.getFromBlock());
                    addChildBuffers(bufs, bufWriters, count, block.getToBlock());
                }
            }
        }

        private static void addAllNullChildBuffers(List<ArrowBuf> bufs, List<BufWriter> bufWriters, int count) {
            bufs.add(dummyArrowBuf(bitSetLength(count)));
            bufWriters.add(out -> writeAllFalseValidity(out, count));
            bufs.add(dummyArrowBuf(count * Long.BYTES));
            bufWriters.add(out -> writeZeroes(out, count * Long.BYTES));
        }

        private static void addChildBuffers(List<ArrowBuf> bufs, List<BufWriter> bufWriters, int count, LongBlock childBlock) {
            accumulateVectorValidity(bufs, bufWriters, childBlock, false);
            bufs.add(dummyArrowBuf(count * Long.BYTES));
            bufWriters.add(out -> {
                for (int position = 0; position < childBlock.getPositionCount(); position++) {
                    out.writeLongLE(childBlock.isNull(position) ? 0 : childBlock.getLong(childBlock.getFirstValueIndex(position)));
                }
                return (long) count * Long.BYTES;
            });
        }

        private static void writeMultivaluedLongs(RecyclerBytesStreamOutput out, LongBlock block) throws IOException {
            for (int position = 0; position < block.getPositionCount(); position++) {
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    out.writeLongLE(block.getLong(valueIndex));
                }
            }
        }
    }

    /**
     * Conversion of DoubleRangeBlocks to an Arrow struct with "from" and "to" float64 fields.
     */
    public static class AsDoubleRange extends BlockArrowFormatter {

        public AsDoubleRange(DataType esqlType) {
            super(esqlType, Types.MinorType.STRUCT);
        }

        @Override
        public Field arrowField(String name) {
            Field superField = super.arrowField(name);
            var fromField = new Field("from", new FieldType(true, Types.MinorType.FLOAT8.getType(), null), null);
            var toField = new Field("to", new FieldType(true, Types.MinorType.FLOAT8.getType(), null), null);
            return new Field(name, superField.getFieldType(), List.of(fromField, toField));
        }

        @Override
        public void addFieldNodes(Block block, boolean asList, List<ArrowFieldNode> nodes) {
            int count = block.getPositionCount();
            if (asList) {
                int valueCount = block.getTotalValueCount();
                nodes.add(new ArrowFieldNode(count, nullValuesCount(block)));  // list
                nodes.add(new ArrowFieldNode(valueCount, 0));                  // struct (items have no independent nulls)
                nodes.add(new ArrowFieldNode(valueCount, 0));                  // from
                nodes.add(new ArrowFieldNode(valueCount, 0));                  // to
            } else {
                nodes.add(new ArrowFieldNode(count, nullValuesCount(block)));
                if (block.areAllValuesNull()) {
                    nodes.add(new ArrowFieldNode(count, count));
                    nodes.add(new ArrowFieldNode(count, count));
                } else {
                    DoubleRangeBlock rangeBlock = (DoubleRangeBlock) block;
                    nodes.add(new ArrowFieldNode(count, nullValuesCount(rangeBlock.getDoubleFromBlock())));
                    nodes.add(new ArrowFieldNode(count, nullValuesCount(rangeBlock.getDoubleToBlock())));
                }
            }
        }

        @Override
        public void convert(Block b, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            DoubleRangeBlock block = (DoubleRangeBlock) b;
            int count = block.getPositionCount();

            if (asList) {
                int valueCount = block.getTotalValueCount();
                addListOffsets(bufs, bufWriters, block);
                // Struct validity: empty — items inside a list entry are never null in ES|QL
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);
                // from child: empty validity + float64 data for all value slots
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);
                bufs.add(dummyArrowBuf(valueCount * Double.BYTES));
                bufWriters.add(out -> {
                    DoubleBlock fromBlock = block.getDoubleFromBlock();
                    writeMultivaluedDoubles(out, fromBlock);
                    return (long) valueCount * Double.BYTES;
                });
                // to child: empty validity + float64 data for all value slots
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);
                bufs.add(dummyArrowBuf(valueCount * Double.BYTES));
                bufWriters.add(out -> {
                    DoubleBlock toBlock = block.getDoubleToBlock();
                    writeMultivaluedDoubles(out, toBlock);
                    return (long) valueCount * Double.BYTES;
                });
            } else {
                // Struct validity bitmap
                accumulateVectorValidity(bufs, bufWriters, block, false);
                if (block.areAllValuesNull()) {
                    // ConstantNullBlock's child accessors return `this`, and getDouble() throws.
                    // Write all-null validity and zeroed data for both children.
                    addAllNullChildBuffers(bufs, bufWriters, count);
                    addAllNullChildBuffers(bufs, bufWriters, count);
                } else {
                    addChildBuffers(bufs, bufWriters, count, block.getDoubleFromBlock());
                    addChildBuffers(bufs, bufWriters, count, block.getDoubleToBlock());
                }
            }
        }

        private static void addAllNullChildBuffers(List<ArrowBuf> bufs, List<BufWriter> bufWriters, int count) {
            bufs.add(dummyArrowBuf(bitSetLength(count)));
            bufWriters.add(out -> writeAllFalseValidity(out, count));
            bufs.add(dummyArrowBuf(count * Double.BYTES));
            bufWriters.add(out -> writeZeroes(out, count * Double.BYTES));
        }

        private static void addChildBuffers(List<ArrowBuf> bufs, List<BufWriter> bufWriters, int count, DoubleBlock childBlock) {
            accumulateVectorValidity(bufs, bufWriters, childBlock, false);
            bufs.add(dummyArrowBuf(count * Double.BYTES));
            bufWriters.add(out -> {
                for (int position = 0; position < childBlock.getPositionCount(); position++) {
                    out.writeDoubleLE(childBlock.isNull(position) ? 0 : childBlock.getDouble(childBlock.getFirstValueIndex(position)));
                }
                return (long) count * Double.BYTES;
            });
        }

        private static void writeMultivaluedDoubles(RecyclerBytesStreamOutput out, DoubleBlock block) throws IOException {
            for (int position = 0; position < block.getPositionCount(); position++) {
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    out.writeDoubleLE(block.getDouble(valueIndex));
                }
            }
        }
    }

    /**
     * Keeps aggregate summaries columnar while preserving independently absent metrics as nullable struct children.
     * <p>
     * A row is a null struct only when all four metrics are absent: {@link AggregateMetricDoubleBlock} has no separate
     * validity, so it can't distinguish a missing summary from a summary where every metric is missing. Rows with at
     * least one metric are valid structs whose absent metrics are null children.
     * <p>
     * Summaries are always single-valued, so columns of this type are never encoded as Arrow lists.
     */
    public static class AsAggregateMetricDouble extends BlockArrowFormatter {
        private static final AsFloat64 DOUBLE_FORMATTER = new AsFloat64(DataType.DOUBLE);
        private static final AsInt32 COUNT_FORMATTER = new AsInt32(DataType.INTEGER);

        private final DataType esqlType;

        public AsAggregateMetricDouble(DataType esqlType) {
            super(esqlType, Types.MinorType.STRUCT);
            this.esqlType = esqlType;
        }

        @Override
        public Field arrowField(String name) {
            var doubleType = FieldType.nullable(Types.MinorType.FLOAT8.getType());
            var countType = FieldType.nullable(Types.MinorType.INT.getType());
            return new Field(
                name,
                super.arrowField(name).getFieldType(),
                List.of(
                    new Field("min", doubleType, null),
                    new Field("max", doubleType, null),
                    new Field("sum", doubleType, null),
                    new Field("value_count", countType, null)
                )
            );
        }

        @Override
        public boolean isMultivalued(String columnName, Block block) {
            return forbidMultivalued(columnName, esqlType, block);
        }

        @Override
        public void addFieldNodes(Block block, boolean asList, List<ArrowFieldNode> nodes) {
            AggregateMetricDoubleBlock aggregate = (AggregateMetricDoubleBlock) block;
            DoubleBlock min = aggregate.minBlock();
            DoubleBlock max = aggregate.maxBlock();
            DoubleBlock sum = aggregate.sumBlock();
            IntBlock count = aggregate.countBlock();
            int positions = block.getPositionCount();
            int minNulls = 0;
            int maxNulls = 0;
            int sumNulls = 0;
            int countNulls = 0;
            int summaryNulls = 0;
            if (block.mayHaveNulls()) {
                for (int position = 0; position < positions; position++) {
                    boolean minNull = min.isNull(position);
                    boolean maxNull = max.isNull(position);
                    boolean sumNull = sum.isNull(position);
                    boolean countNull = count.isNull(position);
                    minNulls += minNull ? 1 : 0;
                    maxNulls += maxNull ? 1 : 0;
                    sumNulls += sumNull ? 1 : 0;
                    countNulls += countNull ? 1 : 0;
                    if (minNull && maxNull && sumNull && countNull) {
                        summaryNulls++;
                    }
                }
            }
            // The struct and each of its children have one slot per row.
            nodes.add(new ArrowFieldNode(positions, summaryNulls));
            nodes.add(new ArrowFieldNode(positions, minNulls));
            nodes.add(new ArrowFieldNode(positions, maxNulls));
            nodes.add(new ArrowFieldNode(positions, sumNulls));
            nodes.add(new ArrowFieldNode(positions, countNulls));
        }

        @Override
        public void convert(Block block, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            AggregateMetricDoubleBlock aggregate = (AggregateMetricDoubleBlock) block;
            if (summaryMayBeNull(aggregate)) {
                accumulateVectorValidity(bufs, bufWriters, block, false);
            } else {
                // The block reports nulls if any metric may be null, but a summary is only null when all four are.
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);
            }
            // Child accessors on ConstantNullBlock return the null block itself, which primitive formatters also accept.
            DOUBLE_FORMATTER.convert(aggregate.minBlock(), false, bufs, bufWriters, releasables);
            DOUBLE_FORMATTER.convert(aggregate.maxBlock(), false, bufs, bufWriters, releasables);
            DOUBLE_FORMATTER.convert(aggregate.sumBlock(), false, bufs, bufWriters, releasables);
            COUNT_FORMATTER.convert(aggregate.countBlock(), false, bufs, bufWriters, releasables);
        }

        private static boolean summaryMayBeNull(AggregateMetricDoubleBlock aggregate) {
            return aggregate.minBlock().mayHaveNulls()
                && aggregate.maxBlock().mayHaveNulls()
                && aggregate.sumBlock().mayHaveNulls()
                && aggregate.countBlock().mayHaveNulls();
        }
    }

    /**
     * Conversion of TDigestBlocks to an Arrow struct
     * {@code Struct(min: float64, max: float64, sum: float64, centroids: List<float64>, counts: List<int64>)}.
     * <p>
     * The struct is null for a missing digest. An empty digest is a valid struct with empty lists. {@code min}, {@code max}
     * and {@code sum} are null when absent (which is the case for empty digests), following the Arrow convention that NaN
     * is a value and missing data is null. The lists are never null. The number of samples in the digest is not a field,
     * as it is the sum of {@code counts}.
     * <p>
     * T-Digests are always single-valued, so columns of this type are never encoded as Arrow lists.
     */
    public static class AsTDigest extends BlockArrowFormatter {
        private final DataType esqlType;

        public AsTDigest(DataType esqlType) {
            super(esqlType, Types.MinorType.STRUCT);
            this.esqlType = esqlType;
        }

        @Override
        public Field arrowField(String name) {
            var doubleType = FieldType.nullable(Types.MinorType.FLOAT8.getType());
            return new Field(
                name,
                super.arrowField(name).getFieldType(),
                List.of(
                    new Field("min", doubleType, null),
                    new Field("max", doubleType, null),
                    new Field("sum", doubleType, null),
                    listField("centroids", Types.MinorType.FLOAT8),
                    listField("counts", Types.MinorType.BIGINT)
                )
            );
        }

        private static Field listField(String name, Types.MinorType itemType) {
            return new Field(
                name,
                new FieldType(false, Types.MinorType.LIST.getType(), null),
                List.of(new Field("$data$", new FieldType(false, itemType.getType(), null), null))
            );
        }

        @Override
        public boolean isMultivalued(String columnName, Block block) {
            return forbidMultivalued(columnName, esqlType, block);
        }

        @Override
        public void addFieldNodes(Block block, boolean multivalued, List<ArrowFieldNode> nodes) {
            TDigestBlock digests = (TDigestBlock) block;
            int positions = block.getPositionCount();
            int centroids = centroidOffsets(digests)[positions];
            nodes.add(new ArrowFieldNode(positions, nullValuesCount(block))); // struct
            nodes.add(new ArrowFieldNode(positions, absentCount(digests, TDigestHolder::getMin))); // min
            nodes.add(new ArrowFieldNode(positions, absentCount(digests, TDigestHolder::getMax))); // max
            nodes.add(new ArrowFieldNode(positions, absentCount(digests, TDigestHolder::getSum))); // sum
            nodes.add(new ArrowFieldNode(positions, 0)); // centroids list
            nodes.add(new ArrowFieldNode(centroids, 0)); // centroids values
            nodes.add(new ArrowFieldNode(positions, 0)); // counts list
            nodes.add(new ArrowFieldNode(centroids, 0)); // counts values
        }

        @Override
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            TDigestBlock block = (TDigestBlock) b;
            int[] offsets = centroidOffsets(block);

            // Struct validity
            accumulateVectorValidity(bufs, bufWriters, block, false);

            addDoubleChild(bufs, bufWriters, block, TDigestHolder::getMin);
            addDoubleChild(bufs, bufWriters, block, TDigestHolder::getMax);
            addDoubleChild(bufs, bufWriters, block, TDigestHolder::getSum);

            addListChild(bufs, bufWriters, block, offsets, Double.BYTES, (out, it) -> out.writeDoubleLE(it.currentMean()));
            addListChild(bufs, bufWriters, block, offsets, Long.BYTES, (out, it) -> out.writeLongLE(it.currentCount()));
        }

        /**
         * Offsets of each position's centroids in the list values, {@code positionCount + 1} entries. Null digests have no centroids.
         */
        private static int[] centroidOffsets(TDigestBlock block) {
            int positions = block.getPositionCount();
            int[] offsets = new int[positions + 1];
            TDigestHolder scratch = new TDigestHolder();
            long total = 0;
            for (int position = 0; position < positions; position++) {
                if (block.isNull(position) == false) {
                    total += block.getTDigestHolder(block.getFirstValueIndex(position), scratch).centroidCount();
                }
                offsets[position + 1] = Math.toIntExact(total);
            }
            return offsets;
        }

        /** The value of a float64 child at a position, {@code NaN} if the digest is null or doesn't have that value. */
        private static double childValue(TDigestBlock block, int position, ToDoubleFunction<TDigestHolder> getter, TDigestHolder scratch) {
            if (block.isNull(position)) {
                return Double.NaN;
            }
            return getter.applyAsDouble(block.getTDigestHolder(block.getFirstValueIndex(position), scratch));
        }

        private static int absentCount(TDigestBlock block, ToDoubleFunction<TDigestHolder> getter) {
            TDigestHolder scratch = new TDigestHolder();
            int count = 0;
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (Double.isNaN(childValue(block, position, getter, scratch))) {
                    count++;
                }
            }
            return count;
        }

        /** A nullable float64 child: absent values are null, with a zero placeholder in the values buffer. */
        private static void addDoubleChild(
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            TDigestBlock block,
            ToDoubleFunction<TDigestHolder> getter
        ) {
            int positions = block.getPositionCount();
            if (absentCount(block, getter) == 0) {
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);
            } else {
                bufs.add(dummyArrowBuf(bitSetLength(positions)));
                bufWriters.add(out -> {
                    TDigestHolder scratch = new TDigestHolder();
                    BitSet bits = new BitSet(positions);
                    for (int position = 0; position < positions; position++) {
                        if (Double.isNaN(childValue(block, position, getter, scratch)) == false) {
                            bits.set(position);
                        }
                    }
                    return writeBitSet(out, bits, positions);
                });
            }
            bufs.add(dummyArrowBuf((long) positions * Double.BYTES));
            bufWriters.add(out -> {
                TDigestHolder scratch = new TDigestHolder();
                for (int position = 0; position < positions; position++) {
                    double value = childValue(block, position, getter, scratch);
                    out.writeDoubleLE(Double.isNaN(value) ? 0 : value);
                }
                return (long) positions * Double.BYTES;
            });
        }

        @FunctionalInterface
        private interface CentroidWriter {
            void write(RecyclerBytesStreamOutput out, EncodedTDigest.CentroidIterator centroid) throws IOException;
        }

        /** A non-nullable list child: no validity buffers (null digests are empty lists), offsets, then the values. */
        private static void addListChild(
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            TDigestBlock block,
            int[] offsets,
            int itemBytes,
            CentroidWriter itemWriter
        ) {
            int positions = block.getPositionCount();
            int centroids = offsets[positions];

            // List validity
            bufs.add(dummyArrowBuf(0));
            bufWriters.add(out -> 0);

            // List offsets
            long offsetsLen = (long) Integer.BYTES * (positions + 1);
            bufs.add(dummyArrowBuf(offsetsLen));
            bufWriters.add(out -> {
                for (int offset : offsets) {
                    out.writeIntLE(offset);
                }
                return offsetsLen;
            });

            // Values validity
            bufs.add(dummyArrowBuf(0));
            bufWriters.add(out -> 0);

            // Values
            long valuesLen = (long) centroids * itemBytes;
            bufs.add(dummyArrowBuf(valuesLen));
            bufWriters.add(out -> {
                TDigestHolder scratch = new TDigestHolder();
                EncodedTDigest decoder = new EncodedTDigest();
                for (int position = 0; position < positions; position++) {
                    if (block.isNull(position)) {
                        continue;
                    }
                    BytesRef encoded = block.getTDigestHolder(block.getFirstValueIndex(position), scratch).getEncodedDigest();
                    if (encoded.length == 0) {
                        continue;
                    }
                    decoder.reset(encoded);
                    var it = decoder.centroidIterator();
                    while (it.next()) {
                        itemWriter.write(out, it);
                    }
                }
                return valuesLen;
            });
        }
    }

    /**
     * Conversion of {@code geotile} blocks to an Arrow struct with {@code zoom:uint8}, {@code x:uint32} and {@code y:uint32} fields.
     * <p>
     * A geotile can be null but its fields never are, so the children are declared non-nullable and have no validity buffer.
     * Null geotiles have their fields set to zero.
     * </p>
     */
    public static class AsGeoTile extends BlockArrowFormatter {

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

        public AsGeoTile(DataType esqlType) {
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
        public void addFieldNodes(Block block, boolean multivalued, List<ArrowFieldNode> nodes) {
            int count = block.getPositionCount();
            if (multivalued) {
                int valueCount = block.getTotalValueCount();
                nodes.add(new ArrowFieldNode(count, nullValuesCount(block))); // list
                nodes.add(new ArrowFieldNode(valueCount, 0)); // struct (items have no independent nulls)
                for (int i = 0; i < Component.values().length; i++) {
                    nodes.add(new ArrowFieldNode(valueCount, 0));
                }
            } else {
                nodes.add(new ArrowFieldNode(count, nullValuesCount(block)));
                for (int i = 0; i < Component.values().length; i++) {
                    nodes.add(new ArrowFieldNode(count, 0));
                }
            }
        }

        @Override
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            if (multivalued) {
                addListOffsets(bufs, bufWriters, b);
            }
            // Struct validity. Empty if in a list, as the list carries the nulls.
            accumulateVectorValidity(bufs, bufWriters, b, multivalued);

            for (Component component : Component.values()) {
                // Non-nullable child: empty validity buffer
                bufs.add(dummyArrowBuf(0));
                bufWriters.add(out -> 0);

                int byteCount = arrowValueCount(b, multivalued) * component.width;
                bufs.add(dummyArrowBuf(byteCount));
                bufWriters.add(out -> {
                    if (b.areAllValuesNull()) {
                        // ConstantNullBlock is not a LongBlock
                        return writeZeroes(out, byteCount);
                    }
                    LongBlock block = (LongBlock) b;
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        if (block.isNull(position)) {
                            if (multivalued == false) {
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

    public static class AsNull extends BlockArrowFormatter {
        public AsNull(DataType esqlType) {
            super(esqlType, Types.MinorType.NULL);
        }

        @Override
        public void convert(Block block, boolean asList, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            // Null vector in arrow has no associated buffers
            // See https://arrow.apache.org/docs/format/Columnar.html#null-layout
        }
    }

    // Create a dummy ArrowBuf used for size accounting purposes.
    private static ArrowBuf dummyArrowBuf(long size) {
        return new ArrowBuf(null, null, 0, 0).writerIndex(size);
    }

    // Length in bytes of a validity buffer
    private static int bitSetLength(int totalValues) {
        return (totalValues + 7) / 8;
    }

    private static int arrowValueCount(Block block, boolean asList) {
        return asList ? block.getTotalValueCount() : block.getPositionCount();
    }

    private static void accumulateVectorValidity(List<ArrowBuf> bufs, List<BufWriter> bufWriters, Block b, boolean asList) {
        // For list encoding, validities are output in the parent Arrow List buffer. The values themselves do not contain nulls.
        if (asList || b.mayHaveNulls() == false) {
            // Arrow IPC allows a compact form for "all true" validities using an empty buffer.
            bufs.add(dummyArrowBuf(0));
            bufWriters.add(w -> 0);
            return;
        }

        int valueCount = b.getPositionCount();
        bufs.add(dummyArrowBuf(bitSetLength(valueCount)));
        bufWriters.add(out -> {
            if (b.areAllValuesNull()) {
                return writeAllFalseValidity(out, valueCount);
            } else {
                return writeValidities(out, b, valueCount);
            }
        });
    }

    private static long writeAllTrueValidity(RecyclerBytesStreamOutput out, int valueCount) {
        int allOnesCount = valueCount / 8;
        for (int i = 0; i < allOnesCount; i++) {
            out.writeByte((byte) 0xff);
        }
        int remaining = valueCount % 8;
        if (remaining == 0) {
            return allOnesCount;
        }
        out.writeByte((byte) ((1 << remaining) - 1));
        return allOnesCount + 1;
    }

    private static long writeAllFalseValidity(RecyclerBytesStreamOutput out, int valueCount) {
        int count = bitSetLength(valueCount);
        for (int i = 0; i < count; i++) {
            out.writeByte((byte) 0x00);
        }
        return count;
    }

    private static long writeValidities(RecyclerBytesStreamOutput out, Block block, int valueCount) {
        BitSet bits = new BitSet(valueCount);
        for (int i = 0; i < block.getPositionCount(); i++) {
            // isNull uses position indices, not value indices.
            if (block.isNull(i) == false) {
                bits.set(i);
            }
        }
        return writeBitSet(out, bits, valueCount);
    }

    private static long writeBitSet(RecyclerBytesStreamOutput out, BitSet bits, int bitCount) {
        byte[] bytes = bits.toByteArray();
        out.writeBytes(bytes, 0, bytes.length);

        // toByteArray will return bytes up to the last bit set. It may therefore
        // have a length lower than what is needed to actually store bitCount bits.
        int expectedLength = bitSetLength(bitCount);
        writeZeroes(out, expectedLength - bytes.length);

        return expectedLength;
    }

    private static long writeZeroes(RecyclerBytesStreamOutput out, int byteCount) {
        for (int i = 0; i < byteCount; i++) {
            out.writeByte((byte) 0);
        }
        return byteCount;
    }

    private static void addListOffsets(List<ArrowBuf> bufs, List<BufWriter> bufWriters, Block block) {
        // Add validity buffer
        accumulateVectorValidity(bufs, bufWriters, block, false);

        // Add offsets buffer
        int bufferLen = Integer.BYTES * (block.getPositionCount() + 1);

        bufs.add(dummyArrowBuf(bufferLen));
        bufWriters.add(out -> {
            int offset = 0;
            out.writeIntLE(offset);
            for (int position = 0; position < block.getPositionCount(); position++) {
                offset += block.getValueCount(position);
                out.writeIntLE(offset);
            }

            return bufferLen;
        });
    }
}
