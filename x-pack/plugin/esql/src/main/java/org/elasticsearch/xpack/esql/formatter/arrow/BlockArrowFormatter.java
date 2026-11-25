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
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.exponentialhistogram.BucketIterator;
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
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

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
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            FloatBlock block = (FloatBlock) b;
            assert asList;
            addSimpleFieldNodes(block, true, nodes);
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
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            if (asList) {
                nodes.add(new ArrowFieldNode(b.getPositionCount(), b.nullValuesCount()));
                addListOffsets(bufs, bufWriters, b);
            }
            convertValueVector(b, asList, nodes, bufs, bufWriters);
        }

        /**
         * Emits the field node, validity bitmap, and value buffer for this double vector without an enclosing Arrow list.
         * When {@code flattened} is {@code true}, null positions are skipped and values are emitted flat (used both for
         * list value vectors and for struct children inside a list).
         */
        void convertValueVector(Block b, boolean flattened, List<ArrowFieldNode> nodes, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            DoubleBlock block = (DoubleBlock) b;
            nodes.add(
                flattened
                    ? new ArrowFieldNode(block.getTotalValueCount(), 0)
                    : new ArrowFieldNode(block.getPositionCount(), block.nullValuesCount())
            );
            accumulateVectorValidity(bufs, bufWriters, block, flattened);

            long byteSize = (long) Double.BYTES * arrowValueCount(block, flattened);
            bufs.add(dummyArrowBuf(byteSize));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (flattened == false) {
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
                return byteSize;
            });
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
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            IntBlock block = (IntBlock) b;
            addSimpleFieldNodes(block, asList, nodes);

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            long byteSize = (long) Integer.BYTES * arrowValueCount(block, asList);
            bufs.add(dummyArrowBuf(byteSize));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
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
                return byteSize;
            });
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
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            if (asList) {
                nodes.add(new ArrowFieldNode(b.getPositionCount(), b.nullValuesCount()));
                addListOffsets(bufs, bufWriters, b);
            }
            convertValueVector(b, asList, nodes, bufs, bufWriters);
        }

        /**
         * Emits the field node, validity bitmap, and value buffer for this long vector without an enclosing Arrow list.
         * When {@code flattened} is {@code true}, null positions are skipped and values are emitted flat (used both for
         * list value vectors and for struct children inside a list).
         */
        void convertValueVector(Block b, boolean flattened, List<ArrowFieldNode> nodes, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            LongBlock block = (LongBlock) b;
            nodes.add(
                flattened
                    ? new ArrowFieldNode(block.getTotalValueCount(), 0)
                    : new ArrowFieldNode(block.getPositionCount(), block.nullValuesCount())
            );
            accumulateVectorValidity(bufs, bufWriters, block, flattened);

            long byteSize = (long) Long.BYTES * arrowValueCount(block, flattened);
            bufs.add(dummyArrowBuf(byteSize));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (flattened == false) {
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
                return byteSize;
            });
        }

        protected long arrowValue(long blockValue) {
            return blockValue;
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
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            BooleanBlock block = (BooleanBlock) b;
            addSimpleFieldNodes(block, asList, nodes);

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            bufs.add(dummyArrowBuf(bitSetLength(arrowValueCount(block, asList))));
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

                return writeBitSet(out, bits, count);
            });
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
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            BytesRefBlock block = (BytesRefBlock) b;
            addSimpleFieldNodes(block, asList, nodes);

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            // Offsets vector
            bufs.add(dummyArrowBuf(offsetVectorByteSize(block, asList)));

            bufWriters.add(out -> writeOffsets(out, block, asList));

            // Data vector
            bufs.add(dummyArrowBuf(dataVectorByteSize(block)));

            bufWriters.add(out -> writeData(out, block));
        }

        private static long offsetVectorByteSize(BytesRefBlock block, boolean asList) {
            return (long) Integer.BYTES * (arrowValueCount(block, asList) + 1L);
        }

        private static long writeOffsets(RecyclerBytesStreamOutput out, BytesRefBlock block, boolean asList) throws IOException {
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
                    offset = Math.addExact(offset, block.getBytesRef(valueIndex, scratch).length);
                    out.writeIntLE(offset);
                }
            }
            return offsetVectorByteSize(block, asList);
        }

        private static long writeData(RecyclerBytesStreamOutput out, BytesRefBlock block) throws IOException {
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
                    BytesRef value = block.getBytesRef(valueIndex, scratch);
                    out.write(value.bytes, value.offset, value.length);
                }
            }
            return out.position() - startPosition;
        }

        private static long dataVectorByteSize(BytesRefBlock block) {
            // TODO we can probably get the length from the vector without all this sum
            int length = 0;
            BytesRef scratch = new BytesRef();
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    continue;
                }
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    // Arrow's 32-bit offsets cap the data buffer at Integer.MAX_VALUE bytes.
                    length = Math.addExact(length, block.getBytesRef(valueIndex, scratch).length);
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

        protected TransformedBytesRef(
            DataType esqlType,
            Types.MinorType minorType,
            Map<String, String> extraMetadata,
            BiFunction<BytesRef, BytesRef, BytesRef> valueConverter
        ) {
            super(esqlType, minorType, extraMetadata);
            this.valueConverter = valueConverter;
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
            if (b.areAllValuesNull()) {
                super.convert(b, asList, nodes, bufs, bufWriters, releasables);
                return;
            }
            BytesRefBlock transformed = transformValues((BytesRefBlock) b);
            releasables.add(transformed);
            super.convert(transformed, asList, nodes, bufs, bufWriters, releasables);
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
                        BytesRef bytes = block.getBytesRef(start, scratch);
                        if (bytes.length == 0) {
                            if (emptyAsNull()) {
                                builder.appendNull();
                            } else {
                                builder.appendBytesRef(bytes);
                            }
                        } else {
                            builder.appendBytesRef(valueConverter.apply(bytes, scratch));
                        }
                    } else if (emptyAsNull()) {
                        appendNonEmptyMultivaluedPosition(builder, block, start, end, scratch);
                    } else {
                        builder.beginPositionEntry();
                        for (int valueIndex = start; valueIndex < end; valueIndex++) {
                            convertAndAppend(builder, block, valueIndex, scratch);
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private void appendNonEmptyMultivaluedPosition(
            BytesRefBlock.Builder builder,
            BytesRefBlock block,
            int start,
            int end,
            BytesRef scratch
        ) {
            boolean opened = false;
            for (int valueIndex = start; valueIndex < end; valueIndex++) {
                BytesRef bytes = block.getBytesRef(valueIndex, scratch);
                if (bytes.length == 0) {
                    continue;
                }
                if (opened == false) {
                    builder.beginPositionEntry();
                    opened = true;
                }
                builder.appendBytesRef(valueConverter.apply(bytes, scratch));
            }
            if (opened) {
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }

        /**
         * Whether zero-length values must be treated as null/absent rather than passed through unconverted. Needed when an
         * empty value isn't valid for the target type (e.g. {@code arrow.json}). Single empty values become null; in
         * multivalued positions empty elements are skipped, and a position whose elements are all empty becomes null.
         */
        protected boolean emptyAsNull() {
            return false;
        }

        private void convertAndAppend(BytesRefBlock.Builder builder, BytesRefBlock block, int valueIndex, BytesRef scratch) {
            BytesRef bytes = block.getBytesRef(valueIndex, scratch);
            if (bytes.length != 0) {
                bytes = valueConverter.apply(bytes, scratch);
            }
            builder.appendBytesRef(bytes);
        }
    }

    /**
     * Values stored as xcontent in a {@code BytesRefBlock}, converted to JSON text and exposed as a variable-size string vector
     * tagged with the <a href="https://arrow.apache.org/docs/format/CanonicalExtensions.html#json">{@code arrow.json}</a>
     * Arrow extension type.
     */
    public static class AsJson extends TransformedBytesRef {
        public static final String ARROW_JSON = "arrow.json";

        public AsJson(DataType esqlType) {
            super(
                esqlType,
                Types.MinorType.VARCHAR,
                Map.of(
                    ExtensionType.EXTENSION_METADATA_KEY_NAME,
                    ARROW_JSON,
                    // From https://arrow.apache.org/docs/format/CanonicalExtensions.html#json
                    // "Metadata is either an empty string or a JSON string with an empty object. In the future, additional fields may be
                    // added, but they are not required to interpret the array."
                    ExtensionType.EXTENSION_METADATA_KEY_METADATA,
                    ""
                ),
                ValueConversions::sourceToJson
            );
        }

        /** An empty string isn't valid JSON, so it is exposed as null. */
        @Override
        protected boolean emptyAsNull() {
            return true;
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
            // From https://geoarrow.org/extension-types.html
            // "If all metadata keys are omitted, the ARROW:extension:metadata should also be omitted"
            if (geographic) {
                metadata.put(ExtensionType.EXTENSION_METADATA_KEY_METADATA, GEO_CRS_METADATA);
            }
            return metadata;
        }
    }

    /**
     * Formatter for ES|QL range blocks encoded as an Arrow struct with nullable {@code "from"} and {@code "to"} fields,
     * delegating child bound encoding to scalar formatters.
     */
    static class AsRange extends BlockArrowFormatter {

        private static final AsInt64 DATE_BOUND_FORMATTER = new AsInt64(DataType.DATETIME, Types.MinorType.TIMESTAMPMILLI);
        private static final AsFloat64 DOUBLE_BOUND_FORMATTER = new AsFloat64(DataType.DOUBLE);

        @FunctionalInterface
        private interface BoundVectorConverter {
            void convert(Block boundBlock, boolean flattened, List<ArrowFieldNode> nodes, List<ArrowBuf> bufs, List<BufWriter> bufWriters);
        }

        private final Types.MinorType childType;
        private final Function<Block, Block> fromBlock;
        private final Function<Block, Block> toBlock;
        private final BoundVectorConverter boundConverter;

        private AsRange(
            DataType esqlType,
            Types.MinorType childType,
            Function<Block, Block> fromBlock,
            Function<Block, Block> toBlock,
            BoundVectorConverter boundConverter
        ) {
            super(esqlType, Types.MinorType.STRUCT);
            this.childType = childType;
            this.fromBlock = fromBlock;
            this.toBlock = toBlock;
            this.boundConverter = boundConverter;
        }

        static AsRange dateRange(DataType esqlType) {
            return new AsRange(
                esqlType,
                Types.MinorType.TIMESTAMPMILLI,
                block -> ((LongRangeBlock) block).getFromBlock(),
                block -> ((LongRangeBlock) block).getToBlock(),
                DATE_BOUND_FORMATTER::convertValueVector
            );
        }

        static AsRange doubleRange(DataType esqlType) {
            return new AsRange(
                esqlType,
                Types.MinorType.FLOAT8,
                block -> ((DoubleRangeBlock) block).getDoubleFromBlock(),
                block -> ((DoubleRangeBlock) block).getDoubleToBlock(),
                DOUBLE_BOUND_FORMATTER::convertValueVector
            );
        }

        @Override
        public Field arrowField(String name) {
            Field superField = super.arrowField(name);
            var fromField = new Field("from", FieldType.nullable(childType.getType()), null);
            var toField = new Field("to", FieldType.nullable(childType.getType()), null);
            return new Field(name, superField.getFieldType(), List.of(fromField, toField));
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
            if (asList) {
                addListOffsets(bufs, bufWriters, b);
            }
            accumulateVectorValidity(bufs, bufWriters, b, asList);

            Block from = fromBlock.apply(b);
            Block to = toBlock.apply(b);
            assert arrowValueCount(from, asList) == arrowValueCount(b, asList)
                : "Range 'from' value count ["
                    + arrowValueCount(from, asList)
                    + "] differs from parent ["
                    + arrowValueCount(b, asList)
                    + "]";
            assert arrowValueCount(to, asList) == arrowValueCount(b, asList)
                : "Range 'to' value count [" + arrowValueCount(to, asList) + "] differs from parent [" + arrowValueCount(b, asList) + "]";
            boundConverter.convert(from, asList, nodes, bufs, bufWriters);
            boundConverter.convert(to, asList, nodes, bufs, bufWriters);
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
        public void convert(
            Block block,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            assert asList == false : "aggregate_metric_double columns cannot be encoded as Arrow lists";
            AggregateMetricDoubleBlock aggregate = (AggregateMetricDoubleBlock) block;
            int positions = block.getPositionCount();
            if (summaryMayBeNull(aggregate)) {
                nodes.add(new ArrowFieldNode(positions, block.nullValuesCount()));
                accumulateVectorValidity(bufs, bufWriters, block, false);
            } else {
                // The block reports nulls if any metric may be null, but a summary is only null when all four are.
                nodes.add(new ArrowFieldNode(positions, 0));
                addEmptyValidity(bufs, bufWriters);
            }
            // Child accessors on ConstantNullBlock return the null block itself, which primitive formatters also accept.
            DOUBLE_FORMATTER.convert(aggregate.minBlock(), false, nodes, bufs, bufWriters, releasables);
            DOUBLE_FORMATTER.convert(aggregate.maxBlock(), false, nodes, bufs, bufWriters, releasables);
            DOUBLE_FORMATTER.convert(aggregate.sumBlock(), false, nodes, bufs, bufWriters, releasables);
            COUNT_FORMATTER.convert(aggregate.countBlock(), false, nodes, bufs, bufWriters, releasables);
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
                    nonNullableListField("centroids", Types.MinorType.FLOAT8),
                    nonNullableListField("counts", Types.MinorType.BIGINT)
                )
            );
        }

        @Override
        public boolean isMultivalued(String columnName, Block block) {
            return forbidMultivalued(columnName, esqlType, block);
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
            assert asList == false : "tdigest columns cannot be encoded as Arrow lists";
            TDigestBlock block = (TDigestBlock) b;
            int positions = block.getPositionCount();
            int[] offsets = centroidOffsets(block);
            int centroids = offsets[positions];
            int minAbsent = absentCount(block, TDigestHolder::getMin);
            int maxAbsent = absentCount(block, TDigestHolder::getMax);
            int sumAbsent = absentCount(block, TDigestHolder::getSum);

            nodes.add(new ArrowFieldNode(positions, block.nullValuesCount())); // struct
            nodes.add(new ArrowFieldNode(positions, minAbsent)); // min
            nodes.add(new ArrowFieldNode(positions, maxAbsent)); // max
            nodes.add(new ArrowFieldNode(positions, sumAbsent)); // sum
            nodes.add(new ArrowFieldNode(positions, 0)); // centroids list
            nodes.add(new ArrowFieldNode(centroids, 0)); // centroids values
            nodes.add(new ArrowFieldNode(positions, 0)); // counts list
            nodes.add(new ArrowFieldNode(centroids, 0)); // counts values

            // Struct validity
            accumulateVectorValidity(bufs, bufWriters, block, false);

            addDoubleChild(bufs, bufWriters, block, TDigestHolder::getMin, minAbsent);
            addDoubleChild(bufs, bufWriters, block, TDigestHolder::getMax, maxAbsent);
            addDoubleChild(bufs, bufWriters, block, TDigestHolder::getSum, sumAbsent);

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
            ToDoubleFunction<TDigestHolder> getter,
            int absentCount
        ) {
            int positions = block.getPositionCount();
            if (absentCount == 0) {
                addEmptyValidity(bufs, bufWriters);
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
            addNonNullableListBuffers(bufs, bufWriters, offsets, itemBytes, out -> {
                TDigestHolder scratch = new TDigestHolder();
                EncodedTDigest decoder = new EncodedTDigest();
                long bytes = 0;
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
                        bytes += itemBytes;
                    }
                }
                return bytes;
            });
        }
    }

    /**
     * Classic histograms store bucket upper-limit values and counts using the {@link EncodedTDigest} binary format.
     * Preserve their values and counts as two parallel Arrow lists; an empty histogram remains a valid struct with empty
     * lists, distinct from a null histogram.
     */
    public static class AsHistogram extends BlockArrowFormatter {
        public AsHistogram(DataType esqlType) {
            super(esqlType, Types.MinorType.STRUCT);
        }

        @Override
        public Field arrowField(String name) {
            return new Field(
                name,
                super.arrowField(name).getFieldType(),
                List.of(nonNullableListField("values", Types.MinorType.FLOAT8), nonNullableListField("counts", Types.MinorType.BIGINT))
            );
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
            BytesRefBlock block = (BytesRefBlock) b;
            int[] indexes = itemIndexes(block, asList);
            int items = indexes.length;
            int[] offsets = offsets(block, indexes);
            int values = offsets[items];
            addSimpleFieldNodes(block, asList, nodes);
            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            nodes.add(new ArrowFieldNode(items, 0));
            nodes.add(new ArrowFieldNode(values, 0));
            nodes.add(new ArrowFieldNode(items, 0));
            nodes.add(new ArrowFieldNode(values, 0));
            accumulateVectorValidity(bufs, bufWriters, block, asList);
            addNonNullableListBuffers(bufs, bufWriters, offsets, Double.BYTES, out -> writeBuckets(block, indexes, out, true));
            addNonNullableListBuffers(bufs, bufWriters, offsets, Long.BYTES, out -> writeBuckets(block, indexes, out, false));
        }

        private static int[] offsets(BytesRefBlock block, int[] indexes) {
            int[] offsets = new int[indexes.length + 1];
            EncodedTDigest decoder = new EncodedTDigest();
            BytesRef scratch = new BytesRef();
            long count = 0;
            for (int item = 0; item < indexes.length; item++) {
                if (indexes[item] >= 0) {
                    decoder.reset(block.getBytesRef(indexes[item], scratch));
                    count += decoder.centroidCount();
                }
                offsets[item + 1] = Math.toIntExact(count);
            }
            return offsets;
        }

        private static long writeBuckets(BytesRefBlock block, int[] indexes, RecyclerBytesStreamOutput out, boolean values)
            throws IOException {
            EncodedTDigest decoder = new EncodedTDigest();
            BytesRef scratch = new BytesRef();
            long bytes = 0;
            for (int index : indexes) {
                if (index < 0) {
                    continue;
                }
                decoder.reset(block.getBytesRef(index, scratch));
                var it = decoder.centroidIterator();
                while (it.next()) {
                    if (values) {
                        out.writeDoubleLE(it.currentMean());
                        bytes += Double.BYTES;
                    } else {
                        out.writeLongLE(it.currentCount());
                        bytes += Long.BYTES;
                    }
                }
            }
            return bytes;
        }
    }

    /**
     * Conversion of {@link ExponentialHistogramBlock} to an Arrow struct matching the {@code exponential_histogram} field
     * structure: {@code Struct(scale: int32, sum: float64, min: float64, max: float64, zero: Struct(count: int64,
     * threshold: float64), positive: Struct(indices: List<int64>, counts: List<int64>), negative: Struct(indices:
     * List<int64>, counts: List<int64>))}.
     * <p>
     * The struct is null for a missing histogram. An empty histogram is a valid struct with empty bucket lists and null
     * {@code sum}, {@code min}, and {@code max} (following the Arrow convention that missing values are null rather than
     * NaN). The {@code scale}, {@code zero}, {@code positive}, and {@code negative} children and their nested fields and
     * lists are non-nullable. Total sample count is omitted as it is {@code zero.count + sum(positive.counts) +
     * sum(negative.counts)}. Multivalued columns are encoded as a list of structs.
     */
    public static class AsExponentialHistogram extends BlockArrowFormatter {
        public AsExponentialHistogram(DataType esqlType) {
            super(esqlType, Types.MinorType.STRUCT);
        }

        @Override
        public Field arrowField(String name) {
            var intType = new FieldType(false, Types.MinorType.INT.getType(), null);
            var longType = new FieldType(false, Types.MinorType.BIGINT.getType(), null);
            var doubleType = new FieldType(false, Types.MinorType.FLOAT8.getType(), null);
            var nullableDouble = FieldType.nullable(Types.MinorType.FLOAT8.getType());
            var zero = new Field(
                "zero",
                new FieldType(false, Types.MinorType.STRUCT.getType(), null),
                List.of(new Field("count", longType, null), new Field("threshold", doubleType, null))
            );
            var buckets = List.of(
                nonNullableListField("indices", Types.MinorType.BIGINT),
                nonNullableListField("counts", Types.MinorType.BIGINT)
            );
            var structType = new FieldType(false, Types.MinorType.STRUCT.getType(), null);
            return new Field(
                name,
                super.arrowField(name).getFieldType(),
                List.of(
                    new Field("scale", intType, null),
                    new Field("sum", nullableDouble, null),
                    new Field("min", nullableDouble, null),
                    new Field("max", nullableDouble, null),
                    zero,
                    new Field("positive", structType, buckets),
                    new Field("negative", structType, buckets)
                )
            );
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
            ExponentialHistogramBlock block = (ExponentialHistogramBlock) b;
            int[] indexes = itemIndexes(block, asList);
            int items = indexes.length;

            int[] scales = new int[items];
            long[] zeroCounts = new long[items];
            double[] zeroThresholds = new double[items];
            int[] positiveOffsets = new int[items + 1];
            int[] negativeOffsets = new int[items + 1];
            LongVector positiveIndices;
            LongVector positiveCounts;
            LongVector negativeIndices;
            LongVector negativeCounts;
            try (
                LongVector.Builder positiveIndicesBuilder = block.blockFactory().newLongVectorBuilder(items);
                LongVector.Builder positiveCountsBuilder = block.blockFactory().newLongVectorBuilder(items);
                LongVector.Builder negativeIndicesBuilder = block.blockFactory().newLongVectorBuilder(items);
                LongVector.Builder negativeCountsBuilder = block.blockFactory().newLongVectorBuilder(items)
            ) {
                ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
                int positiveTotal = 0;
                int negativeTotal = 0;
                for (int item = 0; item < items; item++) {
                    int index = indexes[item];
                    if (index >= 0) {
                        var histogram = block.getExponentialHistogram(index, scratch);
                        scales[item] = histogram.scale();
                        var zero = histogram.zeroBucket();
                        zeroCounts[item] = zero.count();
                        double threshold = zero.zeroThreshold();
                        zeroThresholds[item] = Double.isNaN(threshold) ? 0 : threshold;
                        positiveTotal = appendBuckets(
                            histogram.positiveBuckets().iterator(),
                            positiveIndicesBuilder,
                            positiveCountsBuilder,
                            positiveTotal
                        );
                        negativeTotal = appendBuckets(
                            histogram.negativeBuckets().iterator(),
                            negativeIndicesBuilder,
                            negativeCountsBuilder,
                            negativeTotal
                        );
                    }
                    positiveOffsets[item + 1] = positiveTotal;
                    negativeOffsets[item + 1] = negativeTotal;
                }
                positiveIndices = positiveIndicesBuilder.build();
                releasables.add(positiveIndices);
                positiveCounts = positiveCountsBuilder.build();
                releasables.add(positiveCounts);
                negativeIndices = negativeIndicesBuilder.build();
                releasables.add(negativeIndices);
                negativeCounts = negativeCountsBuilder.build();
                releasables.add(negativeCounts);
            }

            DoubleBlock sumBlock = block.getSumBlock();
            DoubleBlock minBlock = block.getMinBlock();
            DoubleBlock maxBlock = block.getMaxBlock();
            int sumAbsent = absentCount(sumBlock, indexes);
            int minAbsent = absentCount(minBlock, indexes);
            int maxAbsent = absentCount(maxBlock, indexes);

            addSimpleFieldNodes(block, asList, nodes);
            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            nodes.add(new ArrowFieldNode(items, 0)); // scale
            addIntChild(bufs, bufWriters, scales);

            nodes.add(new ArrowFieldNode(items, sumAbsent)); // sum
            addDoubleSubBlockChild(bufs, bufWriters, sumBlock, indexes, sumAbsent);

            nodes.add(new ArrowFieldNode(items, minAbsent)); // min
            addDoubleSubBlockChild(bufs, bufWriters, minBlock, indexes, minAbsent);

            nodes.add(new ArrowFieldNode(items, maxAbsent)); // max
            addDoubleSubBlockChild(bufs, bufWriters, maxBlock, indexes, maxAbsent);

            nodes.add(new ArrowFieldNode(items, 0)); // zero
            nodes.add(new ArrowFieldNode(items, 0)); // zero.count
            nodes.add(new ArrowFieldNode(items, 0)); // zero.threshold
            addEmptyValidity(bufs, bufWriters); // zero struct
            addLongChild(bufs, bufWriters, zeroCounts);
            addDoubleChild(bufs, bufWriters, zeroThresholds);

            addBucketNodes(nodes, items, positiveIndices.getPositionCount());
            addBucketBuffers(bufs, bufWriters, positiveOffsets, positiveIndices, positiveCounts);

            addBucketNodes(nodes, items, negativeIndices.getPositionCount());
            addBucketBuffers(bufs, bufWriters, negativeOffsets, negativeIndices, negativeCounts);
        }

        private static int appendBuckets(BucketIterator buckets, LongVector.Builder indices, LongVector.Builder counts, int total) {
            while (buckets.hasNext()) {
                indices.appendLong(buckets.peekIndex());
                counts.appendLong(buckets.peekCount());
                total = Math.addExact(total, 1);
                buckets.advance();
            }
            return total;
        }

        private static void addBucketNodes(List<ArrowFieldNode> nodes, int positions, int buckets) {
            nodes.add(new ArrowFieldNode(positions, 0)); // bucket struct
            nodes.add(new ArrowFieldNode(positions, 0)); // indices list
            nodes.add(new ArrowFieldNode(buckets, 0)); // indices
            nodes.add(new ArrowFieldNode(positions, 0)); // counts list
            nodes.add(new ArrowFieldNode(buckets, 0)); // counts
        }

        private static void addBucketBuffers(
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            int[] offsets,
            LongVector indices,
            LongVector counts
        ) {
            addEmptyValidity(bufs, bufWriters); // bucket struct
            addNonNullableListBuffers(bufs, bufWriters, offsets, Long.BYTES, out -> writeLongs(out, indices));
            addNonNullableListBuffers(bufs, bufWriters, offsets, Long.BYTES, out -> writeLongs(out, counts));
        }

        private static int absentCount(DoubleBlock subBlock, int[] indexes) {
            if (subBlock.mayHaveNulls() == false) {
                return 0;
            }
            if (subBlock.areAllValuesNull()) {
                return indexes.length;
            }
            int count = 0;
            for (int index : indexes) {
                if (index < 0 || subBlock.isNull(index)) {
                    count++;
                }
            }
            return count;
        }

        private static void addDoubleSubBlockChild(
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            DoubleBlock subBlock,
            int[] indexes,
            int absentCount
        ) {
            int items = indexes.length;
            if (absentCount == 0) {
                addEmptyValidity(bufs, bufWriters);
            } else {
                bufs.add(dummyArrowBuf(bitSetLength(items)));
                bufWriters.add(out -> {
                    if (subBlock.areAllValuesNull()) {
                        return writeAllFalseValidity(out, items);
                    }
                    BitSet valid = new BitSet(items);
                    for (int item = 0; item < items; item++) {
                        int index = indexes[item];
                        if (index >= 0 && subBlock.isNull(index) == false) {
                            valid.set(item);
                        }
                    }
                    return writeBitSet(out, valid, items);
                });
            }
            long bytes = (long) items * Double.BYTES;
            bufs.add(dummyArrowBuf(bytes));
            bufWriters.add(out -> {
                if (subBlock.areAllValuesNull()) {
                    return writeZeroes(out, bytes);
                }
                for (int index : indexes) {
                    if (index < 0 || subBlock.isNull(index)) {
                        out.writeDoubleLE(0);
                    } else {
                        out.writeDoubleLE(subBlock.getDouble(subBlock.getFirstValueIndex(index)));
                    }
                }
                return bytes;
            });
        }

        private static void addIntChild(List<ArrowBuf> bufs, List<BufWriter> bufWriters, int[] values) {
            addEmptyValidity(bufs, bufWriters);
            long bytes = (long) values.length * Integer.BYTES;
            bufs.add(dummyArrowBuf(bytes));
            bufWriters.add(out -> {
                for (int value : values) {
                    out.writeIntLE(value);
                }
                return bytes;
            });
        }

        private static void addLongChild(List<ArrowBuf> bufs, List<BufWriter> bufWriters, long[] values) {
            addEmptyValidity(bufs, bufWriters);
            long bytes = (long) values.length * Long.BYTES;
            bufs.add(dummyArrowBuf(bytes));
            bufWriters.add(out -> {
                for (long value : values) {
                    out.writeLongLE(value);
                }
                return bytes;
            });
        }

        private static void addDoubleChild(List<ArrowBuf> bufs, List<BufWriter> bufWriters, double[] values) {
            addEmptyValidity(bufs, bufWriters);
            long bytes = (long) values.length * Double.BYTES;
            bufs.add(dummyArrowBuf(bytes));
            bufWriters.add(out -> {
                for (double value : values) {
                    out.writeDoubleLE(value);
                }
                return bytes;
            });
        }

        private static long writeLongs(RecyclerBytesStreamOutput out, LongVector values) throws IOException {
            int count = values.getPositionCount();
            for (int i = 0; i < count; i++) {
                out.writeLongLE(values.getLong(i));
            }
            return (long) count * Long.BYTES;
        }
    }

    private static Field nonNullableListField(String name, Types.MinorType itemType) {
        return new Field(
            name,
            new FieldType(false, Types.MinorType.LIST.getType(), null),
            List.of(new Field("$data$", new FieldType(false, itemType.getType(), null), null))
        );
    }

    private static void addEmptyValidity(List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
        bufs.add(dummyArrowBuf(0));
        bufWriters.add(out -> 0);
    }

    private static void addNonNullableListBuffers(
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
    private static int[] itemIndexes(Block block, boolean asList) {
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

    /**
     * Conversion of {@code null} and {@code unsupported} columns to Arrow {@code Null} vectors (no data buffers).
     */
    public static class AsNull extends BlockArrowFormatter {
        public AsNull(DataType esqlType) {
            super(esqlType, Types.MinorType.NULL);
        }

        @Override
        public boolean isMultivalued(String columnName, Block block) {
            return false;
        }

        @Override
        public void convert(
            Block block,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            assert asList == false : "Null columns cannot be encoded as Arrow lists";
            addSimpleFieldNodes(block, false, nodes);
            // Null vector in arrow has no associated buffers
            // See https://arrow.apache.org/docs/format/Columnar.html#null-layout
        }
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
        for (long i = 0; i < byteCount; i++) {
            out.writeByte((byte) 0);
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
