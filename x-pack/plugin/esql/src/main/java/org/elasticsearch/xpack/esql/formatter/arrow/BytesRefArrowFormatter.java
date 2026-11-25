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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Conversion of {@link BytesRefBlock}s to variable-size Arrow string or binary vectors.
 */
class BytesRefArrowFormatter extends BlockArrowFormatter {

    BytesRefArrowFormatter(DataType esqlType, Types.MinorType minorType) {
        super(esqlType, minorType);
    }

    protected BytesRefArrowFormatter(DataType esqlType, Types.MinorType minorType, Map<String, String> extraMetadata) {
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

    /**
     * Conversion of ByteRefs where each value is itself converted to a different format.
     */
    static class TransformedBytesRef extends BytesRefArrowFormatter {

        private final BiFunction<BytesRef, BytesRef, BytesRef> valueConverter;

        /**
         * @param esqlType ESQL data type
         * @param minorType Arrow type
         * @param valueConverter a function that takes (value, scratch) input parameters and returns the transformed value
         */
        TransformedBytesRef(DataType esqlType, Types.MinorType minorType, BiFunction<BytesRef, BytesRef, BytesRef> valueConverter) {
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
    static class AsJson extends TransformedBytesRef {
        static final String ARROW_JSON = "arrow.json";

        AsJson(DataType esqlType) {
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
    static class AsGeoArrowWkb extends BytesRefArrowFormatter {
        static final String GEOARROW_WKB = "geoarrow.wkb";

        /**
         * GeoArrow extension metadata for geographic types: the CRS is {@code OGC:CRS84} (WGS 84, longitude/latitude order),
         * given as an authority code.
         */
        private static final String GEO_CRS_METADATA = "{\"crs\":\"OGC:CRS84\",\"crs_type\":\"authority_code\"}";

        AsGeoArrowWkb(DataType esqlType) {
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
}
