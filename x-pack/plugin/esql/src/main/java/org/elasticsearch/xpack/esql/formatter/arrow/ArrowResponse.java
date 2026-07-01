/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ArrowResponse implements ChunkedRestResponseBodyPart, Releasable {

    public static class Column {
        private final BlockArrowFormatter converter;
        private final String name;
        private boolean multivalued;

        public Column(DataType esqlType, String name) {
            this.converter = ESQL_FORMATTERS.get(esqlType);
            if (converter == null) {
                throw new IllegalArgumentException("ES|QL type [" + esqlType.outputType() + "] is not supported by the Arrow format");
            }
            this.name = name;
        }
    }

    private final List<Column> columns;
    private Iterator<ResponseSegment> segments;
    private ResponseSegment currentSegment;

    public ArrowResponse(List<Column> columns, List<Page> pages) {
        this.columns = columns;

        // Find multivalued columns
        int colSize = columns.size();
        for (int col = 0; col < colSize; col++) {
            for (Page page : pages) {
                if (page.getBlock(col).mayHaveMultivaluedFields()) {
                    columns.get(col).multivalued = true;
                    break;
                }
            }
        }

        currentSegment = new SchemaResponse(this);
        List<ResponseSegment> rest = new ArrayList<>(pages.size());

        for (Page page : pages) {
            rest.add(new PageResponse(this, page));
        }

        rest.add(new EndResponse(this));
        segments = rest.iterator();
    }

    @Override
    public boolean isPartComplete() {
        return currentSegment == null;
    }

    @Override
    public boolean isLastPart() {
        // Even if sent in chunks, the entirety of ESQL data is available, so it's single (chunked) part
        return true;
    }

    @Override
    public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
        listener.onFailure(new IllegalStateException("no continuations available"));
    }

    @Override
    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
        try {
            return currentSegment.encodeChunk(sizeHint, recycler);
        } finally {
            if (currentSegment.isDone()) {
                currentSegment = segments.hasNext() ? segments.next() : null;
            }
        }
    }

    @Override
    public String getResponseContentTypeString() {
        return ArrowFormat.CONTENT_TYPE;
    }

    @Override
    public void close() {
        currentSegment = null;
        segments = null;
    }

    /**
     * An Arrow response is composed of different segments, each being a set of chunks:
     * the schema header, the data buffers, and the trailer.
     */
    protected abstract static class ResponseSegment {

        protected final ArrowResponse response;

        ResponseSegment(ArrowResponse response) {
            this.response = response;
        }

        public final ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(recycler);
            try {
                encodeChunk(sizeHint, output);
                BytesReference ref = output.bytes();
                RecyclerBytesStreamOutput closeRef = output;
                output = null;
                ReleasableBytesReference result = new ReleasableBytesReference(ref, () -> Releasables.closeExpectNoException(closeRef));
                return result;
            } finally {
                Releasables.closeExpectNoException(output);
            }
        }

        protected abstract void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException;

        protected abstract boolean isDone();

        /**
         * Adapts a {@link BytesStream} so that Arrow can write to it.
         */
        protected static WritableByteChannel arrowOut(BytesStream output) {
            return new WritableByteChannel() {
                @Override
                public int write(ByteBuffer byteBuffer) throws IOException {
                    if (byteBuffer.hasArray() == false) {
                        throw new AssertionError("only implemented for array backed buffers");
                    }
                    int length = byteBuffer.remaining();
                    output.write(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), length);
                    byteBuffer.position(byteBuffer.position() + length);
                    assert byteBuffer.hasRemaining() == false;
                    return length;
                }

                @Override
                public boolean isOpen() {
                    return true;
                }

                @Override
                public void close() {}
            };
        }
    }

    /**
     * Header part of the Arrow response containing the dataframe schema.
     *
     * @see <a href="https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format">IPC Streaming Format</a>
     */
    private static class SchemaResponse extends ResponseSegment {

        private static final FieldType LIST_FIELD_TYPE = FieldType.nullable(MinorType.LIST.getType());

        private boolean done = false;

        SchemaResponse(ArrowResponse response) {
            super(response);
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        protected void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException {
            WriteChannel arrowOut = new WriteChannel(arrowOut(out));
            MessageSerializer.serialize(arrowOut, arrowSchema());
            done = true;
        }

        private Schema arrowSchema() {
            return new Schema(response.columns.stream().map(c -> {
                var field = c.converter.arrowField(c.name);
                if (c.multivalued) {
                    // A variable-sized list is a vector of offsets and a child vector of values
                    // See https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
                    var fieldType = field.getFieldType();
                    // Copy the ESQL type metadata at the list level
                    var listMetadata = Map.of(
                        BlockArrowFormatter.ESQL_TYPE_METADATA,
                        fieldType.getMetadata().get(BlockArrowFormatter.ESQL_TYPE_METADATA)
                    );
                    var listType = new FieldType(true, LIST_FIELD_TYPE.getType(), null, listMetadata);
                    // Value vector is non-nullable (ES|QL multivalues cannot contain nulls).
                    var valueType = new FieldType(false, fieldType.getType(), fieldType.getDictionary(), fieldType.getMetadata());
                    // The nested vector is named "$data$", following what the Arrow/Java library does.
                    return new Field(c.name, listType, List.of(new Field("$data$", valueType, field.getChildren())));
                } else {
                    return field;
                }
            }).toList());
        }
    }

    /**
     * Page response segment: write an ES|QL page as an Arrow RecordBatch
     */
    private static class PageResponse extends ResponseSegment {
        private final Page page;
        private boolean done = false;

        PageResponse(ArrowResponse response, Page page) {
            super(response);
            this.page = page;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        // Writes some data and returns the number of bytes written.
        interface BufWriter {
            long write() throws IOException;
        }

        @Override
        protected void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException {
            // An Arrow record batch consists of:
            // - fields metadata, giving the number of items and the number of null values for each field
            // - data buffers for each field. The number of buffers for a field depends on its type, e.g.:
            // - for primitive types, there's a validity buffer (for nulls) and a value buffer.
            // - for strings, there's a validity buffer, an offsets buffer and a data buffer
            // See https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message

            // Field metadata
            List<ArrowFieldNode> nodes = new ArrayList<>(page.getBlockCount());

            // Buffers added to the record batch. They're used to track data size so that Arrow can compute offsets
            // but contain no data. Actual writing will be done by the bufWriters. This avoids having to deal with
            // Arrow's memory management, and in the future will allow direct write from ESQL block vectors.
            List<ArrowBuf> bufs = new ArrayList<>(page.getBlockCount() * 2);

            // Closures that will actually write a Block's data. Maps 1:1 to `bufs`.
            List<BlockArrowFormatter.BufWriter> bufWriters = new ArrayList<>(page.getBlockCount() * 2);

            // Give Arrow a WriteChannel that will iterate on `bufWriters` when requested to write a buffer.
            WriteChannel arrowOut = new WriteChannel(arrowOut(out)) {
                int bufIdx = 0;
                long extraPosition = 0;

                @Override
                public void write(ArrowBuf buffer) throws IOException {
                    var len = bufWriters.get(bufIdx++).write(out);
                    // Consistency check
                    if (len != buffer.writerIndex()) {
                        throw new IllegalStateException(
                            "Buffer [" + (bufIdx - 1) + "]: wrote [" + len + "] bytes, but expected [" + buffer.writerIndex() + "]"
                        );
                    }
                    extraPosition += len;
                }

                @Override
                public long getCurrentPosition() {
                    return super.getCurrentPosition() + extraPosition;
                }

                @Override
                public long align() throws IOException {
                    int trailingByteSize = (int) (getCurrentPosition() % 8);
                    if (trailingByteSize != 0) { // align on 8 byte boundaries
                        return writeZeros(8 - trailingByteSize);
                    }
                    return 0;
                }
            };

            // Create Arrow buffers for each of the blocks in this page
            for (int b = 0; b < page.getBlockCount(); b++) {
                var column = response.columns.get(b);
                var converter = column.converter;

                Block block = page.getBlock(b);
                converter.addFieldNodes(block, column.multivalued, nodes);
                converter.convert(block, column.multivalued, bufs, bufWriters);
            }

            // Consistency check
            if (bufs.size() != bufWriters.size()) {
                throw new IllegalStateException(
                    "Inconsistent Arrow buffers: [" + bufs.size() + "] buffers and [" + bufWriters.size() + "] writers"
                );
            }

            // Create the batch and serialize it
            ArrowRecordBatch batch = new ArrowRecordBatch(
                page.getPositionCount(),
                nodes,
                bufs,
                NoCompressionCodec.DEFAULT_BODY_COMPRESSION,
                true, // align buffers
                false // retain buffers
            );
            MessageSerializer.serialize(arrowOut, batch);

            done = true; // one day we should respect sizeHint here. kindness.
        }
    }

    /**
     * Trailer segment: write the Arrow end of stream marker
     */
    private static class EndResponse extends ResponseSegment {
        private boolean done = false;

        private EndResponse(ArrowResponse response) {
            super(response);
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        protected void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException {
            ArrowStreamWriter.writeEndOfStream(new WriteChannel(arrowOut(out)), IpcOption.DEFAULT);
            done = true;
        }
    }

    private static final BlockArrowFormatter TO_BE_IMPLEMENTED_MARKER = new BlockArrowFormatter(DataType.UNSUPPORTED, MinorType.NULL) {
        @Override
        public void convert(Block block, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {}
    };

    /**
     * Converters for every ES|QL type
     */
    static final EnumMap<DataType, BlockArrowFormatter> ESQL_FORMATTERS;

    // DataTypes that don't have an Arrow formatter but should for completeness with other formats
    // See https://github.com/elastic/elasticsearch/issues/146394
    static final EnumSet<DataType> TO_BE_IMPLEMENTED_FORMATTERS;

    static {
        ESQL_FORMATTERS = new EnumMap<>(DataType.class);
        TO_BE_IMPLEMENTED_FORMATTERS = EnumSet.noneOf(DataType.class);
        for (var type : DataType.values()) {
            var formatter = formatterForType(type);
            if (formatter == TO_BE_IMPLEMENTED_MARKER) {
                TO_BE_IMPLEMENTED_FORMATTERS.add(type);
            } else if (formatter != null) {
                ESQL_FORMATTERS.put(type, formatter);
            }
        }
    }

    /**
     * Converters for every ES|QL type. Returns null if the type is not supported in the output.
     */
    private static BlockArrowFormatter formatterForType(DataType type) {
        // Explicitly list every unsupported type so that compilation fails when we add a new type and must take care of it.
        //
        // For reference:
        // - DataType: list of ESQL data types (not all are present in outputs)
        // - ResponseValueUtils: conversions to a toString-able representation for text output
        // - PositionToXContent: conversions for ESQL JSON output
        return switch (type) {
            case UNSUPPORTED, NULL -> new BlockArrowFormatter.AsNull(type);

            case BOOLEAN -> new BlockArrowFormatter.AsBoolean(type);

            case INTEGER, COUNTER_INTEGER -> new BlockArrowFormatter.AsInt32(type);
            case LONG, COUNTER_LONG -> new BlockArrowFormatter.AsInt64(type);
            case UNSIGNED_LONG -> new BlockArrowFormatter.AsInt64(type, MinorType.UINT8);

            case DOUBLE, COUNTER_DOUBLE -> new BlockArrowFormatter.AsFloat64(type);

            case KEYWORD, TEXT -> new BlockArrowFormatter.AsVarChar(type);

            case DATETIME -> new BlockArrowFormatter.AsInt64(type, MinorType.TIMESTAMPMILLI);
            case DATE_NANOS -> new BlockArrowFormatter.AsInt64(type, MinorType.TIMESTAMPNANO);

            // ip are represented as 16-byte ipv6 addresses. We shorten mapped ipv4 addresses to 4 bytes.
            // Another option would be to use a fixed size binary to avoid the offset array. But with mostly
            // ipv4 addresses it would still be twice as big.
            case IP -> new BlockArrowFormatter.TransformedBytesRef(type, MinorType.VARBINARY, ValueConversions::shortenIpV4Addresses);

            // geo_point: Keep WKB format (JSON converts to WKT)
            // FIXME: use GeoArrow
            case GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE -> new BlockArrowFormatter.AsVarBinary(type);

            // version: convert to string
            case VERSION -> new BlockArrowFormatter.TransformedBytesRef(type, MinorType.VARCHAR, ValueConversions::versionToString);

            // _source: json
            // TODO: support also CBOR and SMILE with an additional formatting parameter
            case SOURCE -> new BlockArrowFormatter.TransformedBytesRef(type, MinorType.VARCHAR, ValueConversions::sourceToJson);

            case DATE_RANGE -> new BlockArrowFormatter.AsDateRange(type);

            case TSID_DATA_TYPE -> TO_BE_IMPLEMENTED_MARKER;
            case DENSE_VECTOR -> TO_BE_IMPLEMENTED_MARKER;

            case AGGREGATE_METRIC_DOUBLE -> TO_BE_IMPLEMENTED_MARKER;
            case EXPONENTIAL_HISTOGRAM -> TO_BE_IMPLEMENTED_MARKER;
            case FLATTENED -> TO_BE_IMPLEMENTED_MARKER;
            case GEOHASH -> TO_BE_IMPLEMENTED_MARKER;
            case GEOHEX -> TO_BE_IMPLEMENTED_MARKER;
            case GEOTILE -> TO_BE_IMPLEMENTED_MARKER;
            case HISTOGRAM -> TO_BE_IMPLEMENTED_MARKER;
            case TDIGEST -> TO_BE_IMPLEMENTED_MARKER;

            // Types that should not appear in ESQL outputs
            case SHORT -> null;
            case BYTE -> null;
            case FLOAT -> null;
            case HALF_FLOAT -> null;
            case SCALED_FLOAT -> null;
            case OBJECT -> null;
            case DATE_PERIOD -> null;
            case TIME_DURATION -> null;
            case DOC_DATA_TYPE -> null;
            case PARTIAL_AGG -> null;
        };
    }
}
