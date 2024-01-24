/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.arrow.shim.Shim;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.ChunkedRestResponseBody;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class ArrowResponse implements Releasable {
    public static class Column {
        private final String esqlType;
        private final FieldType arrowType;
        private final String name;

        public Column(String esqlType, String name) {
            this.esqlType = esqlType;
            this.arrowType = arrowFieldType(esqlType);
            this.name = name;
        }

        String esqlType() {
            return esqlType;
        }

        Field arrowField() {
            return new Field(name, arrowType, List.of());
        }
    }

    private final List<Column> columns;
    private final List<Page> pages;
    private final Runnable closeMe;

    public ArrowResponse(List<Column> columns, List<Page> pages, Runnable closeMe) {
        this.columns = columns;
        this.pages = pages;
        this.closeMe = closeMe;
    }

    List<Column> columns() {
        return columns;
    }

    List<Page> pages() {
        return pages;
    }

    @Override
    public void close() {
        closeMe.run();
    }

    public ChunkedRestResponseBody chunkedResponse() {
        // TODO dictionaries

        SchemaResponse schemaResponse = new SchemaResponse(this);
        List<ChunkedRestResponseBody> rest = new ArrayList<>(pages.size());
        for (int p = 0; p < pages.size() - 1; p++) {
            rest.add(new PageResponse(this, pages.get(p), false));
        }
        rest.add(new PageResponse(this, pages.get(this.pages.size() - 1), true));

        return ChunkedRestResponseBody.fromMany(schemaResponse, rest.iterator());
    }

    protected abstract static class AbstractArrowChunkedResponse implements ChunkedRestResponseBody {
        static {
            // Init the arrow shim
            Shim.init();
        }

        protected final ArrowResponse response;

        AbstractArrowChunkedResponse(ArrowResponse response) {
            this.response = response;
        }

        @Override
        public void close() {}

        @Override
        public final ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(recycler);
            try {
                encodeChunk(sizeHint, output);
                BytesReference ref = output.bytes();
                RecyclerBytesStreamOutput closeRef = output;
                output = null;
                ReleasableBytesReference result = new ReleasableBytesReference(ref, () -> Releasables.closeExpectNoException(closeRef));
                return result;
            } catch (Exception e) {
                logger.error("failed to write arrow chunk", e);
                throw e;
            } finally {
                if (output != null) {
                    // assert false : "failed to write arrow chunk";
                    Releasables.closeExpectNoException(output);
                }
            }
        }

        protected abstract void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException;

        /**
         * Adapts our {@link RecyclerBytesStreamOutput} to the format that Arrow
         * likes to write to.
         */
        protected static WritableByteChannel arrowOut(RecyclerBytesStreamOutput output) {
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

        @Override
        public final String getResponseContentTypeString() {
            return ArrowFormat.CONTENT_TYPE;
        }
    }

    private static class SchemaResponse extends AbstractArrowChunkedResponse {
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
            return new Schema(response.columns.stream().map(ArrowResponse.Column::arrowField).toList());
        }
    }

    private static class PageResponse extends AbstractArrowChunkedResponse {
        private final Page page;
        private final boolean finalPage;
        private boolean done = false;

        PageResponse(ArrowResponse response, Page page, boolean finalPage) {
            super(response);
            this.page = page;
            this.finalPage = finalPage;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public final void close() {
            if (finalPage) {
                // TODO close the pages as we go
                Releasables.closeExpectNoException(response);
            }
        }

        interface WriteBuf {
            long write() throws IOException;
        }

        @Override
        protected void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException {
            List<ArrowFieldNode> nodes = new ArrayList<>(page.getBlockCount());
            List<WriteBuf> writeBufs = new ArrayList<>(page.getBlockCount() * 2);
            List<ArrowBuf> bufs = new ArrayList<>(page.getBlockCount() * 2);
            WriteChannel arrowOut = new WriteChannel(arrowOut(out)) {
                int bufIdx = 0;
                long extraPosition = 0;

                @Override
                public void write(ArrowBuf buffer) throws IOException {
                    extraPosition += writeBufs.get(bufIdx++).write();
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
            for (int b = 0; b < page.getBlockCount(); b++) {
                accumulateBlock(out, nodes, writeBufs, bufs, page.getBlock(b));
            }
            ArrowRecordBatch batch = new ArrowRecordBatch(
                page.getPositionCount(),
                nodes,
                bufs,
                NoCompressionCodec.DEFAULT_BODY_COMPRESSION,
                true,
                false
            );
            MessageSerializer.serialize(arrowOut, batch);
            if (finalPage) {
                ArrowStreamWriter.writeEndOfStream(arrowOut, IpcOption.DEFAULT);
            }
            done = true; // one day we should respect sizeHint here. kindness.
        }

        private static int validityCount(int totalValues) {
            return (totalValues - 1) / Byte.SIZE + 1;
        }

        private void accumulateBlock(
            RecyclerBytesStreamOutput out,
            List<ArrowFieldNode> nodes,
            List<WriteBuf> writeBufs,
            List<ArrowBuf> bufs,
            Block block
        ) {
            nodes.add(new ArrowFieldNode(block.getPositionCount(), block.nullValuesCount()));
            switch (block.elementType()) {
                case LONG -> {
                    LongBlock b = (LongBlock) block;
                    LongVector v = b.asVector();
                    if (v != null) {
                        bufs.add(dummy().writerIndex(validityCount(v.getPositionCount())));
                        writeBufs.add(() -> writeAllTrueValidity(out, v.getPositionCount()));
                        bufs.add(dummy().writerIndex(Long.BYTES * block.getPositionCount()));
                        writeBufs.add(() -> writeVector(out, v));
                        return;
                    }
                    throw new UnsupportedOperationException();
                }
                default -> throw new UnsupportedOperationException();
            }
        }

        private long writeAllTrueValidity(RecyclerBytesStreamOutput out, int valueCount) {
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

        private long writeVector(RecyclerBytesStreamOutput out, LongVector vector) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            for (int i = 0; i < vector.getPositionCount(); i++) {
                out.writeLong(vector.getLong(i));
            }
            return vector.getPositionCount() * Long.BYTES;
        }

        private ArrowBuf dummy() {
            return new ArrowBuf(null, null, 0, 0);
        }
    }

    static FieldType arrowFieldType(String fieldType) {
        return switch (fieldType) {
            case "long" -> FieldType.nullable(Types.MinorType.BIGINT.getType());
            default -> throw new UnsupportedOperationException("NOCOMMIT");
        };
    }
}
