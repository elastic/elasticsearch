/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.xpack.esql.arrow.Shim;
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
import java.util.stream.IntStream;

abstract class ArrowResponse implements ChunkedRestResponseBody {
    static {
        // Init the arrow shim
        Shim.init();
    }

    static ChunkedRestResponseBody response(EsqlQueryResponse response) {
        // TODO dictionaries

        SchemaResponse schemaResponse = new SchemaResponse(response);
        List<ChunkedRestResponseBody> rest = new ArrayList<>(response.pages().size());
        for (int p = 0; p < response.pages().size() - 1; p++) {
            rest.add(new PageResponse(response, response.pages().get(p), false));
        }
        rest.add(new PageResponse(response, response.pages().get(response.pages().size() - 1), true));

        return ChunkedRestResponseBody.fromMany(schemaResponse, rest.iterator());
    }

    protected final EsqlQueryResponse response;

    ArrowResponse(EsqlQueryResponse response) {
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

    private static class SchemaResponse extends ArrowResponse {
        private boolean done = false;

        SchemaResponse(EsqlQueryResponse response) {
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
            return new Schema(response.columns().stream().map(SchemaResponse::arrowField).toList());
        }

        private static Field arrowField(ColumnInfo column) {
            return new Field(column.name(), arrowFieldType(column.type()), List.of());
        }
    }

    private static class PageResponse extends ArrowResponse {
        private final Page page;
        private final boolean finalPage;
        private boolean done = false;

        PageResponse(EsqlQueryResponse response, Page page, boolean finalPage) {
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

        @Override
        protected void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException {
            WriteChannel arrowOut = new WriteChannel(arrowOut(out)) {
                int blockIdx = 0;
                long extraPosition = 0;

                @Override
                public void write(ArrowBuf buffer) throws IOException {
                    extraPosition += writeBlock(out, page.getBlock(blockIdx++));
                }

                @Override
                public long getCurrentPosition() {
                    return super.getCurrentPosition() + extraPosition;
                }
            };
            ArrowRecordBatch batch = new ArrowRecordBatch(
                page.getPositionCount(),
                IntStream.range(0, page.getBlockCount()).mapToObj(i -> arrowFieldNode(page.getBlock(i))).toList(),
                IntStream.range(0, page.getBlockCount()).mapToObj(i -> dummyBuf(page.getBlock(i))).toList(),
                NoCompressionCodec.DEFAULT_BODY_COMPRESSION,
                true,
                false
            );
            MessageSerializer.serialize(arrowOut, batch);
            if (finalPage) {
                arrowOut.writeIntLittleEndian(0);
            }
            done = true; // one day we should respect sizeHint here. kindness.
        }

        private long writeBlock(RecyclerBytesStreamOutput out, Block block) throws IOException {
            return switch (block.elementType()) {
                case LONG -> {
                    if (block.asVector() instanceof LongVector vector) {
                        // TODO could we "just" get the memory of the array and dump it?
                        for (int i = 0; i < vector.getPositionCount(); i++) {
                            out.writeLong(vector.getLong(i));
                        }
                        yield vector.getPositionCount() * Long.BYTES;
                    } else {
                        throw new UnsupportedOperationException();
                    }
                }
                default -> throw new UnsupportedOperationException();
            };
        }

        private ArrowFieldNode arrowFieldNode(Block b) {
            return new ArrowFieldNode(b.getPositionCount(), b.nullValuesCount());
        }

        private ArrowBuf dummyBuf(Block block) {
            return new ArrowBuf(null, null, 0, 0).writerIndex(switch (block.elementType()) {
                case LONG -> Long.BYTES * block.getPositionCount();
                default -> throw new IllegalArgumentException();
            });
        }
    }

    static FieldType arrowFieldType(String fieldType) {
        return switch (fieldType) {
            case "long" -> FieldType.nullable(Types.MinorType.BIGINT.getType());
            default -> throw new UnsupportedOperationException("NOCOMMIT");
        };
    }
}
