/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import static org.hamcrest.Matchers.equalTo;

public class ArrowResponseTests extends ESTestCase {
    public void testLongZeros() throws IOException {
        compareSerializationToArrow(
            new ArrowResponse(
                List.of(new ArrowResponse.Column("long", "a")),
                List.of(new Page(longVector(10, i -> 0L).asBlock())),
                () -> {}
            )
        );
    }

    // TODO more schemata

    private static final int BEFORE = 20;
    private static final int AFTER = 80;

    private void compareSerializationToArrow(ArrowResponse response) throws IOException {
        BytesReference directBlocks = serializeBlocksDirectly(response);
        BytesReference nativeArrow = serializeWithNativeArrow(response);

        int length = Math.max(directBlocks.length(), nativeArrow.length());
        for (int i = 0; i < length; i++) {
            if (directBlocks.length() < i || nativeArrow.length() < i) {
                throw new AssertionError(
                    "matched until ended:\n"
                        + describeRange(directBlocks, nativeArrow, Math.max(0, i - BEFORE), Math.min(length, i + AFTER))
                );
            }
            if (directBlocks.get(i) != nativeArrow.get(i)) {
                throw new AssertionError(
                    "first mismatch:\n" + describeRange(directBlocks, nativeArrow, Math.max(0, i - BEFORE), Math.min(length, i + AFTER))
                );
            }
        }
    }

    private String describeRange(BytesReference directBlocks, BytesReference nativeArrow, int from, int to) {
        StringBuilder b = new StringBuilder();
        for (int i = from; i < to; i++) {
            String d = positionToString(directBlocks, i);
            String n = positionToString(nativeArrow, i);
            b.append(String.format(Locale.ROOT, "%08d: ", i));
            b.append(d);
            b.append(' ');
            b.append(n);
            if (d.equals(n) == false) {
                b.append(" <---");
            }
            b.append('\n');
        }
        return b.toString();
    }

    private String positionToString(BytesReference bytes, int i) {
        return i < bytes.length() ? String.format(Locale.ROOT, "%02X", Byte.toUnsignedInt(bytes.get(i))) : "--";
    }

    private BytesReference serializeBlocksDirectly(ArrowResponse response) throws IOException {
        ChunkedRestResponseBody body = response.chunkedResponse();
        List<BytesReference> ourEncoding = new ArrayList<>();
        while (body.isDone() == false) {
            ourEncoding.add(body.encodeChunk(1500, BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }

        return CompositeBytesReference.of(ourEncoding.toArray(BytesReference[]::new));
    }

    private BytesReference serializeWithNativeArrow(ArrowResponse response) throws IOException {
        Schema schema = new Schema(response.columns().stream().map(ArrowResponse.Column::arrowField).toList());
        try (
            BufferAllocator rootAllocator = new RootAllocator();
            VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
            BytesStreamOutput out = new BytesStreamOutput();
        ) {
            try (ArrowStreamWriter writer = new ArrowStreamWriter(schemaRoot, null, out)) {
                for (Page page : response.pages()) {
                    schemaRoot.clear();
                    for (int c = 0; c < response.columns().size(); c++) {
                        switch (response.columns().get(c).esqlType()) {
                            case "long" -> {
                                LongBlock b = page.getBlock(c);
                                LongVector v = b.asVector();
                                if (v == null) {
                                    throw new IllegalArgumentException();
                                }
                                BigIntVector arrow = (BigIntVector) schemaRoot.getVector(c);
                                arrow.allocateNew(v.getPositionCount());
                                for (int p = 0; p < v.getPositionCount(); p++) {
                                    arrow.set(p, v.getLong(p));
                                }
                                arrow.setValueCount(v.getPositionCount());
                            }
                            default -> throw new IllegalArgumentException();
                        }
                    }
                    schemaRoot.setRowCount(page.getPositionCount());
                    writer.start();
                    writer.writeBatch();
                }
            }
            return out.bytes();
        }
    }

    private ArrowResponse randomResponse(List<ArrowResponse.Column> columns) {
        List<Page> pages = new ArrayList<>();

        // NOCOMMIT randomize number of pages
        pages.add(randomPage(columns));
        return new ArrowResponse(columns, pages, () -> {});
    }

    private Page randomPage(List<ArrowResponse.Column> schema) {
        int positions = 10; // between(10, 65535); NOCOMMIT randomize
        Block[] blocks = new Block[schema.size()];
        for (int b = 0; b < blocks.length; b++) {
            blocks[b] = switch (schema.get(b).esqlType()) {
                case "long" -> longVector(positions, i -> randomLong()).asBlock(); // NOCOMMIT randomize
                default -> throw new IllegalArgumentException();
            };
        }
        return new Page(blocks);
    }

    private LongVector longVector(int positions, LongUnaryOperator supplier) {
        LongVector.FixedBuilder builder = BLOCK_FACTORY.newLongVectorFixedBuilder(positions);
        for (int i = 0; i < positions; i++) {
            builder.appendLong(supplier.applyAsLong(i));
        }
        return builder.build();
    }

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test-noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );
}
