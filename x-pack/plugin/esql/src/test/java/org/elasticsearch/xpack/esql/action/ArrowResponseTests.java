/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xpack.esql.TestBlockFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

public class ArrowResponseTests extends ESTestCase {
    public void testLongZeros() throws IOException {
        compareSerializationToArrow(response(List.of(new ColumnInfo("a", "long")), List.of(new Page(longVector(10, () -> 0L).asBlock()))));
    }

    // TODO more schemata

    private void compareSerializationToArrow(EsqlQueryResponse response) throws IOException {
        ChunkedRestResponseBody body = ArrowResponse.response(response);
        List<BytesReference> ourEncoding = new ArrayList<>();
        while (body.isDone() == false) {
            ourEncoding.add(body.encodeChunk(1500, BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }

        BytesReference our = CompositeBytesReference.of(ourEncoding.toArray(BytesReference[]::new));

        BytesStreamOutput arrow = new BytesStreamOutput();

    }

    private BytesReference serializeWithNativeArrow(EsqlQueryResponse response) throws IOException {
        List<Field> fields = new ArrayList<>(response.columns().size());
        for (ColumnInfo info : response.columns()) {
            fields.add(new Field(info.name(), ArrowResponse.arrowFieldType(info.type()), List.of()));
        }
        Schema schema = new Schema(fields);

        try (
            BufferAllocator rootAllocator = new RootAllocator();
            VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
            BytesStreamOutput out = new BytesStreamOutput();
        ) {
            try (ArrowStreamWriter writer = new ArrowStreamWriter(schemaRoot, null, out)) {
                for (Page page : response.pages()) {
                    schemaRoot.clear();
                    for (int c = 0; c < response.columns().size(); c++) {
                        switch (response.columns().get(c).type()) {
                            case "long" -> {
                                LongBlock b = page.getBlock(c);
                                LongVector v = b.asVector();
                                if (v == null) {
                                    throw new IllegalArgumentException();
                                }
                                BigIntVector arrow = (BigIntVector) schemaRoot.getVector(c);
                                arrow.allocateNew(v.getPositionCount());
                                for (int p = 0; p < v.getPositionCount(); p++) {
                                    arrow.set(0, v.getLong(p));
                                }
                            }
                            default -> throw new IllegalArgumentException();
                        }
                    }
                    writer.start();
                    writer.writeBatch();
                }
            }
            return out.bytes();
        }
    }

    private EsqlQueryResponse randomResponse(List<ColumnInfo> columns) {
        List<Page> pages = new ArrayList<>();

        // NOCOMMIT randomize number of pages
        pages.add(randomPage(columns));
        return response(columns, pages);
    }

    private EsqlQueryResponse response(List<ColumnInfo> columns, List<Page> pages) {
        return new EsqlQueryResponse(columns, pages, null, true, null, false, false);
    }

    private Page randomPage(List<ColumnInfo> schema) {
        int positions = 10; // between(10, 65535); NOCOMMIT randomize
        Block[] blocks = new Block[schema.size()];
        for (int b = 0; b < blocks.length; b++) {
            blocks[b] = switch (schema.get(b).type()) {
                case "long" -> longVector(positions, () -> 0L).asBlock(); // NOCOMMIT randomize
                default -> throw new IllegalArgumentException();
            };
        }
        return new Page(blocks);
    }

    private LongVector longVector(int positions, LongSupplier supplier) {
        LongVector.FixedBuilder builder = TestBlockFactory.getNonBreakingInstance().newLongVectorFixedBuilder(positions);
        for (int i = 0; i < positions; i++) {
            builder.appendLong(supplier.getAsLong());
        }
        return builder.build();
    }
}
