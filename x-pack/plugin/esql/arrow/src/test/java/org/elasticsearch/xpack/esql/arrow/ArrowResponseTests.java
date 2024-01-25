/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import com.carrotsearch.randomizedtesting.annotations.Seed;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
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
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

@Seed("72404B36EFE39D13")
public class ArrowResponseTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<ArrowResponse.Column> justInt = List.of(new ArrowResponse.Column("int", "a"));
        List<ArrowResponse.Column> justLong = List.of(new ArrowResponse.Column("long", "a"));
        List<ArrowResponse.Column> justDouble = List.of(new ArrowResponse.Column("double", "a"));
        List<ArrowResponse.Column> justKeyword = List.of(new ArrowResponse.Column("keyword", "a"));

        List<TestCase> cases = new ArrayList<>();

        cases.add(new TestCase("int no pages", justInt, () -> List.of()));
        TestCase.cases(cases, "int all zeros", justInt, () -> new Page(intVector(10, i -> 0).asBlock()));
        TestCase.cases(cases, "int increment", justInt, () -> new Page(intVector(10, i -> i).asBlock()));
        TestCase.cases(cases, "int random", justInt, () -> new Page(intVector(between(1, 10_000), i -> randomInt()).asBlock()));

        cases.add(new TestCase("long no pages", justLong, () -> List.of()));
        TestCase.cases(cases, "long all zeros", justLong, () -> new Page(longVector(10, i -> 0L).asBlock()));
        TestCase.cases(cases, "long increment", justLong, () -> new Page(longVector(10, i -> i).asBlock()));
        TestCase.cases(cases, "long random", justLong, () -> new Page(longVector(between(1, 10_000), i -> randomLong()).asBlock()));

        cases.add(new TestCase("double no pages", justDouble, () -> List.of()));
        TestCase.cases(cases, "double all zeros", justDouble, () -> new Page(doubleVector(10, i -> 0L).asBlock()));
        TestCase.cases(cases, "double increment", justDouble, () -> new Page(doubleVector(10, i -> i).asBlock()));
        TestCase.cases(cases, "double random", justDouble, () -> new Page(doubleVector(between(1, 10_000), i -> randomDouble()).asBlock()));

        cases.add(new TestCase("keyword no pages", justKeyword, () -> List.of()));
        TestCase.cases(
            cases,
            "keyword empty",
            justKeyword,
            () -> new Page(bytesRefVector(10, i -> new BytesRef(BytesRef.EMPTY_BYTES, 0, 0)).asBlock())
        );
        TestCase.cases(cases, "keyword \"a\"", justKeyword, () -> new Page(bytesRefVector(10, i -> new BytesRef("a")).asBlock()));
        TestCase.cases(cases, "keyword \"foo\"", justKeyword, () -> new Page(bytesRefVector(10, i -> new BytesRef("foo")).asBlock()));
        TestCase.cases(
            cases,
            "keyword \"foo\"|\"bar\"",
            justKeyword,
            () -> new Page(bytesRefVector(10, i -> i % 2 == 0 ? new BytesRef("foo") : new BytesRef("bar")).asBlock())
        );
        TestCase.cases(cases, "keyword random", justKeyword, () -> new Page(fullyRandomKeywordVector(between(1, 10_000))));

        TestCase.cases(
            cases,
            "keyword|int|double",
            List.of(
                new ArrowResponse.Column("keyword", "a"),
                new ArrowResponse.Column("int", "b"),
                new ArrowResponse.Column("double", "c")
            ),
            () -> {
                int positions = between(1, 10_000);
                return new Page(
                    bytesRefVector(positions, i -> new BytesRef(randomAlphaOfLengthBetween(0, 100))).asBlock(),
                    intVector(positions, i -> randomInt()).asBlock(),
                    doubleVector(positions, i -> randomDouble()).asBlock()
                );
            }
        );
        for (Map.Entry<String, IntFunction<Block>> first : RANDOM.entrySet()) {
            for (Map.Entry<String, IntFunction<Block>> second : RANDOM.entrySet()) {
                TestCase.cases(
                    cases,
                    first.getKey() + "|" + second.getKey() + "random",
                    List.of(new ArrowResponse.Column(first.getKey(), "a"), new ArrowResponse.Column(second.getKey(), "b")),
                    () -> {
                        int positions = between(1, 10_000);
                        return new Page(first.getValue().apply(positions), second.getValue().apply(positions));
                    }
                );
            }
        }

        return () -> Iterators.map(cases.iterator(), c -> new Object[] { c });
    }

    private static final Map<String, IntFunction<Block>> RANDOM = Map.ofEntries(
        Map.entry("double", ArrowResponseTests::fullyRandomDoubleVector),
        Map.entry("int", ArrowResponseTests::fullyRandomIntVector),
        Map.entry("long", ArrowResponseTests::fullyRandomLongVector),
        Map.entry("keyword", ArrowResponseTests::fullyRandomKeywordVector)
    );

    private final TestCase testCase;

    public ArrowResponseTests(@Name("desc") TestCase testCase) {
        this.testCase = testCase;
    }

    // TODO more schemata

    private static final int BEFORE = 20;
    private static final int AFTER = 80;

    public void test() throws IOException {
        BytesReference directBlocks = serializeBlocksDirectly();
        BytesReference nativeArrow = serializeWithNativeArrow();

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

    private BytesReference serializeBlocksDirectly() throws IOException {
        ChunkedRestResponseBody body = testCase.response().chunkedResponse();
        List<BytesReference> ourEncoding = new ArrayList<>();
        while (body.isDone() == false) {
            ourEncoding.add(body.encodeChunk(1500, BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }

        return CompositeBytesReference.of(ourEncoding.toArray(BytesReference[]::new));
    }

    private BytesReference serializeWithNativeArrow() throws IOException {
        Schema schema = new Schema(testCase.response().columns().stream().map(ArrowResponse.Column::arrowField).toList());
        try (
            BufferAllocator rootAllocator = new RootAllocator();
            VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
            BytesStreamOutput out = new BytesStreamOutput();
        ) {
            try (ArrowStreamWriter writer = new ArrowStreamWriter(schemaRoot, null, out)) {
                for (Page page : testCase.response.pages()) {
                    schemaRoot.clear();
                    for (int c = 0; c < testCase.response.columns().size(); c++) {
                        ArrowResponse.Column column = testCase.response.columns().get(c);
                        switch (column.esqlType()) {
                            case "keyword" -> {
                                BytesRef scratch = new BytesRef();
                                BytesRefBlock b = page.getBlock(c);
                                BytesRefVector v = b.asVector();
                                if (v == null) {
                                    throw new IllegalArgumentException();
                                }
                                VarCharVector arrow = (VarCharVector) schemaRoot.getVector(c);
                                arrow.allocateNew(v.getPositionCount());
                                for (int p = 0; p < v.getPositionCount(); p++) {
                                    BytesRef bytes = v.getBytesRef(p, scratch);
                                    arrow.setSafe(p, bytes.bytes, bytes.offset, bytes.length);
                                }
                                arrow.setValueCount(v.getPositionCount());
                            }
                            case "double" -> {
                                DoubleBlock b = page.getBlock(c);
                                DoubleVector v = b.asVector();
                                if (v == null) {
                                    throw new IllegalArgumentException();
                                }
                                Float8Vector arrow = (Float8Vector) schemaRoot.getVector(c);
                                arrow.allocateNew(v.getPositionCount());
                                for (int p = 0; p < v.getPositionCount(); p++) {
                                    arrow.set(p, v.getDouble(p));
                                }
                                arrow.setValueCount(v.getPositionCount());
                            }
                            case "int" -> {
                                IntBlock b = page.getBlock(c);
                                IntVector v = b.asVector();
                                if (v == null) {
                                    throw new IllegalArgumentException();
                                }
                                org.apache.arrow.vector.IntVector arrow = (org.apache.arrow.vector.IntVector) schemaRoot.getVector(c);
                                arrow.allocateNew(v.getPositionCount());
                                for (int p = 0; p < v.getPositionCount(); p++) {
                                    arrow.set(p, v.getInt(p));
                                }
                                arrow.setValueCount(v.getPositionCount());
                            }
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
                            default -> throw new IllegalArgumentException("NOCOMMIT: " + column.esqlType());
                        }
                    }
                    schemaRoot.setRowCount(page.getPositionCount());
                    writer.writeBatch();
                }
            }
            return out.bytes();
        }
    }

    private static IntVector intVector(int positions, IntUnaryOperator v) {
        IntVector.FixedBuilder builder = BLOCK_FACTORY.newIntVectorFixedBuilder(positions);
        for (int i = 0; i < positions; i++) {
            builder.appendInt(v.applyAsInt(i));
        }
        return builder.build();
    }

    private static Block fullyRandomIntVector(int positions) {
        return intVector(positions, i -> randomInt()).asBlock();
    }

    private static LongVector longVector(int positions, IntToLongFunction v) {
        LongVector.FixedBuilder builder = BLOCK_FACTORY.newLongVectorFixedBuilder(positions);
        for (int i = 0; i < positions; i++) {
            builder.appendLong(v.applyAsLong(i));
        }
        return builder.build();
    }

    private static Block fullyRandomLongVector(int positions) {
        return longVector(positions, i -> randomLong()).asBlock();
    }

    private static DoubleVector doubleVector(int positions, IntToDoubleFunction v) {
        DoubleVector.FixedBuilder builder = BLOCK_FACTORY.newDoubleVectorFixedBuilder(positions);
        for (int i = 0; i < positions; i++) {
            builder.appendDouble(v.applyAsDouble(i));
        }
        return builder.build();
    }

    private static Block fullyRandomDoubleVector(int positions) {
        return doubleVector(positions, i -> randomDouble()).asBlock();
    }

    private static BytesRefVector bytesRefVector(int positions, IntFunction<BytesRef> v) {
        BytesRefVector.Builder builder = BLOCK_FACTORY.newBytesRefVectorBuilder(positions);
        for (int i = 0; i < positions; i++) {
            builder.appendBytesRef(v.apply(i));
        }
        return builder.build();
    }

    private static Block fullyRandomKeywordVector(int positions) {
        return bytesRefVector(positions, i -> new BytesRef(randomAlphaOfLengthBetween(0, 100))).asBlock();
    }

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test-noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private static class TestCase {
        private final String description;
        private final List<ArrowResponse.Column> columns;
        private final Supplier<List<Page>> pages;
        private ArrowResponse response;

        static void cases(List<TestCase> cases, String description, List<ArrowResponse.Column> columns, Supplier<Page> page) {
            cases.add(onePage(description, columns, page));
            cases.add(twoPages(description, columns, page));
            cases.add(randomPages(description, columns, page));
        }

        static TestCase onePage(String description, List<ArrowResponse.Column> columns, Supplier<Page> page) {
            return new TestCase(description + " one page", columns, () -> List.of(page.get()));
        }

        static TestCase twoPages(String description, List<ArrowResponse.Column> columns, Supplier<Page> page) {
            return new TestCase(description + " two pages", columns, () -> List.of(page.get(), page.get()));
        }

        static TestCase randomPages(String description, List<ArrowResponse.Column> columns, Supplier<Page> page) {
            return new TestCase(description + " random pages", columns, () -> {
                int pageCount = between(0, 100);
                List<Page> pages = new ArrayList<>(pageCount);
                for (int p = 0; p < pageCount; p++) {
                    int positions = between(1, 10_000);
                    pages.add(page.get());
                }
                return pages;
            });
        }

        private TestCase(String description, List<ArrowResponse.Column> columns, Supplier<List<Page>> pages) {
            this.description = description;
            this.columns = columns;
            this.pages = pages;
        }

        ArrowResponse response() {
            if (response == null) {
                response = new ArrowResponse(columns, pages.get(), () -> {});
            }
            return response;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
