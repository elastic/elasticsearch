/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ChunkTests extends AbstractScalarFunctionTestCase {

    private static String PARAGRAPH_INPUT = """
        The Adirondacks, a vast mountain region in northern New York, offer a breathtaking mix of rugged wilderness, serene lakes,
        and charming small towns. Spanning over six million acres, the Adirondack Park is larger than Yellowstone, Yosemite, and the
        Grand Canyon combined, yet it’s dotted with communities where people live, work, and play amidst nature. Visitors come year-round
        to hike High Peaks trails, paddle across mirror-like waters, or ski through snow-covered forests. The area’s pristine beauty,
        rich history, and commitment to conservation create a unique balance between wild preservation and human presence, making
        the Adirondacks a timeless escape into natural tranquility.
        """;

    public ChunkTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static String randomWordsBetween(int min, int max) {
        return IntStream.range(0, randomIntBetween(min, max))
            .mapToObj(i -> randomAlphaOfLengthBetween(1, 10))
            .collect(Collectors.joining(" "));
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            List.of(new TestCaseSupplier("Chunk basic test", List.of(DataType.KEYWORD, DataType.INTEGER, DataType.INTEGER), () -> {
                String text = randomWordsBetween(25, 50);
                int numChunks = between(1, 5);
                int chunkSize = between(10, 20);
                ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(chunkSize, 0);

                List<String> chunks = Chunk.chunkText(text, chunkingSettings, numChunks);
                Object expectedResult = chunks.size() == 1
                    ? new BytesRef(chunks.get(0).trim())
                    : chunks.stream().map(s -> new BytesRef(s.trim())).toList();

                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                        new TestCaseSupplier.TypedData(numChunks, DataType.INTEGER, "num_chunks"),
                        new TestCaseSupplier.TypedData(chunkSize, DataType.INTEGER, "chunk_size")
                    ),
                    "ChunkBytesRefEvaluator[str=Attribute[channel=0], numChunks=Attribute[channel=1], chunkSize=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(expectedResult)
                );
            }), new TestCaseSupplier("Chunk basic test with text input", List.of(DataType.TEXT, DataType.INTEGER, DataType.INTEGER), () -> {
                String text = randomWordsBetween(25, 50);
                int numChunks = between(1, 5);
                int chunkSize = between(10, 20);
                ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(chunkSize, 0);

                List<String> chunks = Chunk.chunkText(text, chunkingSettings, numChunks);
                Object expectedResult = chunks.size() == 1
                    ? new BytesRef(chunks.get(0).trim())
                    : chunks.stream().map(s -> new BytesRef(s.trim())).toList();

                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(text), DataType.TEXT, "str"),
                        new TestCaseSupplier.TypedData(numChunks, DataType.INTEGER, "num_chunks"),
                        new TestCaseSupplier.TypedData(chunkSize, DataType.INTEGER, "chunk_size")
                    ),
                    "ChunkBytesRefEvaluator[str=Attribute[channel=0], numChunks=Attribute[channel=1], chunkSize=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(expectedResult)
                );
            }))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Chunk(source, args.get(0), args.size() < 2 ? null : args.get(1), args.size() < 3 ? null : args.get(2));
    }

    public void testNegativeNumChunks() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process("a tiger", -1, 10));
        assertThat(ex.getMessage(), containsString("Num chunks parameter cannot be negative, found [-1]"));
    }

    public void testNegativeChunkSize() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process("a tiger", 1, -1));
        assertThat(ex.getMessage(), containsString("Chunk size parameter cannot be negative, found [-1]"));
    }

    public void testDefaults() {
        ChunkingSettings settings = new SentenceBoundaryChunkingSettings(Chunk.DEFAULT_CHUNK_SIZE, 0);
        List<String> expected = Chunk.chunkText(PARAGRAPH_INPUT, settings, Chunk.DEFAULT_NUM_CHUNKS).stream().map(String::trim).toList();

        List<String> result = process(PARAGRAPH_INPUT, null, null);
        assertThat(result, equalTo(expected));
    }

    public void testDefaultNumChunks() {
        int chunkSize = randomIntBetween(20, 30);
        ChunkingSettings settings = new SentenceBoundaryChunkingSettings(chunkSize, 0);
        List<String> expected = Chunk.chunkText(PARAGRAPH_INPUT, settings, Chunk.DEFAULT_NUM_CHUNKS).stream().map(String::trim).toList();

        List<String> result = process(PARAGRAPH_INPUT, null, chunkSize);
        assertThat(result, equalTo(expected));
    }

    public void testDefaultChunkSize() {
        int numChunks = randomIntBetween(1, 3);
        ChunkingSettings settings = new SentenceBoundaryChunkingSettings(Chunk.DEFAULT_CHUNK_SIZE, 0);
        List<String> expected = Chunk.chunkText(PARAGRAPH_INPUT, settings, numChunks).stream().map(String::trim).toList();

        List<String> result = process(PARAGRAPH_INPUT, numChunks, null);
        assertThat(result, equalTo(expected));
    }

    private List<String> process(String str, Integer numChunks, Integer chunkSize) {
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new Chunk(
                    Source.EMPTY,
                    field("str", DataType.KEYWORD),
                    numChunks == null ? null : new Literal(Source.EMPTY, numChunks, DataType.INTEGER),
                    chunkSize == null ? null : new Literal(Source.EMPTY, chunkSize, DataType.INTEGER)
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(str))))
        ) {
            if (block.isNull(0)) {
                return null;
            }
            Object result = toJavaObject(block, 0);
            if (result instanceof BytesRef bytesRef) {
                return List.of(bytesRef.utf8ToString());
            } else {
                @SuppressWarnings("unchecked")
                List<BytesRef> list = (List<BytesRef>) result;
                return list.stream().map(BytesRef::utf8ToString).toList();
            }
        }
    }

}
