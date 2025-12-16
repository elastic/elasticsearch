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
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.Chunk.CHUNK_SIZE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.Chunk.NUM_CHUNKS;
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
            List.of(new TestCaseSupplier("Chunk with defaults", List.of(DataType.KEYWORD), () -> {
                String text = randomWordsBetween(25, 50);
                ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(Chunk.DEFAULT_CHUNK_SIZE, 0);

                List<String> chunks = Chunk.chunkText(text, chunkingSettings, Chunk.DEFAULT_NUM_CHUNKS);
                Object expectedResult = chunks.size() == 1
                    ? new BytesRef(chunks.get(0).trim())
                    : chunks.stream().map(s -> new BytesRef(s.trim())).toList();

                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str")),
                    "ChunkBytesRefEvaluator[str=Attribute[channel=0], numChunks=LiteralsEvaluator[lit="
                        + Chunk.DEFAULT_NUM_CHUNKS
                        + "], chunkSize=LiteralsEvaluator[lit="
                        + Chunk.DEFAULT_CHUNK_SIZE
                        + "]]",
                    DataType.KEYWORD,
                    equalTo(expectedResult)
                );
            }), new TestCaseSupplier("Chunk with defaults text input", List.of(DataType.TEXT), () -> {
                String text = randomWordsBetween(25, 50);
                ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(Chunk.DEFAULT_CHUNK_SIZE, 0);

                List<String> chunks = Chunk.chunkText(text, chunkingSettings, Chunk.DEFAULT_NUM_CHUNKS);
                Object expectedResult = chunks.size() == 1
                    ? new BytesRef(chunks.get(0).trim())
                    : chunks.stream().map(s -> new BytesRef(s.trim())).toList();

                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef(text), DataType.TEXT, "str")),
                    "ChunkBytesRefEvaluator[str=Attribute[channel=0], numChunks=LiteralsEvaluator[lit="
                        + Chunk.DEFAULT_NUM_CHUNKS
                        + "], chunkSize=LiteralsEvaluator[lit="
                        + Chunk.DEFAULT_CHUNK_SIZE
                        + "]]",
                    DataType.KEYWORD,
                    equalTo(expectedResult)
                );
            }))
        );
    }

    private static MapExpression createOptionsMap(Integer numChunks, Integer chunkSize) {
        List<Expression> keyValuePairs = new ArrayList<>();

        if (Objects.nonNull(numChunks)) {
            keyValuePairs.add(Literal.keyword(Source.EMPTY, NUM_CHUNKS));
            keyValuePairs.add(new Literal(Source.EMPTY, numChunks, DataType.INTEGER));
        }

        if (Objects.nonNull(chunkSize)) {
            keyValuePairs.add(Literal.keyword(Source.EMPTY, CHUNK_SIZE));
            keyValuePairs.add(new Literal(Source.EMPTY, chunkSize, DataType.INTEGER));
        }

        return new MapExpression(Source.EMPTY, keyValuePairs);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        // With MapParam, args contains: field, options_map
        Expression options = args.size() < 2 ? null : args.get(1);
        // TODO needed?
        if (options instanceof Literal lit && lit.value() == null) {
            options = null;
        }
        return new Chunk(source, args.get(0), options);
    }

    public void testDefaults() {
        // Default of 300 is huge, only one chunk returned in this case
        verifyChunks(null, null, 1);
    }

    public void testDefaultNumChunks() {
        int chunkSize = 20;
        verifyChunks(null, chunkSize, 8);
    }

    public void testDefaultChunkSize() {
        int numChunks = 1; // Default of 300 is huge, only one chunk returned in this case
        verifyChunks(numChunks, null, numChunks);
    }

    public void testSpecifiedOptions() {
        int numChunks = randomIntBetween(2, 4);
        int chunkSize = randomIntBetween(20, 30);
        verifyChunks(numChunks, chunkSize, numChunks);
    }

    private void verifyChunks(Integer numChunks, Integer chunkSize, int expectedNumChunksReturned) {
        int numChunksOrDefault = numChunks != null ? numChunks : Chunk.DEFAULT_NUM_CHUNKS;
        int chunkSizeOrDefault = chunkSize != null ? chunkSize : Chunk.DEFAULT_CHUNK_SIZE;
        ChunkingSettings settings = new SentenceBoundaryChunkingSettings(chunkSizeOrDefault, 0);
        List<String> expected = Chunk.chunkText(PARAGRAPH_INPUT, settings, numChunksOrDefault).stream().map(String::trim).toList();

        List<String> result = process(PARAGRAPH_INPUT, numChunksOrDefault, chunkSizeOrDefault);
        assertThat(result.size(), equalTo(expectedNumChunksReturned));
        assertThat(result, equalTo(expected));
    }

    private List<String> process(String str, Integer numChunks, Integer chunkSize) {
        MapExpression optionsMap = (numChunks == null && chunkSize == null) ? null : createOptionsMap(numChunks, chunkSize);

        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(new Chunk(Source.EMPTY, field("str", DataType.KEYWORD), optionsMap)).get(
                driverContext()
            );
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
