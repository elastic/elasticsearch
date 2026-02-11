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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.Chunk.ALLOWED_CHUNKING_SETTING_OPTIONS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.Chunk.DEFAULT_CHUNKING_SETTINGS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
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
        return parameterSuppliersFromTypedData(testCaseSuppliers());
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(createTestCaseSupplier("Chunk with defaults", DataType.KEYWORD));
        suppliers.add(createTestCaseSupplier("Chunk with defaults text input", DataType.TEXT));
        return addFunctionNamedParams(suppliers);
    }

    private static TestCaseSupplier createTestCaseSupplier(String description, DataType fieldDataType) {
        return new TestCaseSupplier(description, List.of(fieldDataType), () -> {
            String text = randomWordsBetween(25, 50);
            ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(Chunk.DEFAULT_CHUNK_SIZE, 0);

            List<String> chunks = chunkText(text, chunkingSettings);
            Object expectedResult = chunks.size() == 1
                ? new BytesRef(chunks.get(0).trim())
                : chunks.stream().map(s -> new BytesRef(s.trim())).toList();

            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(text), fieldDataType, "field")),
                "ChunkEvaluator[field=Attribute[channel=0], "
                    + "chunkingSettings={\"strategy\":\"sentence\",\"max_chunk_size\":300,\"sentence_overlap\":0}]",
                DataType.KEYWORD,
                equalTo(expectedResult)
            );
        });
    }

    /**
     * Adds function named parameters to all the test case suppliers provided
     */
    private static List<TestCaseSupplier> addFunctionNamedParams(List<TestCaseSupplier> suppliers) {
        List<TestCaseSupplier> result = new ArrayList<>(suppliers);
        for (TestCaseSupplier supplier : suppliers) {
            List<DataType> dataTypes = new ArrayList<>(supplier.types());
            dataTypes.add(UNSUPPORTED);
            result.add(new TestCaseSupplier(supplier.name() + ", with chunking_settings", dataTypes, () -> {
                String text = randomWordsBetween(25, 50);
                int chunkSize = 25;
                ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(chunkSize, 0);

                List<String> chunks = chunkText(text, chunkingSettings);
                Object expectedResult = chunks.size() == 1
                    ? new BytesRef(chunks.get(0).trim())
                    : chunks.stream().map(s -> new BytesRef(s.trim())).toList();

                List<TestCaseSupplier.TypedData> values = List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), supplier.types().get(0), "field"),
                    new TestCaseSupplier.TypedData(createChunkingSettings(chunkingSettings), UNSUPPORTED, "chunking_settings")
                        .forceLiteral()
                );

                return new TestCaseSupplier.TestCase(
                    values,
                    "ChunkEvaluator[field=Attribute[channel=0], "
                        + "chunkingSettings={\"strategy\":\"sentence\",\"max_chunk_size\":25,\"sentence_overlap\":0}]",
                    DataType.KEYWORD,
                    equalTo(expectedResult)
                );
            }));
        }
        return result;
    }

    private static MapExpression createChunkingSettings(ChunkingSettings chunkingSettings) {
        List<Expression> chunkingSettingsMap = new ArrayList<>();

        if (Objects.nonNull(chunkingSettings)) {
            chunkingSettings.asMap().forEach((k, v) -> {
                chunkingSettingsMap.add(Literal.keyword(Source.EMPTY, k));
                DataType dataType = ALLOWED_CHUNKING_SETTING_OPTIONS.get(k);
                Object value = v;
                if (dataType == DataType.KEYWORD) {
                    if (v instanceof List<?> list) {
                        value = list.stream().map(item -> BytesRefs.toBytesRef(item)).toList();
                    } else if (v instanceof String str) {
                        value = BytesRefs.toBytesRef(str);
                    }
                }
                chunkingSettingsMap.add(new Literal(Source.EMPTY, value, dataType));
            });
        }

        return new MapExpression(Source.EMPTY, chunkingSettingsMap);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        // With MapParam, args contains: field, options_map
        Expression options = args.size() < 2 ? null : args.get(1);
        return new Chunk(source, args.get(0), options);
    }

    @Override
    public void testFold() {
        Expression expression = buildFieldExpression(testCase);
        // Skip testFold if the expression is not foldable (e.g., when chunking_settings contains MapExpression)
        if (expression.foldable() == false) {
            return;
        }
        super.testFold();
    }

    public void testDefaults() {
        // Default of 300 is huge, only one chunk returned in this case
        verifyChunks(null, 1);
    }

    public void testDefaultChunkingSettings() {
        verifyChunks(null, 1);
    }

    public void testSpecifiedChunkingSettings() {
        // We can't randomize here, because we're testing on specifically specified chunk size that's variable.
        int chunkSize = 25;
        int expectedNumChunks = 6;
        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(chunkSize, 0);
        verifyChunks(chunkingSettings, expectedNumChunks);
    }

    public void testRandomChunkingSettings() {
        ChunkingSettings chunkingSettings = createRandomChunkingSettings();
        List<String> result = process(PARAGRAPH_INPUT, chunkingSettings);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        // Actual results depend on chunking settings passed in
    }

    // Paranoia check, this test will fail if we add new chunking settings options without updating the Chunk function
    public void testChunkDefinesAllAllowedChunkingSettingsOptions() {
        Set<String> allowedOptions = ALLOWED_CHUNKING_SETTING_OPTIONS.keySet();
        Set<String> allOptions = Arrays.stream(ChunkingSettingsOptions.values())
            .map(ChunkingSettingsOptions::toString)
            .collect(Collectors.toSet());

        assertEquals(allOptions, allowedOptions);
    }

    private void verifyChunks(ChunkingSettings chunkingSettings, int expectedNumChunksReturned) {
        ChunkingSettings chunkingSettingsOrDefault = chunkingSettings != null ? chunkingSettings : DEFAULT_CHUNKING_SETTINGS;
        List<String> expected = chunkText(PARAGRAPH_INPUT, chunkingSettingsOrDefault).stream().map(String::trim).toList();

        List<String> result = process(PARAGRAPH_INPUT, chunkingSettingsOrDefault);
        assertThat(result.size(), equalTo(expectedNumChunksReturned));
        assertThat(result, equalTo(expected));
    }

    private List<String> process(String str, ChunkingSettings chunkingSettings) {
        MapExpression optionsMap = chunkingSettings == null ? null : createChunkingSettings(chunkingSettings);

        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(new Chunk(Source.EMPTY, field("field", DataType.KEYWORD), optionsMap)).get(
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
