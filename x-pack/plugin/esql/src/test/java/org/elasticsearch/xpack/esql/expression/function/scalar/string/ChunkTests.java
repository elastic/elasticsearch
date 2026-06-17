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

import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.Chunk.ALLOWED_CHUNKING_SETTING_OPTIONS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.hamcrest.Matchers.equalTo;

public class ChunkTests extends AbstractScalarFunctionTestCase {
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

    public static MapExpression createChunkingSettings(ChunkingSettings chunkingSettings) {
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
}
