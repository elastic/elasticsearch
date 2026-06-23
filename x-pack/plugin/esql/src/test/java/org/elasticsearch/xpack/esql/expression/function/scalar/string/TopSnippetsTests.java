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
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
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
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets.DEFAULT_NUM_SNIPPETS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets.DEFAULT_WORD_SIZE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.hamcrest.Matchers.equalTo;

public class TopSnippetsTests extends AbstractScalarFunctionTestCase {

    public TopSnippetsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
        suppliers.add(createTestCaseSupplier("TopSnippets with defaults", DataType.KEYWORD, DataType.KEYWORD));
        suppliers.add(createTestCaseSupplier("TopSnippets with defaults text input", DataType.TEXT, DataType.KEYWORD));
        return addFunctionNamedParams(suppliers);
    }

    private static TestCaseSupplier createTestCaseSupplier(String description, DataType fieldDataType, DataType queryDataType) {
        return new TestCaseSupplier(description, List.of(fieldDataType, queryDataType), () -> {
            String text = randomWordsBetween(25, 50);
            String query = randomFrom("park", "nature", "trail");
            ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(DEFAULT_WORD_SIZE, 0);

            List<String> chunks = chunkText(text, chunkingSettings);
            MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();
            List<String> scoredChunks = scorer.scoreChunks(chunks, query, DEFAULT_NUM_SNIPPETS, false)
                .stream()
                .map(ScoredChunk::content)
                .toList();

            Object expectedResult;
            if (scoredChunks.isEmpty()) {
                expectedResult = null;
            } else if (scoredChunks.size() == 1) {
                expectedResult = new BytesRef(scoredChunks.get(0).trim());
            } else {
                expectedResult = scoredChunks.stream().map(s -> new BytesRef(s.trim())).toList();
            }

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), fieldDataType, "field"),
                    new TestCaseSupplier.TypedData(new BytesRef(query), DataType.KEYWORD, "query").forceLiteral()
                ),
                "TopSnippetsEvaluator[field=Attribute[channel=0], queryString="
                    + query
                    + ", "
                    + "chunkingSettings={\"strategy\":\"sentence\",\"max_chunk_size\":300,\"sentence_overlap\":0}, "
                    + "scorer=MemoryIndexChunkScorer, numSnippets=5, docOrder=false]",
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
            result.add(new TestCaseSupplier(supplier.name() + ", with options", dataTypes, () -> {
                String text = randomWordsBetween(25, 50);
                String query = randomFrom("park", "nature", "trail");
                int numSnippets = 3;
                int numWords = 25;
                ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(numWords, 0);

                List<String> chunks = chunkText(text, chunkingSettings);
                MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();
                List<String> scoredChunks = scorer.scoreChunks(chunks, query, numSnippets, false)
                    .stream()
                    .map(ScoredChunk::content)
                    .toList();

                Object expectedResult;
                if (scoredChunks.isEmpty()) {
                    expectedResult = null;
                } else if (scoredChunks.size() == 1) {
                    expectedResult = new BytesRef(scoredChunks.get(0).trim());
                } else {
                    expectedResult = scoredChunks.stream().map(s -> new BytesRef(s.trim())).toList();
                }

                List<TestCaseSupplier.TypedData> values = List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), supplier.types().get(0), "field"),
                    new TestCaseSupplier.TypedData(new BytesRef(query), DataType.KEYWORD, "query").forceLiteral(),
                    new TestCaseSupplier.TypedData(createOptions(numSnippets, numWords), UNSUPPORTED, "options").forceLiteral()
                );

                return new TestCaseSupplier.TestCase(
                    values,
                    "TopSnippetsEvaluator[field=Attribute[channel=0], queryString="
                        + query
                        + ", "
                        + "chunkingSettings={\"strategy\":\"sentence\",\"max_chunk_size\":25,\"sentence_overlap\":0}, "
                        + "scorer=MemoryIndexChunkScorer, numSnippets=3, docOrder=false]",
                    DataType.KEYWORD,
                    equalTo(expectedResult)
                );
            }));
        }
        return result;
    }

    private static MapExpression createOptions(Integer numSnippets, Integer numWords) {
        return createOptions(numSnippets, numWords, null, null, null, null, null, null);
    }

    public static MapExpression createOptions(
        Integer numSnippets,
        Integer numWords,
        Boolean highlight,
        String preTag,
        String postTag,
        String encoder,
        String order,
        String analyzer
    ) {
        List<Expression> optionsMap = new ArrayList<>();

        if (Objects.nonNull(numSnippets)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "num_snippets"));
            optionsMap.add(new Literal(Source.EMPTY, numSnippets, DataType.INTEGER));
        }

        if (Objects.nonNull(numWords)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "num_words"));
            optionsMap.add(new Literal(Source.EMPTY, numWords, DataType.INTEGER));
        }

        if (Objects.nonNull(highlight)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "highlight"));
            optionsMap.add(new Literal(Source.EMPTY, highlight, DataType.BOOLEAN));
        }

        if (Objects.nonNull(preTag)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "pre_tag"));
            optionsMap.add(Literal.keyword(Source.EMPTY, preTag));
        }

        if (Objects.nonNull(postTag)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "post_tag"));
            optionsMap.add(Literal.keyword(Source.EMPTY, postTag));
        }

        if (Objects.nonNull(encoder)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "encoder"));
            optionsMap.add(Literal.keyword(Source.EMPTY, encoder));
        }

        if (Objects.nonNull(order)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "order"));
            optionsMap.add(Literal.keyword(Source.EMPTY, order));
        }

        if (Objects.nonNull(analyzer)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "analyzer"));
            optionsMap.add(Literal.keyword(Source.EMPTY, analyzer));
        }

        return optionsMap.isEmpty() ? null : new MapExpression(Source.EMPTY, optionsMap);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression options = args.size() < 3 ? null : args.get(2);
        return new TopSnippets(source, args.get(0), args.get(1), options);
    }

    @Override
    public void testFold() {
        Expression expression = buildFieldExpression(testCase);
        // Skip testFold if the expression is not foldable (e.g., when options contains MapExpression)
        if (expression.foldable() == false) {
            return;
        }
        super.testFold();
    }
}
