/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.model.TestModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.toSemanticTextFieldChunks;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SemanticTextFieldTests extends AbstractXContentTestCase<SemanticTextField> {
    private static final String NAME = "field";

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return n -> n.endsWith(CHUNKED_EMBEDDINGS_FIELD);
    }

    @Override
    protected void assertEqualInstances(SemanticTextField expectedInstance, SemanticTextField newInstance) {
        assertThat(newInstance.fieldName(), equalTo(expectedInstance.fieldName()));
        assertThat(newInstance.originalValues(), equalTo(expectedInstance.originalValues()));
        assertThat(newInstance.inference().modelSettings(), equalTo(expectedInstance.inference().modelSettings()));
        assertThat(newInstance.inference().chunks().size(), equalTo(expectedInstance.inference().chunks().size()));
        SemanticTextField.ModelSettings modelSettings = newInstance.inference().modelSettings();
        for (int i = 0; i < newInstance.inference().chunks().size(); i++) {
            assertThat(newInstance.inference().chunks().get(i).text(), equalTo(expectedInstance.inference().chunks().get(i).text()));
            switch (modelSettings.taskType()) {
                case TEXT_EMBEDDING -> {
                    double[] expectedVector = parseDenseVector(
                        expectedInstance.inference().chunks().get(i).rawEmbeddings(),
                        modelSettings.dimensions(),
                        expectedInstance.contentType()
                    );
                    double[] newVector = parseDenseVector(
                        newInstance.inference().chunks().get(i).rawEmbeddings(),
                        modelSettings.dimensions(),
                        newInstance.contentType()
                    );
                    assertArrayEquals(expectedVector, newVector, 0f);
                }
                case SPARSE_EMBEDDING -> {
                    List<TextExpansionResults.WeightedToken> expectedTokens = parseWeightedTokens(
                        expectedInstance.inference().chunks().get(i).rawEmbeddings(),
                        expectedInstance.contentType()
                    );
                    List<TextExpansionResults.WeightedToken> newTokens = parseWeightedTokens(
                        newInstance.inference().chunks().get(i).rawEmbeddings(),
                        newInstance.contentType()
                    );
                    assertThat(newTokens, equalTo(expectedTokens));
                }
                default -> throw new AssertionError("Invalid task type " + modelSettings.taskType());
            }
        }
    }

    @Override
    protected SemanticTextField createTestInstance() {
        List<String> rawValues = randomList(1, 5, () -> randomAlphaOfLengthBetween(10, 20));
        return randomSemanticText(NAME, TestModel.createRandomInstance(), rawValues, randomFrom(XContentType.values()));
    }

    @Override
    protected SemanticTextField doParseInstance(XContentParser parser) throws IOException {
        return SemanticTextField.parse(parser, new Tuple<>(NAME, parser.contentType()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testModelSettingsValidation() {
        NullPointerException npe = expectThrows(NullPointerException.class, () -> {
            new SemanticTextField.ModelSettings(null, 10, SimilarityMeasure.COSINE);
        });
        assertThat(npe.getMessage(), equalTo("task type must not be null"));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(TaskType.COMPLETION, 10, SimilarityMeasure.COSINE);
        });
        assertThat(ex.getMessage(), containsString("Wrong [task_type]"));

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> { new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, 10, null); }
        );
        assertThat(ex.getMessage(), containsString("[dimensions] is not allowed"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, SimilarityMeasure.COSINE);
        });
        assertThat(ex.getMessage(), containsString("[similarity] is not allowed"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(TaskType.TEXT_EMBEDDING, null, SimilarityMeasure.COSINE);
        });
        assertThat(ex.getMessage(), containsString("required [dimensions] field is missing"));

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> { new SemanticTextField.ModelSettings(TaskType.TEXT_EMBEDDING, 10, null); }
        );
        assertThat(ex.getMessage(), containsString("required [similarity] field is missing"));
    }

    public static ChunkedTextEmbeddingResults randomTextEmbeddings(Model model, List<String> inputs) {
        List<org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk> chunks = new ArrayList<>();
        for (String input : inputs) {
            double[] values = new double[model.getServiceSettings().dimensions()];
            for (int j = 0; j < values.length; j++) {
                values[j] = randomDouble();
            }
            chunks.add(new org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk(input, values));
        }
        return new ChunkedTextEmbeddingResults(chunks);
    }

    public static ChunkedSparseEmbeddingResults randomSparseEmbeddings(List<String> inputs) {
        List<ChunkedTextExpansionResults.ChunkedResult> chunks = new ArrayList<>();
        for (String input : inputs) {
            var tokens = new ArrayList<TextExpansionResults.WeightedToken>();
            for (var token : input.split("\\s+")) {
                tokens.add(new TextExpansionResults.WeightedToken(token, randomFloat()));
            }
            chunks.add(new ChunkedTextExpansionResults.ChunkedResult(input, tokens));
        }
        return new ChunkedSparseEmbeddingResults(chunks);
    }

    public static SemanticTextField randomSemanticText(String fieldName, Model model, List<String> inputs, XContentType contentType) {
        ChunkedInferenceServiceResults results = switch (model.getTaskType()) {
            case TEXT_EMBEDDING -> randomTextEmbeddings(model, inputs);
            case SPARSE_EMBEDDING -> randomSparseEmbeddings(inputs);
            default -> throw new AssertionError("invalid task type: " + model.getTaskType().name());
        };
        return new SemanticTextField(
            fieldName,
            inputs,
            new SemanticTextField.InferenceResult(
                model.getInferenceEntityId(),
                new SemanticTextField.ModelSettings(model),
                toSemanticTextFieldChunks(fieldName, model.getInferenceEntityId(), List.of(results), contentType)
            ),
            contentType
        );
    }

    public static ChunkedInferenceServiceResults toChunkedResult(SemanticTextField field) {
        switch (field.inference().modelSettings().taskType()) {
            case SPARSE_EMBEDDING -> {
                List<ChunkedTextExpansionResults.ChunkedResult> chunks = new ArrayList<>();
                for (var chunk : field.inference().chunks()) {
                    var tokens = parseWeightedTokens(chunk.rawEmbeddings(), field.contentType());
                    chunks.add(new ChunkedTextExpansionResults.ChunkedResult(chunk.text(), tokens));
                }
                return new ChunkedSparseEmbeddingResults(chunks);
            }
            case TEXT_EMBEDDING -> {
                List<org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk> chunks =
                    new ArrayList<>();
                for (var chunk : field.inference().chunks()) {
                    double[] values = parseDenseVector(
                        chunk.rawEmbeddings(),
                        field.inference().modelSettings().dimensions(),
                        field.contentType()
                    );
                    chunks.add(
                        new org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk(
                            chunk.text(),
                            values
                        )
                    );
                }
                return new ChunkedTextEmbeddingResults(chunks);
            }
            default -> throw new AssertionError("Invalid task_type: " + field.inference().modelSettings().taskType().name());
        }
    }

    private static double[] parseDenseVector(BytesReference value, int numDims, XContentType contentType) {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, value, contentType)) {
            parser.nextToken();
            assertThat(parser.currentToken(), equalTo(XContentParser.Token.START_ARRAY));
            double[] values = new double[numDims];
            for (int i = 0; i < numDims; i++) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
                values[i] = parser.doubleValue();
            }
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_ARRAY));
            return values;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<TextExpansionResults.WeightedToken> parseWeightedTokens(BytesReference value, XContentType contentType) {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, value, contentType)) {
            Map<String, Object> map = parser.map();
            List<TextExpansionResults.WeightedToken> weightedTokens = new ArrayList<>();
            for (var entry : map.entrySet()) {
                weightedTokens.add(new TextExpansionResults.WeightedToken(entry.getKey(), ((Number) entry.getValue()).floatValue()));
            }
            return weightedTokens;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
