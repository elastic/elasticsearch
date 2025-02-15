/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingByte;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingFloat;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingSparse;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.core.utils.FloatConversionUtils;
import org.elasticsearch.xpack.inference.model.TestModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.toSemanticTextFieldChunk;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SemanticTextFieldTests extends AbstractXContentTestCase<SemanticTextField> {
    private static final String NAME = "field";

    private final boolean useLegacyFormat;

    public SemanticTextFieldTests(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return n -> n.endsWith(CHUNKED_EMBEDDINGS_FIELD);
    }

    @Override
    protected void assertEqualInstances(SemanticTextField expectedInstance, SemanticTextField newInstance) {
        assertThat(newInstance.useLegacyFormat(), equalTo(newInstance.useLegacyFormat()));
        assertThat(newInstance.fieldName(), equalTo(expectedInstance.fieldName()));
        assertThat(newInstance.originalValues(), equalTo(expectedInstance.originalValues()));
        assertThat(newInstance.inference().modelSettings(), equalTo(expectedInstance.inference().modelSettings()));
        assertThat(newInstance.inference().chunks().size(), equalTo(expectedInstance.inference().chunks().size()));
        MinimalServiceSettings modelSettings = newInstance.inference().modelSettings();
        for (var entry : newInstance.inference().chunks().entrySet()) {
            var expectedChunks = expectedInstance.inference().chunks().get(entry.getKey());
            assertNotNull(expectedChunks);
            assertThat(entry.getValue().size(), equalTo(expectedChunks.size()));
            for (int i = 0; i < entry.getValue().size(); i++) {
                var actualChunk = entry.getValue().get(i);
                assertThat(actualChunk.text(), equalTo(expectedChunks.get(i).text()));
                assertThat(actualChunk.startOffset(), equalTo(expectedChunks.get(i).startOffset()));
                assertThat(actualChunk.endOffset(), equalTo(expectedChunks.get(i).endOffset()));
                switch (modelSettings.taskType()) {
                    case TEXT_EMBEDDING -> {
                        double[] expectedVector = parseDenseVector(
                            expectedChunks.get(i).rawEmbeddings(),
                            modelSettings.dimensions(),
                            expectedInstance.contentType()
                        );
                        double[] newVector = parseDenseVector(
                            actualChunk.rawEmbeddings(),
                            modelSettings.dimensions(),
                            newInstance.contentType()
                        );
                        assertArrayEquals(expectedVector, newVector, 0.0000001f);
                    }
                    case SPARSE_EMBEDDING -> {
                        List<WeightedToken> expectedTokens = parseWeightedTokens(
                            expectedChunks.get(i).rawEmbeddings(),
                            expectedInstance.contentType()
                        );
                        List<WeightedToken> newTokens = parseWeightedTokens(actualChunk.rawEmbeddings(), newInstance.contentType());
                        assertThat(newTokens, equalTo(expectedTokens));
                    }
                    default -> throw new AssertionError("Invalid task type " + modelSettings.taskType());
                }
            }
        }
    }

    @Override
    protected SemanticTextField createTestInstance() {
        List<String> rawValues = randomList(1, 5, () -> randomSemanticTextInput().toString());
        try { // try catch required for override
            return randomSemanticText(
                useLegacyFormat,
                NAME,
                TestModel.createRandomInstance(),
                rawValues,
                randomFrom(XContentType.values())
            );
        } catch (IOException e) {
            fail("Failed to create random SemanticTextField instance");
        }
        return null;
    }

    @Override
    protected SemanticTextField doParseInstance(XContentParser parser) throws IOException {
        return SemanticTextField.parse(parser, new SemanticTextField.ParserContext(useLegacyFormat, NAME, parser.contentType()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testModelSettingsValidation() {
        NullPointerException npe = expectThrows(NullPointerException.class, () -> {
            new MinimalServiceSettings(null, 10, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);
        });
        assertThat(npe.getMessage(), equalTo("task type must not be null"));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            new MinimalServiceSettings(TaskType.SPARSE_EMBEDDING, 10, null, null);
        });
        assertThat(ex.getMessage(), containsString("[dimensions] is not allowed"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new MinimalServiceSettings(TaskType.SPARSE_EMBEDDING, null, SimilarityMeasure.COSINE, null);
        });
        assertThat(ex.getMessage(), containsString("[similarity] is not allowed"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new MinimalServiceSettings(TaskType.SPARSE_EMBEDDING, null, null, DenseVectorFieldMapper.ElementType.FLOAT);
        });
        assertThat(ex.getMessage(), containsString("[element_type] is not allowed"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new MinimalServiceSettings(TaskType.TEXT_EMBEDDING, null, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);
        });
        assertThat(ex.getMessage(), containsString("required [dimensions] field is missing"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new MinimalServiceSettings(TaskType.TEXT_EMBEDDING, 10, null, DenseVectorFieldMapper.ElementType.FLOAT);
        });
        assertThat(ex.getMessage(), containsString("required [similarity] field is missing"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new MinimalServiceSettings(TaskType.TEXT_EMBEDDING, 10, SimilarityMeasure.COSINE, null);
        });
        assertThat(ex.getMessage(), containsString("required [element_type] field is missing"));
    }

    public static ChunkedInferenceEmbeddingByte randomChunkedInferenceEmbeddingByte(Model model, List<String> inputs) {
        List<ChunkedInferenceEmbeddingByte.ByteEmbeddingChunk> chunks = new ArrayList<>();
        for (String input : inputs) {
            byte[] values = new byte[model.getServiceSettings().dimensions()];
            for (int j = 0; j < values.length; j++) {
                values[j] = randomByte();
            }
            chunks.add(
                new ChunkedInferenceEmbeddingByte.ByteEmbeddingChunk(values, input, new ChunkedInference.TextOffset(0, input.length()))
            );
        }
        return new ChunkedInferenceEmbeddingByte(chunks);
    }

    public static ChunkedInferenceEmbeddingFloat randomChunkedInferenceEmbeddingFloat(Model model, List<String> inputs) {
        List<ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk> chunks = new ArrayList<>();
        for (String input : inputs) {
            float[] values = new float[model.getServiceSettings().dimensions()];
            for (int j = 0; j < values.length; j++) {
                values[j] = randomFloat();
            }
            chunks.add(
                new ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk(values, input, new ChunkedInference.TextOffset(0, input.length()))
            );
        }
        return new ChunkedInferenceEmbeddingFloat(chunks);
    }

    public static ChunkedInferenceEmbeddingSparse randomChunkedInferenceEmbeddingSparse(List<String> inputs) {
        return randomChunkedInferenceEmbeddingSparse(inputs, true);
    }

    public static ChunkedInferenceEmbeddingSparse randomChunkedInferenceEmbeddingSparse(List<String> inputs, boolean withFloats) {
        List<ChunkedInferenceEmbeddingSparse.SparseEmbeddingChunk> chunks = new ArrayList<>();
        for (String input : inputs) {
            var tokens = new ArrayList<WeightedToken>();
            for (var token : input.split("\\s+")) {
                tokens.add(new WeightedToken(token, withFloats ? randomFloat() : randomIntBetween(1, 255)));
            }
            chunks.add(
                new ChunkedInferenceEmbeddingSparse.SparseEmbeddingChunk(tokens, input, new ChunkedInference.TextOffset(0, input.length()))
            );
        }
        return new ChunkedInferenceEmbeddingSparse(chunks);
    }

    public static SemanticTextField randomSemanticText(
        boolean useLegacyFormat,
        String fieldName,
        Model model,
        List<String> inputs,
        XContentType contentType
    ) throws IOException {
        ChunkedInference results = switch (model.getTaskType()) {
            case TEXT_EMBEDDING -> switch (model.getServiceSettings().elementType()) {
                case FLOAT -> randomChunkedInferenceEmbeddingFloat(model, inputs);
                case BIT, BYTE -> randomChunkedInferenceEmbeddingByte(model, inputs);
            };
            case SPARSE_EMBEDDING -> randomChunkedInferenceEmbeddingSparse(inputs);
            default -> throw new AssertionError("invalid task type: " + model.getTaskType().name());
        };
        return semanticTextFieldFromChunkedInferenceResults(useLegacyFormat, fieldName, model, inputs, results, contentType);
    }

    public static SemanticTextField semanticTextFieldFromChunkedInferenceResults(
        boolean useLegacyFormat,
        String fieldName,
        Model model,
        List<String> inputs,
        ChunkedInference results,
        XContentType contentType
    ) throws IOException {

        // In this test framework, we don't perform "real" chunking; each input generates one chunk. Thus, we can assume there is a
        // one-to-one relationship between inputs and chunks. Iterate over the inputs and chunks to match each input with its
        // corresponding chunk.
        final List<SemanticTextField.Chunk> chunks = new ArrayList<>(inputs.size());
        int offsetAdjustment = 0;
        Iterator<String> inputsIt = inputs.iterator();
        Iterator<ChunkedInference.Chunk> chunkIt = results.chunksAsMatchedTextAndByteReference(contentType.xContent());
        while (inputsIt.hasNext() && chunkIt.hasNext()) {
            String input = inputsIt.next();
            var chunk = chunkIt.next();
            chunks.add(toSemanticTextFieldChunk(input, offsetAdjustment, chunk, useLegacyFormat));

            // When using the inference metadata fields format, all the input values are concatenated so that the
            // chunk text offsets are expressed in the context of a single string. Calculate the offset adjustment
            // to apply to account for this.
            offsetAdjustment = input.length() + 1; // Add one for separator char length
        }

        if (inputsIt.hasNext() || chunkIt.hasNext()) {
            throw new IllegalArgumentException("Input list size and chunk count do not match");
        }

        return new SemanticTextField(
            useLegacyFormat,
            fieldName,
            useLegacyFormat ? inputs : null,
            new SemanticTextField.InferenceResult(
                model.getInferenceEntityId(),
                new MinimalServiceSettings(model),
                Map.of(fieldName, chunks)
            ),
            contentType
        );
    }

    /**
     * Returns a randomly generated object for Semantic Text tests purpose.
     */
    public static Object randomSemanticTextInput() {
        if (rarely()) {
            return switch (randomIntBetween(0, 4)) {
                case 0 -> randomInt();
                case 1 -> randomLong();
                case 2 -> randomFloat();
                case 3 -> randomBoolean();
                case 4 -> randomDouble();
                default -> throw new IllegalStateException("Illegal state while generating random semantic text input");
            };
        } else {
            return randomAlphaOfLengthBetween(10, 20);
        }
    }

    public static ChunkedInference toChunkedResult(
        boolean useLegacyFormat,
        Map<String, List<String>> matchedTextMap,
        SemanticTextField field
    ) {
        switch (field.inference().modelSettings().taskType()) {
            case SPARSE_EMBEDDING -> {
                List<ChunkedInferenceEmbeddingSparse.SparseEmbeddingChunk> chunks = new ArrayList<>();
                for (var entry : field.inference().chunks().entrySet()) {
                    String entryField = entry.getKey();
                    List<SemanticTextField.Chunk> entryChunks = entry.getValue();
                    List<String> entryFieldMatchedText = validateAndGetMatchedTextForField(matchedTextMap, entryField, entryChunks.size());

                    ListIterator<String> matchedTextIt = entryFieldMatchedText.listIterator();
                    for (var chunk : entryChunks) {
                        String matchedText = matchedTextIt.next();
                        ChunkedInference.TextOffset offset = createOffset(useLegacyFormat, chunk, matchedText);
                        var tokens = parseWeightedTokens(chunk.rawEmbeddings(), field.contentType());
                        chunks.add(new ChunkedInferenceEmbeddingSparse.SparseEmbeddingChunk(tokens, matchedText, offset));
                    }
                }
                return new ChunkedInferenceEmbeddingSparse(chunks);
            }
            case TEXT_EMBEDDING -> {
                List<ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk> chunks = new ArrayList<>();
                for (var entry : field.inference().chunks().entrySet()) {
                    String entryField = entry.getKey();
                    List<SemanticTextField.Chunk> entryChunks = entry.getValue();
                    List<String> entryFieldMatchedText = validateAndGetMatchedTextForField(matchedTextMap, entryField, entryChunks.size());

                    ListIterator<String> matchedTextIt = entryFieldMatchedText.listIterator();
                    for (var chunk : entryChunks) {
                        String matchedText = matchedTextIt.next();
                        ChunkedInference.TextOffset offset = createOffset(useLegacyFormat, chunk, matchedText);
                        double[] values = parseDenseVector(
                            chunk.rawEmbeddings(),
                            field.inference().modelSettings().dimensions(),
                            field.contentType()
                        );
                        chunks.add(
                            new ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk(
                                FloatConversionUtils.floatArrayOf(values),
                                matchedText,
                                offset
                            )
                        );
                    }
                }
                return new ChunkedInferenceEmbeddingFloat(chunks);
            }
            default -> throw new AssertionError("Invalid task_type: " + field.inference().modelSettings().taskType().name());
        }
    }

    private static List<String> validateAndGetMatchedTextForField(
        Map<String, List<String>> matchedTextMap,
        String fieldName,
        int chunkCount
    ) {
        List<String> fieldMatchedText = matchedTextMap.get(fieldName);
        if (fieldMatchedText == null) {
            throw new IllegalStateException("No matched text list exists for field [" + fieldName + "]");
        } else if (fieldMatchedText.size() != chunkCount) {
            throw new IllegalStateException("Matched text list size does not equal chunk count for field [" + fieldName + "]");
        }

        return fieldMatchedText;
    }

    /**
     * Create a {@link ChunkedInference.TextOffset} instance with valid offset values. When using the legacy semantic text format, the
     * offset values are not written to {@link SemanticTextField.Chunk}, so we cannot read them from there. Instead, use the knowledge that
     * the matched text corresponds to one complete input value (i.e. one input value -> one chunk) to calculate the offset values.
     *
     * @param useLegacyFormat Whether the old format should be used
     * @param chunk           The chunk to get/calculate offset values for
     * @param matchedText     The matched text to calculate offset values for
     * @return A {@link ChunkedInference.TextOffset} instance with valid offset values
     */
    private static ChunkedInference.TextOffset createOffset(boolean useLegacyFormat, SemanticTextField.Chunk chunk, String matchedText) {
        final int startOffset = useLegacyFormat ? 0 : chunk.startOffset();
        final int endOffset = useLegacyFormat ? matchedText.length() : chunk.endOffset();

        return new ChunkedInference.TextOffset(startOffset, endOffset);
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

    private static List<WeightedToken> parseWeightedTokens(BytesReference value, XContentType contentType) {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, value, contentType)) {
            Map<String, Object> map = parser.map();
            List<WeightedToken> weightedTokens = new ArrayList<>();
            for (var entry : map.entrySet()) {
                weightedTokens.add(new WeightedToken(entry.getKey(), ((Number) entry.getValue()).floatValue()));
            }
            return weightedTokens;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
