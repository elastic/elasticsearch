/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.elasticsearch.inference.DataFormat.BASE64;
import static org.elasticsearch.inference.DataType.AUDIO;
import static org.elasticsearch.inference.DataType.IMAGE;
import static org.elasticsearch.inference.DataType.PDF;
import static org.elasticsearch.inference.DataType.TEXT;
import static org.elasticsearch.inference.DataType.VIDEO;
import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.elasticsearch.inference.InferenceStringTests.randomDataURI;
import static org.elasticsearch.inference.InputType.INGEST;
import static org.elasticsearch.xpack.inference.TaskTypeTests.randomEmbeddingTaskType;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests.createModel;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests.getEmbeddingServiceSettings;
import static org.hamcrest.CoreMatchers.is;

public class JinaAIEmbeddingsRequestEntityTests extends ESTestCase {

    public static final String TEXT_INPUT = "text input";
    public static final String MODEL_NAME = "modelName";

    public void testXContent_nonMultimodal_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var lateChunking = randomBoolean();
        var dimensions = randomNonNegativeInt();
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.INTERNAL_INGEST,
            createModel(
                null,
                MODEL_NAME,
                embeddingType,
                new JinaAIEmbeddingsTaskSettings(INGEST, lateChunking),
                "apiKey",
                dimensions,
                randomEmbeddingTaskType(),
                false
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input":["abc"],
              "model":"%s",
              "embedding_type":"%s",
              "task":"retrieval.passage",
              "late_chunking":%b,
              "dimensions":%d
            }""", MODEL_NAME, embeddingType.toRequestString(), lateChunking, dimensions))));
    }

    public void testXContent_nonMultimodal_WritesOnlyLateChunkingField_WhenItIsTheOnlyOptionalFieldDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.INTERNAL_INGEST,
            createModel(
                null,
                MODEL_NAME,
                null,
                new JinaAIEmbeddingsTaskSettings(null, false),
                "apiKey",
                null,
                randomEmbeddingTaskType(),
                false
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace("""
            {
              "input":["abc"],
              "model":"modelName",
              "embedding_type":"float",
              "task":"retrieval.passage",
              "late_chunking":false
            }""")));
    }

    public void testXContent_nonMultimodal_WritesFalseLateChunkingField_WhenLateChunkingSetToTrueButInputExceedsWordCountLimit()
        throws IOException {
        int wordCount = JinaAIEmbeddingsRequestEntity.MAX_WORD_COUNT_FOR_LATE_CHUNKING + 1;
        var testInput = new InferenceStringGroup(
            IntStream.range(0, wordCount / 2).mapToObj(i -> "word" + i).collect(Collectors.joining(" ")) + "."
        );
        var testInputs = List.of(testInput, testInput, testInput);

        var entity = new JinaAIEmbeddingsRequestEntity(
            testInputs,
            InputType.INTERNAL_INGEST,
            createModel(
                null,
                MODEL_NAME,
                null,
                new JinaAIEmbeddingsTaskSettings(null, true),
                "apiKey",
                null,
                randomEmbeddingTaskType(),
                false
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input":["%s","%s","%s"],
              "model":"modelName",
              "embedding_type":"float",
              "task":"retrieval.passage",
              "late_chunking":false
            }""", testInput.textValue(), testInput.textValue(), testInput.textValue()))));
    }

    public void testXContent_nonMultimodal_WritesInputTypeField_WhenItIsDefinedOnlyInTaskSettings() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            null,
            createModel(
                null,
                MODEL_NAME,
                null,
                new JinaAIEmbeddingsTaskSettings(InputType.SEARCH, null),
                "apiKey",
                null,
                randomEmbeddingTaskType(),
                false
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace("""
            {
              "input":["abc"],
              "model":"modelName",
              "embedding_type":"float",
              "task":"retrieval.query"
            }""")));
    }

    public void testXContent_nonMultimodal_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            null,
            createModel(
                null,
                MODEL_NAME,
                null,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                "apiKey",
                null,
                randomEmbeddingTaskType(),
                false
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace("""
            {
              "input":["abc"],
              "model":"modelName",
              "embedding_type":"float"
            }""")));
    }

    public void testXContent_nonMultimodal_EmbeddingTypesBit() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.CLUSTERING,
            createModel(
                null,
                "model",
                JinaAIEmbeddingType.BIT,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                "apiKey",
                null,
                randomEmbeddingTaskType(),
                false
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace("""
            {
              "input":["abc"],
              "model":"model",
              "embedding_type":"binary",
              "task":"separation"
            }""")));
    }

    public void testXContent_nonMultimodal_EmbeddingTypesBinary() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.SEARCH,
            createModel(
                null,
                "model",
                JinaAIEmbeddingType.BINARY,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                "apiKey",
                null,
                randomEmbeddingTaskType(),
                false
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace("""
            {
              "input":["abc"],
              "model":"model",
              "embedding_type":"binary",
              "task":"retrieval.query"
            }""")));
    }

    public void testXContent_nonMultimodal_doesNotWriteDimensions_whenDimensionsSetByUserIsFalse() throws IOException {
        var taskType = randomEmbeddingTaskType();
        var serviceSettings = getEmbeddingServiceSettings(MODEL_NAME, null, null, 512, null, null, false, taskType, false);
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            null,
            createModel(null, serviceSettings, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, null, "apiKey", taskType)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace("""
            {
              "input":["abc"],
              "model":"modelName",
              "embedding_type":"float"
            }""")));
    }

    public void testXContent_Multimodal_WritesText() throws IOException {
        testXContent_Multimodal(TEXT, TEXT_INPUT, "text");
    }

    public void testXContent_Multimodal_WritesImage() throws IOException {
        testXContent_Multimodal(IMAGE, TEST_DATA_URI, "image");
    }

    public void testXContent_Multimodal_WritesAudio() throws IOException {
        testXContent_Multimodal(AUDIO, TEST_DATA_URI, "audio");
    }

    public void testXContent_Multimodal_WritesVideo() throws IOException {
        testXContent_Multimodal(VIDEO, TEST_DATA_URI, "video");
    }

    private static void testXContent_Multimodal(DataType dataType, String value, String fieldName) throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(new InferenceString(dataType, value))),
            null,
            createModel(null, MODEL_NAME, null, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": [
                {"%s":"%s"}
              ],
              "model":"%s",
              "embedding_type":"float"
            }""", fieldName, value, MODEL_NAME))));
    }

    public void testXContent_Multimodal_WritesPdf() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(new InferenceString(PDF, TEST_DATA_URI))),
            null,
            createModel(null, MODEL_NAME, null, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": {
                "pdf":"%s"
              },
              "model":"%s",
              "embedding_type":"float"
            }""", TEST_DATA_URI, MODEL_NAME))));
    }

    public void testXContent_Multimodal_GroupedInputs() throws IOException {
        var firstImageInput = randomDataURI();
        var secondImageInput = randomDataURI();
        var firstAudioInput = randomDataURI();
        var secondAudioInput = randomDataURI();
        var videoInput = randomDataURI();
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(
                new InferenceStringGroup(List.of(new InferenceString(TEXT, TEXT_INPUT), new InferenceString(IMAGE, firstImageInput))),
                new InferenceStringGroup(
                    List.of(
                        new InferenceString(AUDIO, firstAudioInput),
                        new InferenceString(VIDEO, videoInput),
                        new InferenceString(IMAGE, secondImageInput)
                    )
                ),
                new InferenceStringGroup(new InferenceString(AUDIO, secondAudioInput))
            ),
            null,
            createModel(null, MODEL_NAME, null, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": [
                {
                  "content": [
                    {"text":"%s"},
                    {"image":"%s"}
                  ]
                },
                {
                  "content": [
                    {"audio":"%s"},
                    {"video":"%s"},
                    {"image":"%s"}
                  ]
                },
                {
                  "audio": "%s"
                }
              ],
              "model":"%s",
              "embedding_type":"float"
            }""", TEXT_INPUT, firstImageInput, firstAudioInput, videoInput, secondImageInput, secondAudioInput, MODEL_NAME))));
    }

    public void testXContent_multimodal_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var lateChunking = randomBoolean();
        var dimensions = randomNonNegativeInt();
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(TEXT_INPUT), new InferenceStringGroup(new InferenceString(IMAGE, TEST_DATA_URI))),
            InputType.INTERNAL_INGEST,
            createModel(
                null,
                MODEL_NAME,
                embeddingType,
                new JinaAIEmbeddingsTaskSettings(INGEST, lateChunking),
                "apiKey",
                dimensions,
                TaskType.EMBEDDING,
                true
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": [
                {"text":"%s"},
                {"image":"%s"}
              ],
              "model":"%s",
              "embedding_type":"%s",
              "task":"retrieval.passage",
              "late_chunking":false,
              "dimensions":%d
            }""", TEXT_INPUT, TEST_DATA_URI, MODEL_NAME, embeddingType.toRequestString(), dimensions))));
    }

    public void testXContent_multimodal_WritesTrueLateChunkingField_WhenLateChunkingSetToTrueAndInputContainsOnlyTextInput()
        throws IOException {

        String textInput1 = "text input 1";
        String textInput2 = "text input 2";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(textInput1), new InferenceStringGroup(textInput2)),
            InputType.INTERNAL_INGEST,
            createModel(null, MODEL_NAME, null, new JinaAIEmbeddingsTaskSettings(null, true), "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": [
                {"text":"%s"},
                {"text":"%s"}
              ],
              "model":"modelName",
              "embedding_type":"float",
              "task":"retrieval.passage",
              "late_chunking":true
            }""", textInput1, textInput2))));
    }

    public void testXContent_multimodal_WritesFalseLateChunkingField_WhenLateChunkingSetToTrueAndInputContainsNonTextInput()
        throws IOException {

        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(
                new InferenceStringGroup(TEXT_INPUT),
                new InferenceStringGroup(List.of(new InferenceString(IMAGE, BASE64, TEST_DATA_URI)))
            ),
            InputType.INTERNAL_INGEST,
            createModel(null, MODEL_NAME, null, new JinaAIEmbeddingsTaskSettings(null, true), "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": [
                {"text":"%s"},
                {"image":"%s"}
              ],
              "model":"modelName",
              "embedding_type":"float",
              "task":"retrieval.passage",
              "late_chunking":false
            }""", TEXT_INPUT, TEST_DATA_URI))));
    }

    public void testXContent_multimodal_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(TEXT_INPUT), new InferenceStringGroup(new InferenceString(IMAGE, TEST_DATA_URI))),
            null,
            createModel(null, MODEL_NAME, null, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": [
                {"text":"%s"},
                {"image":"%s"}
              ],
              "model":"%s",
              "embedding_type":"float"
            }""", TEXT_INPUT, TEST_DATA_URI, MODEL_NAME))));
    }

    public void testXContent_DoesNotWriteTaskField_WhenModelDoesNotSupportSpecifiedInputType() throws IOException {
        String modelName = "jina-clip-v2";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(TEXT_INPUT)),
            INGEST,
            createModel(null, modelName, null, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": [
                {"text":"%s"}
              ],
              "model":"%s",
              "embedding_type":"float"
            }""", TEXT_INPUT, modelName))));
    }

    public void testXContent_DoesNotWriteTaskField_WhenModelDoesNotSupportSpecifiedInputType_InputTypeInTaskSettings() throws IOException {
        String modelName = "jina-clip-v2";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(TEXT_INPUT)),
            null,
            createModel(null, modelName, null, new JinaAIEmbeddingsTaskSettings(INGEST), "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
              "input": [
                {"text":"%s"}
              ],
              "model":"%s",
              "embedding_type":"float"
            }""", TEXT_INPUT, modelName))));
    }
}
