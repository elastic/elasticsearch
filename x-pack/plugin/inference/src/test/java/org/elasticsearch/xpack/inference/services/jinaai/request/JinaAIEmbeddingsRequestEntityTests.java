/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.elasticsearch.common.Strings;
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

import static org.elasticsearch.inference.InferenceString.DataFormat.BASE64;
import static org.elasticsearch.inference.InferenceString.DataType.IMAGE;
import static org.elasticsearch.inference.InputType.INGEST;
import static org.elasticsearch.xpack.inference.TaskTypeTests.randomEmbeddingTaskType;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests.createModel;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests.getEmbeddingServiceSettings;
import static org.hamcrest.CoreMatchers.is;

public class JinaAIEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_nonMultimodal_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var modelName = "modelName";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var lateChunking = randomBoolean();
        var dimensions = randomNonNegativeInt();
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.INTERNAL_INGEST,
            createModel(
                null,
                modelName,
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

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"input":["abc"],"model":"%s","embedding_type":"%s",\
                        "task":"retrieval.passage","late_chunking":%b,"dimensions":%d}""",
                    modelName,
                    embeddingType.toRequestString(),
                    lateChunking,
                    dimensions
                )
            )
        );
    }

    public void testXContent_nonMultimodal_WritesOnlyLateChunkingField_WhenItIsTheOnlyOptionalFieldDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.INTERNAL_INGEST,
            createModel(
                null,
                "modelName",
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

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"modelName","embedding_type":"float","task":"retrieval.passage","late_chunking":false}"""));
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
                "modelName",
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

        assertThat(xContentResult, is(Strings.format("""
            {"input":["%s","%s","%s"],"model":"modelName","embedding_type":"float",\
            "task":"retrieval.passage","late_chunking":false}""", testInput.textValue(), testInput.textValue(), testInput.textValue())));
    }

    public void testXContent_nonMultimodal_WritesInputTypeField_WhenItIsDefinedOnlyInTaskSettings() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            null,
            createModel(
                null,
                "modelName",
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

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"modelName","embedding_type":"float","task":"retrieval.query"}"""));
    }

    public void testXContent_nonMultimodal_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            null,
            createModel(
                null,
                "modelName",
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

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"modelName","embedding_type":"float"}"""));
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

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"binary","task":"separation"}"""));
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

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"binary","task":"retrieval.query"}"""));
    }

    public void testXContent_nonMultimodal_doesNotWriteDimensions_whenDimensionsSetByUserIsFalse() throws IOException {
        var taskType = randomEmbeddingTaskType();
        var serviceSettings = getEmbeddingServiceSettings("modelName", null, null, 512, null, null, false, taskType, false);
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            null,
            createModel(null, serviceSettings, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, null, "apiKey", taskType)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"modelName","embedding_type":"float"}"""));
    }

    public void testXContent_multimodal_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var modelName = "modelName";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var lateChunking = randomBoolean();
        var dimensions = randomNonNegativeInt();
        String textInput = "text input";
        String imageInput = "image input";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(textInput), new InferenceStringGroup(new InferenceString(IMAGE, imageInput))),
            InputType.INTERNAL_INGEST,
            createModel(
                null,
                modelName,
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

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"input":[{"text":"%s"},{"image":"%s"}],"model":"%s",\
                        "embedding_type":"%s","task":"retrieval.passage","late_chunking":false,"dimensions":%d}""",
                    textInput,
                    imageInput,
                    modelName,
                    embeddingType.toRequestString(),
                    dimensions
                )
            )
        );
    }

    public void testXContent_multimodal_WritesTrueLateChunkingField_WhenLateChunkingSetToTrueAndInputContainsOnlyTextInput()
        throws IOException {

        String textInput1 = "text input 1";
        String textInput2 = "text input 2";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(textInput1), new InferenceStringGroup(textInput2)),
            InputType.INTERNAL_INGEST,
            createModel(null, "modelName", null, new JinaAIEmbeddingsTaskSettings(null, true), "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"input":[{"text":"%s"},{"text":"%s"}],"model":"modelName","embedding_type":"float",\
            "task":"retrieval.passage","late_chunking":true}""", textInput1, textInput2)));
    }

    public void testXContent_multimodal_WritesFalseLateChunkingField_WhenLateChunkingSetToTrueAndInputContainsNonTextInput()
        throws IOException {

        String textInput = "text input";
        String imageInput = "image input";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(textInput), new InferenceStringGroup(List.of(new InferenceString(IMAGE, BASE64, imageInput)))),
            InputType.INTERNAL_INGEST,
            createModel(null, "modelName", null, new JinaAIEmbeddingsTaskSettings(null, true), "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"input":[{"text":"%s"},{"image":"%s"}],"model":"modelName","embedding_type":"float",\
            "task":"retrieval.passage","late_chunking":false}""", textInput, imageInput)));
    }

    public void testXContent_multimodal_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        String textInput = "text input";
        String imageInput = "image input";
        String modelName = "modelName";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(textInput), new InferenceStringGroup(new InferenceString(IMAGE, imageInput))),
            null,
            createModel(null, modelName, null, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"input":[{"text":"%s"},{"image":"%s"}],"model":"%s","embedding_type":"float"}""", textInput, imageInput, modelName)));
    }

    public void testXContent_DoesNotWriteTaskField_WhenModelDoesNotSupportSpecifiedInputType() throws IOException {
        String textInput = "text input";
        String modelName = "jina-clip-v2";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(textInput)),
            INGEST,
            createModel(null, modelName, null, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"input":[{"text":"%s"}],"model":"%s","embedding_type":"float"}""", textInput, modelName)));
    }

    public void testXContent_DoesNotWriteTaskField_WhenModelDoesNotSupportSpecifiedInputType_InputTypeInTaskSettings() throws IOException {
        String textInput = "text input";
        String modelName = "jina-clip-v2";
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup(textInput)),
            null,
            createModel(null, modelName, null, new JinaAIEmbeddingsTaskSettings(INGEST), "apiKey", null, TaskType.EMBEDDING, true)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"input":[{"text":"%s"}],"model":"%s","embedding_type":"float"}""", textInput, modelName)));
    }
}
