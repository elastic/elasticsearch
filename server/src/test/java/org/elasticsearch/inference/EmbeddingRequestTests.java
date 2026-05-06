/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.EmbeddingRequest.JINA_AI_EMBEDDING_TASK_ADDED;
import static org.elasticsearch.inference.InferenceString.EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;

public class EmbeddingRequestTests extends AbstractBWCSerializationTestCase<EmbeddingRequest> {

    public void testParser_withSingleString() throws IOException {
        var requestJson = """
            {
                "input": "some text input",
                "input_type": "search"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, "some text input"))));
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_withBase64ContentObject() throws IOException {
        var nonTextType = randomFrom(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        var format = DataFormat.BASE64;
        var requestJson = Strings.format("""
            {
                "input": {
                    "content": {"type": "%s", "format": "%s", "value": "%s"}
                },
                "input_type": "search"
            }
            """, nonTextType, format, InferenceStringTests.TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(nonTextType, format, InferenceStringTests.TEST_DATA_URI)))
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_withStringArray() throws IOException {
        var requestJson = """
            {
                "input": ["first text input", "second text input"],
                "input_type": "search"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, "first text input"))),
                new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, "second text input")))
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_withSingleContentObjectWithMultipleEntries() throws IOException {
        var imageFormat = DataFormat.BASE64;
        var requestJson = Strings.format("""
            {
                "input": {
                    "content": [
                        {"type": "text", "format": "text", "value": "some text input"},
                        {"type": "image", "format": "%s", "value": "%s"}
                    ]
                },
                "input_type": "search"
            }
            """, imageFormat, InferenceStringTests.TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(
                    List.of(
                        new InferenceString(DataType.TEXT, "some text input"),
                        new InferenceString(DataType.IMAGE, imageFormat, InferenceStringTests.TEST_DATA_URI)
                    )
                )
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_withMultipleContentObjects() throws IOException {
        var imageFormat = DataFormat.BASE64;
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "image", "format": "%s", "value": "%s"}
                    },
                    {
                        "content": [
                            {"type": "text", "format": "text", "value": "first text input"},
                            {"type": "text", "format": "text", "value": "second text input"}
                        ]
                    },
                    "third input"
                ],
                "input_type": "search"
            }
            """, imageFormat, InferenceStringTests.TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(DataType.IMAGE, imageFormat, InferenceStringTests.TEST_DATA_URI))),
                new InferenceStringGroup(
                    List.of(new InferenceString(DataType.TEXT, "first text input"), new InferenceString(DataType.TEXT, "second text input"))
                ),
                new InferenceStringGroup("third input")
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_withUnspecifiedFormats_usesDefaults() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "image", "value": "%s"}
                    },
                    {
                        "content": [
                            {"type": "text", "value": "first text input"},
                            {"type": "text", "value": "second text input"}
                        ]
                    }
                ],
                "input_type": "search"
            }
            """, InferenceStringTests.TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(
                    List.of(new InferenceString(DataType.IMAGE, DataFormat.BASE64, InferenceStringTests.TEST_DATA_URI))
                ),
                new InferenceStringGroup(
                    List.of(new InferenceString(DataType.TEXT, "first text input"), new InferenceString(DataType.TEXT, "second text input"))
                )
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_withNoInputType() throws IOException {
        var requestJson = """
            {
                "input": "some text input"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, "some text input"))));
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.UNSPECIFIED));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_withTaskSettings() throws IOException {
        var requestJson = """
            {
                "input": "some text input",
                "task_settings": {
                  "field_one": "value_one",
                  "field_two": 123
                }
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, "some text input"))));
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.UNSPECIFIED));
            assertThat(request.taskSettings(), is(Map.of("field_one", "value_one", "field_two", 123)));
        }
    }

    public void testParser_withEmptyTaskSettings() throws IOException {
        var requestJson = """
            {
                "input": "some text input",
                "task_settings": {}
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, "some text input"))));
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.UNSPECIFIED));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    /**
     * Versions before {@link InferenceString#EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED} throw an exception when serializing audio,
     * video or pdf content, so we filter those out of the bwc versions to avoid test failures.
     * The logic is tested directly by {@link #testAudioVideoPdfAreNotBackwardsCompatible}
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED)).toList();
    }

    public void testAudioVideoPdfAreNotBackwardsCompatible() throws IOException {
        testSerializationIsNotBackwardsCompatible(
            EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED,
            i -> i.inputs().stream().anyMatch(input -> input.inferenceStrings().stream().anyMatch(InferenceStringTests::isAudioVideoOrPdf)),
            """
                Cannot send an inference request with audio, video or pdf inputs to an older node. \
                Please wait until all nodes are upgraded before using audio, video or pdf inputs"""
        );
    }

    @Override
    protected Writeable.Reader<EmbeddingRequest> instanceReader() {
        return EmbeddingRequest::new;
    }

    @Override
    protected EmbeddingRequest createTestInstance() {
        return createRandom();
    }

    public static EmbeddingRequest createRandom() {
        return new EmbeddingRequest(
            randomEmbeddingContents(),
            randomFrom(InputType.values()),
            Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8))
        );
    }

    private static List<InferenceStringGroup> randomEmbeddingContents() {
        var contents = new ArrayList<InferenceStringGroup>();
        for (int i = 0; i < randomInt(5); ++i) {
            contents.add(InferenceStringGroupTests.createRandom());
        }
        return contents;
    }

    @Override
    protected EmbeddingRequest mutateInstance(EmbeddingRequest instance) throws IOException {
        var embeddingContents = instance.inputs();
        var inputType = instance.inputType();
        var taskSettings = instance.taskSettings();
        switch (randomInt(2)) {
            case 0 -> embeddingContents = randomValueOtherThan(embeddingContents, EmbeddingRequestTests::randomEmbeddingContents);
            case 1 -> inputType = randomValueOtherThan(inputType, () -> randomFrom(InputType.values()));
            case 2 -> taskSettings = randomValueOtherThan(
                taskSettings,
                () -> Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8))
            );
        }
        return new EmbeddingRequest(embeddingContents, inputType, taskSettings);
    }

    @Override
    protected EmbeddingRequest mutateInstanceForVersion(EmbeddingRequest instance, TransportVersion version) {
        if (version.supports(JINA_AI_EMBEDDING_TASK_ADDED)) {
            return instance;
        } else {
            return new EmbeddingRequest(instance.inputs(), instance.inputType(), Map.of());
        }
    }

    @Override
    protected EmbeddingRequest doParseInstance(XContentParser parser) throws IOException {
        return EmbeddingRequest.PARSER.parse(parser, null);
    }
}
