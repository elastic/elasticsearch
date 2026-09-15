/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.EmbeddingRequest.JINA_AI_EMBEDDING_TASK_ADDED;
import static org.elasticsearch.inference.InferenceString.EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceString.URL_INPUT_FORMAT_FEATURE_FLAG;
import static org.elasticsearch.inference.InferenceString.URL_INPUT_FORMAT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.elasticsearch.inference.InferenceStringTests.randomDataTypeSupportingBase64;
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
            var expectedInputs = List.of(new InferenceStringGroup("some text input"));
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    public void testParser_withBase64ContentObject() throws IOException {
        var nonTextType = randomDataTypeSupportingBase64();
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
            var expectedInputs = List.of(new InferenceStringGroup("first text input"), new InferenceStringGroup("second text input"));
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
                        InferenceString.ofText("some text input"),
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
                new InferenceStringGroup(List.of(InferenceString.ofText("first text input"), InferenceString.ofText("second text input"))),
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
                new InferenceStringGroup(List.of(InferenceString.ofText("first text input"), InferenceString.ofText("second text input")))
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
            var expectedInputs = List.of(new InferenceStringGroup("some text input"));
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
            var expectedInputs = List.of(new InferenceStringGroup("some text input"));
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
            var expectedInputs = List.of(new InferenceStringGroup("some text input"));
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.UNSPECIFIED));
            assertThat(request.taskSettings(), anEmptyMap());
        }
    }

    /**
     * Versions before {@link InferenceString#EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED} throw an exception when serializing audio,
     * video or pdf content, and versions before {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED} throw an exception when
     * serializing URL-format inputs, so we filter those out of the bwc versions to avoid test failures.
     * The backwards-compatibility logic is tested directly by {@link #testAudioVideoPdfAreNotBackwardsCompatible} and
     * {@link #testUrlFormatIsNotBackwardsCompatible}.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream()
            .filter(version -> version.supports(EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED))
            .filter(version -> version.supports(URL_INPUT_FORMAT_SUPPORT_ADDED))
            .toList();
    }

    /**
     * Verifies that audio, video and pdf inputs cannot be sent to nodes that do not support
     * {@link InferenceString#EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED}.
     * <p>
     * We use a specific BASE64-format instance rather than a random one to avoid interference from the later
     * {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED} gate: random generation could produce URL-format instances,
     * which would fail with the URL-format error rather than the audio/video/pdf error and break the assertion.
     */
    public void testAudioVideoPdfAreNotBackwardsCompatible() throws IOException {
        var audioRequest = new EmbeddingRequest(
            List.of(new InferenceStringGroup(new InferenceString(DataType.AUDIO, DataFormat.BASE64, TEST_DATA_URI))),
            InputType.UNSPECIFIED,
            Map.of()
        );
        var preAvpVersions = super.bwcVersions().stream()
            .filter(v -> v.supports(EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED) == false)
            .toList();
        assertRequestNotBackwardsCompatible(
            audioRequest,
            preAvpVersions,
            "Cannot send an inference request with audio, video or pdf inputs to an older node. "
                + "Please wait until all nodes are upgraded before using audio, video or pdf inputs"
        );
    }

    /**
     * Verifies that URL-format inputs cannot be sent to nodes that do not support
     * {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED}.
     * <p>
     * We use an {@link DataType#IMAGE} instance since IMAGE pre-dates the audio/video/pdf gate and will not
     * trigger it, ensuring we always get the URL-specific error on any pre-URL node.
     */
    public void testUrlFormatIsNotBackwardsCompatible() throws IOException {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        var urlRequest = new EmbeddingRequest(
            List.of(new InferenceStringGroup(new InferenceString(DataType.IMAGE, DataFormat.URL, "https://example.com/image.png"))),
            InputType.UNSPECIFIED,
            Map.of()
        );
        var preUrlVersions = super.bwcVersions().stream().filter(v -> v.supports(URL_INPUT_FORMAT_SUPPORT_ADDED) == false).toList();
        assertRequestNotBackwardsCompatible(
            urlRequest,
            preUrlVersions,
            "Cannot send an inference request with URL format inputs to an older node. "
                + "Please wait until all nodes are upgraded before using URL format inputs"
        );
    }

    /**
     * Asserts that serializing {@code request} to each of the given {@code unsupportedVersions} throws an
     * {@link ElasticsearchStatusException} with {@link RestStatus#BAD_REQUEST} and the given {@code expectedMessage}.
     */
    private void assertRequestNotBackwardsCompatible(
        EmbeddingRequest request,
        List<TransportVersion> unsupportedVersions,
        String expectedMessage
    ) throws IOException {
        for (var version : unsupportedVersions) {
            var ex = assertThrows(
                ElasticsearchStatusException.class,
                () -> copyWriteable(request, getNamedWriteableRegistry(), instanceReader(), version)
            );
            assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
            assertThat(ex.getMessage(), is(expectedMessage));
        }
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
