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
import org.elasticsearch.inference.InferenceString.DataFormat;
import org.elasticsearch.inference.InferenceString.DataType;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, "some text input")))
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
        }
    }

    public void testParser_withSingleContentObject() throws IOException {
        var imageFormat = randomFrom(InferenceString.supportedFormatsForType(DataType.IMAGE));
        var requestJson = Strings.format("""
            {
                "input": {
                    "content": {"type": "image", "format": "%s", "value": "some image input"}
                },
                "input_type": "search"
            }
            """, imageFormat);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(DataType.IMAGE, imageFormat, "some image input")))
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
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
                new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, "first text input"))),
                new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, "second text input")))
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
        }
    }

    public void testParser_withSingleContentObjectWithMultipleEntries() throws IOException {
        var imageFormat = randomFrom(InferenceString.supportedFormatsForType(DataType.IMAGE));
        var requestJson = Strings.format("""
            {
                "input": {
                    "content": [
                        {"type": "text", "format": "text", "value": "some text input"},
                        {"type": "image", "format": "%s", "value": "some image input"}
                    ]
                },
                "input_type": "search"
            }
            """, imageFormat);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(
                    List.of(
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, "some text input"),
                        new InferenceString(DataType.IMAGE, imageFormat, "some image input")
                    )
                )
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
        }
    }

    public void testParser_withMultipleContentObjects() throws IOException {
        var imageFormat = randomFrom(InferenceString.supportedFormatsForType(DataType.IMAGE));
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "image", "format": "%s", "value": "some image input"}
                    },
                    {
                        "content": [
                            {"type": "text", "format": "text", "value": "first text input"},
                            {"type": "text", "format": "text", "value": "second text input"}
                        ]
                    }
                ],
                "input_type": "search"
            }
            """, imageFormat);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(DataType.IMAGE, imageFormat, "some image input"))),
                new InferenceStringGroup(
                    List.of(
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, "first text input"),
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, "second text input")
                    )
                )
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
        }
    }

    public void testParser_withUnspecifiedFormats_usesDefaults() throws IOException {
        var requestJson = """
            {
                "input": [
                    {
                        "content": {"type": "image", "value": "some image input"}
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
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(DataType.IMAGE, DataFormat.BASE64, "some image input"))),
                new InferenceStringGroup(
                    List.of(
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, "first text input"),
                        new InferenceString(DataType.TEXT, DataFormat.TEXT, "second text input")
                    )
                )
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
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
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, DataFormat.TEXT, "some text input")))
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.UNSPECIFIED));
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
        return new EmbeddingRequest(randomEmbeddingContents(), randomFrom(InputType.values()));
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
        if (randomBoolean()) {
            var embeddingContents = instance.inputs();
            return new EmbeddingRequest(
                randomValueOtherThan(embeddingContents, EmbeddingRequestTests::randomEmbeddingContents),
                instance.inputType()
            );
        } else {
            InputType inputType = instance.inputType();
            return new EmbeddingRequest(instance.inputs(), randomValueOtherThan(inputType, () -> randomFrom(InputType.values())));
        }
    }

    @Override
    protected EmbeddingRequest mutateInstanceForVersion(EmbeddingRequest instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected EmbeddingRequest doParseInstance(XContentParser parser) throws IOException {
        return EmbeddingRequest.PARSER.parse(parser, null);
    }
}
