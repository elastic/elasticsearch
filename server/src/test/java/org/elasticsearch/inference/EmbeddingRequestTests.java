/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class EmbeddingRequestTests extends AbstractWireSerializingTestCase<EmbeddingRequest> {

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
                new InferenceStringGroup(List.of(new InferenceString(InferenceString.DataType.TEXT, "some text input")))
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
        }
    }

    public void testParser_withSingleContentObject() throws IOException {
        var requestJson = """
            {
                "input": {
                    "content": {"type": "image_base64", "value": "some image input"}
                },
                "input_type": "search"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(List.of(new InferenceString(InferenceString.DataType.IMAGE_BASE64, "some image input")))
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
                new InferenceStringGroup(List.of(new InferenceString(InferenceString.DataType.TEXT, "first text input"))),
                new InferenceStringGroup(List.of(new InferenceString(InferenceString.DataType.TEXT, "second text input")))
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
        }
    }

    public void testParser_withSingleContentObjectWithMultipleEntries() throws IOException {
        var requestJson = """
            {
                "input": {
                    "content": [
                        {"type": "text", "value": "some text input"},
                        {"type": "image_base64", "value": "some image input"}
                    ]
                },
                "input_type": "search"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = EmbeddingRequest.PARSER.apply(parser, null);
            var expectedInputs = List.of(
                new InferenceStringGroup(
                    List.of(
                        new InferenceString(InferenceString.DataType.TEXT, "some text input"),
                        new InferenceString(InferenceString.DataType.IMAGE_BASE64, "some image input")
                    )
                )
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
        }
    }

    public void testParser_withMultipleContentObjects() throws IOException {
        var requestJson = """
            {
                "input": [
                    {
                        "content": {"type": "image_base64", "value": "some image input"}
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
                new InferenceStringGroup(List.of(new InferenceString(InferenceString.DataType.IMAGE_BASE64, "some image input"))),
                new InferenceStringGroup(
                    List.of(
                        new InferenceString(InferenceString.DataType.TEXT, "first text input"),
                        new InferenceString(InferenceString.DataType.TEXT, "second text input")
                    )
                )
            );
            assertThat(request.inputs(), is(expectedInputs));
            assertThat(request.inputType(), is(InputType.SEARCH));
        }
    }

    @Override
    protected Writeable.Reader<EmbeddingRequest> instanceReader() {
        return EmbeddingRequest::new;
    }

    @Override
    protected EmbeddingRequest createTestInstance() {
        int contentsToCreate = randomInt(5);
        return new EmbeddingRequest(randomEmbeddingContents(contentsToCreate), randomFrom(InputType.values()));
    }

    private static InferenceStringGroup randomEmbeddingContent() {
        var inferenceStrings = new ArrayList<InferenceString>();
        int inferenceStringsToCreate = randomInt(5);
        for (int j = 0; j < inferenceStringsToCreate; ++j) {
            inferenceStrings.add(InferenceStringTests.createRandom());
        }
        return new InferenceStringGroup(inferenceStrings);
    }

    private static List<InferenceStringGroup> randomEmbeddingContents(int contentsToCreate) {
        var contents = new ArrayList<InferenceStringGroup>();
        for (int i = 0; i < contentsToCreate; ++i) {
            contents.add(randomEmbeddingContent());
        }
        return contents;
    }

    @Override
    protected EmbeddingRequest mutateInstance(EmbeddingRequest instance) throws IOException {
        if (randomBoolean()) {
            var embeddingContents = instance.inputs();
            return new EmbeddingRequest(
                randomValueOtherThan(embeddingContents, () -> randomEmbeddingContents(randomInt(5))),
                instance.inputType()
            );
        } else {
            InputType inputType = instance.inputType();
            return new EmbeddingRequest(instance.inputs(), randomValueOtherThan(inputType, () -> randomFrom(InputType.values())));
        }
    }
}
