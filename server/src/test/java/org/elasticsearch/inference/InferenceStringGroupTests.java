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
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.DataType.TEXT;
import static org.elasticsearch.inference.InferenceString.EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceStringGroup.CONTENT_FIELD;
import static org.elasticsearch.inference.InferenceStringGroup.containsNonTextEntry;
import static org.elasticsearch.inference.InferenceStringGroup.indexContainingMultipleInferenceStrings;
import static org.elasticsearch.inference.InferenceStringGroup.toInferenceStringList;
import static org.elasticsearch.inference.InferenceStringGroup.toStringList;
import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class InferenceStringGroupTests extends AbstractBWCSerializationTestCase<InferenceStringGroup> {

    public void testStringConstructor() {
        String stringValue = "a string";
        var input = new InferenceStringGroup(stringValue);
        assertThat(input.inferenceStrings(), contains(new InferenceString(TEXT, DataFormat.TEXT, stringValue)));
        assertThat(input.containsNonTextEntry(), is(false));
        assertThat(input.containsMultipleInferenceStrings(), is(false));
    }

    public void testSingleInferenceStringConstructor() {
        InferenceString inferenceString = new InferenceString(DataType.IMAGE, DataFormat.BASE64, TEST_DATA_URI);
        var input = new InferenceStringGroup(inferenceString);
        assertThat(input.inferenceStrings(), contains(inferenceString));
        assertThat(input.containsNonTextEntry(), is(true));
        assertThat(input.containsMultipleInferenceStrings(), is(false));
    }

    public void testInferenceStringListConstructor() {
        InferenceString inferenceString1 = new InferenceString(DataType.IMAGE, DataFormat.BASE64, TEST_DATA_URI);
        InferenceString inferenceString2 = new InferenceString(TEXT, "a string");
        var input = new InferenceStringGroup(List.of(inferenceString1, inferenceString2));
        assertThat(input.inferenceStrings(), contains(inferenceString1, inferenceString2));
        assertThat(input.containsNonTextEntry(), is(true));
        assertThat(input.containsMultipleInferenceStrings(), is(true));
    }

    public void testInferenceStringListConstructor_withNullList_throws() {
        assertThrows(NullPointerException.class, () -> new InferenceStringGroup((List<InferenceString>) null));
    }

    public void testInferenceStringListConstructor_withEmptyList_throws() {
        var exception = assertThrows(IllegalArgumentException.class, () -> new InferenceStringGroup(List.of()));
        assertThat(exception.getMessage(), is("InferenceStringGroup constructor argument cannot be an empty list"));
    }

    public void testParser_withEmptyContentObject_throws() throws IOException {
        var requestJson = """
                {
                    "content": {}
                }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(XContentParseException.class, () -> InferenceStringGroup.parse(parser));
            assertThat(exception.getMessage(), containsString("[InferenceStringGroup] failed to parse field [content]"));
            assertThat(exception.getCause().getMessage(), containsString("Required [type, value]"));
        }
    }

    public void testParser_withEmptyContentObjectArray_throws() throws IOException {
        var requestJson = """
                {
                    "content": []
                }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(XContentParseException.class, () -> InferenceStringGroup.parse(parser));
            assertThat(exception.getMessage(), containsString("[InferenceStringGroup] failed to parse field [content]"));
            assertThat(
                exception.getCause().getMessage(),
                containsString("failed to build [InferenceStringGroup] after last required field arrived")
            );
            assertThat(exception.getCause().getCause().getMessage(), containsString("[content] field cannot be an empty array"));
        }
    }

    public void testParser_withNullContent_throws() throws IOException {
        var requestJson = """
                {
                    "content": null
                }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(XContentParseException.class, () -> InferenceStringGroup.parse(parser));
            assertThat(exception.getMessage(), containsString("[InferenceStringGroup] content doesn't support values of type: VALUE_NULL"));
        }
    }

    public void testValue_withMoreThanOneElement_throws() {
        var input = new InferenceStringGroup(List.of(InferenceStringTests.createRandom(), InferenceStringTests.createRandom()));
        var expectedException = expectThrows(AssertionError.class, input::value);
        assertThat(expectedException.getMessage(), is("Multiple-input InferenceStringGroup used in code path expecting a single input."));
    }

    public void testTextValue_withMoreThanOneElement_throws() {
        var input = new InferenceStringGroup(List.of(InferenceStringTests.createRandom(), InferenceStringTests.createRandom()));
        var expectedException = expectThrows(AssertionError.class, input::textValue);
        assertThat(expectedException.getMessage(), is("Multiple-input InferenceStringGroup used in code path expecting a single input."));
    }

    public void testToInferenceStringList() {
        var inferenceString1 = InferenceStringTests.createRandom();
        var inferenceString2 = InferenceStringTests.createRandom();
        var inputs = List.of(new InferenceStringGroup(inferenceString1), new InferenceStringGroup(inferenceString2));
        assertThat(toInferenceStringList(inputs), contains(inferenceString1, inferenceString2));
    }

    public void testToInferenceStringList_withMoreThanOneElement_throws() {
        var input = List.of(new InferenceStringGroup(List.of(InferenceStringTests.createRandom(), InferenceStringTests.createRandom())));
        var expectedException = expectThrows(AssertionError.class, () -> toInferenceStringList(input));
        assertThat(expectedException.getMessage(), is("Multiple-input InferenceStringGroup used in code path expecting a single input."));
    }

    public void testToStringList() {
        String string1 = "string1";
        String string2 = "string2";
        var inputs = List.of(new InferenceStringGroup(string1), new InferenceStringGroup(string2));
        assertThat(toStringList(inputs), contains(string1, string2));
    }

    public void testToStringList_withMoreThanOneElement_throws() {
        var input = List.of(new InferenceStringGroup(List.of(InferenceStringTests.createRandom(), InferenceStringTests.createRandom())));
        var expectedException = expectThrows(AssertionError.class, () -> toStringList(input));
        assertThat(expectedException.getMessage(), is("Multiple-input InferenceStringGroup used in code path expecting a single input."));
    }

    public void testToStringList_WithNonTextValue_Throws() {
        var input = randomList(1, 5, () -> new InferenceStringGroup(randomAlphaOfLength(5)));
        var nonTextInput = new InferenceStringGroup(
            InferenceStringTests.createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(TEXT)))
        );
        input.add(randomInt(input.size()), nonTextInput);
        var expectedException = expectThrows(AssertionError.class, () -> toStringList(input));
        assertThat(expectedException.getMessage(), is("Non-text input returned from InferenceString.textValue"));
    }

    public void testContainsNonTextEntry_withOnlyTextInputs() {
        var inputs = List.of(new InferenceStringGroup("string1"), new InferenceStringGroup("string2"));
        assertThat(containsNonTextEntry(inputs), is(false));
    }

    public void testContainsNonTextEntry_withNonTextInput() {
        DataType nonTextDataType = randomValueOtherThan(TEXT, () -> randomFrom(DataType.values()));
        var inputs = List.of(
            new InferenceStringGroup("string1"),
            new InferenceStringGroup(
                new InferenceString(
                    nonTextDataType,
                    InferenceStringTests.convertToDataURIIfNeeded(nonTextDataType, null, randomAlphanumericOfLength(10))
                )
            )
        );
        assertThat(containsNonTextEntry(inputs), is(true));
    }

    public void testIndexContainingMultipleInferenceStrings_withSingleInferenceString() {
        var inputs = getInputsList();
        assertThat(indexContainingMultipleInferenceStrings(inputs), nullValue());
    }

    public void testIndexContainingMultipleInferenceStrings_withMultipleInferenceStrings() {
        var inputs = getInputsList();

        // Add an InferenceStringGroup with multiple InferenceStrings at a random point in the input list
        var indexToAdd = randomIntBetween(0, inputs.size() - 1);
        var multipleInferenceStrings = new InferenceStringGroup(
            List.of(new InferenceString(TEXT, "a_string"), new InferenceString(TEXT, "a_string"))
        );
        inputs.add(indexToAdd, multipleInferenceStrings);
        assertThat(indexContainingMultipleInferenceStrings(inputs), is(indexToAdd));
    }

    private static ArrayList<InferenceStringGroup> getInputsList() {
        var listSize = randomIntBetween(1, 10);
        var inputs = new ArrayList<InferenceStringGroup>(listSize);
        for (int i = 0; i < listSize; ++i) {
            inputs.add(new InferenceStringGroup("a_string"));
        }
        return inputs;
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
            i -> i.inferenceStrings().stream().anyMatch(InferenceStringTests::isAudioVideoOrPdf),
            """
                Cannot send an inference request with audio, video or pdf inputs to an older node. \
                Please wait until all nodes are upgraded before using audio, video or pdf inputs"""
        );
    }

    @Override
    protected InferenceStringGroup mutateInstanceForVersion(InferenceStringGroup instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected InferenceStringGroup doParseInstance(XContentParser parser) throws IOException {
        return InferenceStringGroup.parse(parser);
    }

    @Override
    protected Writeable.Reader<InferenceStringGroup> instanceReader() {
        return InferenceStringGroup::new;
    }

    @Override
    protected InferenceStringGroup createTestInstance() {
        return createRandom();
    }

    public static InferenceStringGroup createRandom() {
        int inferenceStringsToCreate = randomIntBetween(1, 5);
        var inferenceStrings = new ArrayList<InferenceString>(inferenceStringsToCreate);
        for (int j = 0; j < inferenceStringsToCreate; ++j) {
            inferenceStrings.add(InferenceStringTests.createRandom());
        }
        return new InferenceStringGroup(inferenceStrings);
    }

    @Override
    protected InferenceStringGroup mutateInstance(InferenceStringGroup instance) throws IOException {
        var inferenceStrings = instance.inferenceStrings();
        List<InferenceString> newInferenceStrings = new ArrayList<>(inferenceStrings);
        var maintainListSize = randomBoolean();
        if (maintainListSize) {
            var firstElement = newInferenceStrings.getFirst();
            newInferenceStrings.set(0, randomValueOtherThan(firstElement, InferenceStringTests::createRandom));
        } else {
            // Don't remove from the list if there is only one element
            if (inferenceStrings.size() == 1 || randomBoolean()) {
                newInferenceStrings.add(InferenceStringTests.createRandom());
            } else {
                newInferenceStrings.removeLast();
            }
        }
        return new InferenceStringGroup(newInferenceStrings);
    }

    /**
     * Converts the given {@link InferenceStringGroup} to a map matching what is sent in the request body. Equivalent to converting the
     * input to XContent, then parsing the XContent to a map.
     */
    public static Map<String, Object> toRequestMap(InferenceStringGroup input) {
        List<Map<String, Object>> inferenceStrings = input.inferenceStrings().stream().map(InferenceStringTests::toRequestMap).toList();
        return Map.of(CONTENT_FIELD, inferenceStrings);
    }
}
