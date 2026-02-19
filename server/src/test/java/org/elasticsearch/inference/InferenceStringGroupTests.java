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
import org.elasticsearch.inference.InferenceString.DataFormat;
import org.elasticsearch.inference.InferenceString.DataType;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceString.DataType.TEXT;
import static org.elasticsearch.inference.InferenceStringGroup.CONTENT_FIELD;
import static org.elasticsearch.inference.InferenceStringGroup.containsNonTextEntry;
import static org.elasticsearch.inference.InferenceStringGroup.indexContainingMultipleInferenceStrings;
import static org.elasticsearch.inference.InferenceStringGroup.toInferenceStringList;
import static org.elasticsearch.inference.InferenceStringGroup.toStringList;
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
        InferenceString inferenceString = new InferenceString(DataType.IMAGE, DataFormat.BASE64, "a string");
        var input = new InferenceStringGroup(inferenceString);
        assertThat(input.inferenceStrings(), contains(inferenceString));
        assertThat(input.containsNonTextEntry(), is(true));
        assertThat(input.containsMultipleInferenceStrings(), is(false));
    }

    public void testInferenceStringListConstructor() {
        InferenceString inferenceString1 = new InferenceString(DataType.IMAGE, DataFormat.BASE64, "a string");
        InferenceString inferenceString2 = new InferenceString(TEXT, DataFormat.TEXT, "a string");
        var input = new InferenceStringGroup(List.of(inferenceString1, inferenceString2));
        assertThat(input.inferenceStrings(), contains(inferenceString1, inferenceString2));
        assertThat(input.containsNonTextEntry(), is(true));
        assertThat(input.containsMultipleInferenceStrings(), is(true));
    }

    public void testParser_withEmptyContentObject_throws() throws IOException {
        var requestJson = """
                {
                    "content": {}
                }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            // Need to call nextToken() so that the parser is at the correct element
            parser.nextToken();
            var exception = expectThrows(XContentParseException.class, () -> InferenceStringGroup.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), containsString("[InferenceStringGroup] failed to parse field [content]"));
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
        assertThat(expectedException.getMessage(), is("Multiple-input InferenceStringGroup passed to InferenceStringGroup.toStringList"));
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
        assertThat(expectedException.getMessage(), is("Multiple-input InferenceStringGroup passed to InferenceStringGroup.toStringList"));
    }

    public void testContainsNonTextEntry_withOnlyTextInputs() {
        var inputs = List.of(new InferenceStringGroup("string1"), new InferenceStringGroup("string2"));
        assertThat(containsNonTextEntry(inputs), is(false));
    }

    public void testContainsNonTextEntry_withNonTextInput() {
        DataType nonTextDataType = randomValueOtherThan(TEXT, () -> randomFrom(DataType.values()));
        var inputs = List.of(
            new InferenceStringGroup("string1"),
            new InferenceStringGroup(new InferenceString(nonTextDataType, "non text"))
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

    @Override
    protected InferenceStringGroup mutateInstanceForVersion(InferenceStringGroup instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected InferenceStringGroup doParseInstance(XContentParser parser) throws IOException {
        return InferenceStringGroup.PARSER.parse(parser, null);
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
        var inferenceStrings = new ArrayList<InferenceString>();
        int inferenceStringsToCreate = randomInt(5);
        for (int j = 0; j < inferenceStringsToCreate; ++j) {
            inferenceStrings.add(InferenceStringTests.createRandom());
        }
        return new InferenceStringGroup(inferenceStrings);
    }

    @Override
    protected InferenceStringGroup mutateInstance(InferenceStringGroup instance) throws IOException {
        var inferenceStrings = instance.inferenceStrings();
        List<InferenceString> newInferenceStrings = new ArrayList<>(inferenceStrings);
        if (inferenceStrings.isEmpty() || randomBoolean()) {
            newInferenceStrings.add(InferenceStringTests.createRandom());
        } else {
            newInferenceStrings.removeLast();
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
