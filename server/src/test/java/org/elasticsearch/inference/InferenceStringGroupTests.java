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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.inference.InferenceStringGroup.toInferenceStringList;
import static org.elasticsearch.inference.InferenceStringGroup.toStringList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class InferenceStringGroupTests extends AbstractBWCSerializationTestCase<InferenceStringGroup> {

    public void testStringConstructor() {
        String stringValue = "a string";
        var input = new InferenceStringGroup(stringValue);
        assertThat(input.inferenceStrings(), contains(new InferenceString(DataType.TEXT, DataFormat.TEXT, stringValue)));
    }

    public void testSingleArgumentConstructor() {
        InferenceString inferenceString = new InferenceString(DataType.IMAGE, DataFormat.BASE64, "a string");
        var input = new InferenceStringGroup(inferenceString);
        assertThat(input.inferenceStrings(), contains(inferenceString));
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
}
