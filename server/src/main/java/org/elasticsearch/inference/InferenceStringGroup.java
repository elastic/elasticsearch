/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceString.DataType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This class represents a group of one or more {@link InferenceString} which will produce a single embedding when passed to an embedding
 * provider. Conceptually, this object is equivalent to a "content" object in the embedding request, e.g.
 * <pre>
 * "input": {
 *   "content": [
 *     {"type": "text", "format": "text", "value": "text input"},
 *     {"type": "image", "format": "base64", "value": "data:image/png;base64,..."}
 *   ]
 * }
 * </pre>
 */
public final class InferenceStringGroup implements Writeable, ToXContentObject {
    public static final String CONTENT_FIELD = "content";

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<InferenceStringGroup, Void> PARSER = new ConstructingObjectParser<>(
        InferenceStringGroup.class.getSimpleName(),
        args -> new InferenceStringGroup((List<InferenceString>) args[0])
    );

    static {
        PARSER.declareObjectArray(constructorArg(), InferenceString.PARSER::apply, new ParseField(CONTENT_FIELD));
    }

    private final List<InferenceString> inferenceStrings;
    private final boolean containsNonTextEntry;

    /**
     * @param inferenceStrings the list of {@link InferenceString} which should result in generating a single embedding vector
     */
    public InferenceStringGroup(List<InferenceString> inferenceStrings) {
        this.inferenceStrings = inferenceStrings;
        containsNonTextEntry = inferenceStrings.stream().anyMatch(s -> s.isText() == false);
    }

    public InferenceStringGroup(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(InferenceString::new));
    }

    public InferenceStringGroup(InferenceString input) {
        this(singletonList(input));
    }

    // Convenience constructor for the common use case of a single text input
    public InferenceStringGroup(String input) {
        this(singletonList(new InferenceString(DataType.TEXT, input)));
    }

    public List<InferenceString> inferenceStrings() {
        return inferenceStrings;
    }

    public boolean containsNonTextEntry() {
        return containsNonTextEntry;
    }

    public boolean containsMultipleInferenceStrings() {
        return inferenceStrings.size() > 1;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(inferenceStrings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONTENT_FIELD, inferenceStrings);
        builder.endObject();
        return builder;
    }

    public static InferenceStringGroup parse(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            // Create content object from String
            return new InferenceStringGroup(parser.text());
        } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
            // Create content object from InferenceString(s)
            return InferenceStringGroup.PARSER.apply(parser, null);
        }
        throw new XContentParseException("Unsupported token [" + token + "]");
    }

    public InferenceString value() {
        assertSingleElement();
        return inferenceStrings.getFirst();
    }

    public String textValue() {
        assertSingleElement();
        return InferenceString.textValue(inferenceStrings.getFirst());
    }

    private void assertSingleElement() {
        assert inferenceStrings.size() == 1 : "Multiple-input InferenceStringGroup used in code path expecting a single input.";
    }

    /**
     * Converts a list of {@link InferenceStringGroup} to an equally-sized list of {@link InferenceString}.
     * <p>
     * <b>
     * This method should only be called in code paths that do not handle grouped embedding inputs, i.e. code paths that expect to generate
     * one embedding per input rather than one embedding for multiple inputs.
     * </b>
     * @param inferenceStringGroups the list of {@link InferenceStringGroup} to convert
     * @return a list of {@link InferenceString}
     */
    public static List<InferenceString> toInferenceStringList(List<InferenceStringGroup> inferenceStringGroups) {
        return inferenceStringGroups.stream().map(group -> {
            assert group.inferenceStrings.size() == 1 : "Multiple-input InferenceStringGroup passed to InferenceStringGroup.toStringList";
            return group.inferenceStrings.getFirst();
        }).toList();
    }

    /**
     * Converts a list of {@link InferenceStringGroup} to an equally-sized list of {@link String}.
     * <p>
     * <b>
     * This method should only be called in code paths that both do not handle grouped embedding inputs AND that do not deal with
     * multimodal inputs, i.e. code paths that expect to generate one embedding per input rather than one embedding for multiple inputs,
     * AND where all inputs are guaranteed to be raw text.
     * </b>
     * @param inferenceStringGroups the list of {@link InferenceStringGroup} to convert
     * @return a list of {@link InferenceString}
     */
    public static List<String> toStringList(List<InferenceStringGroup> inferenceStringGroups) {
        return InferenceString.toStringList(toInferenceStringList(inferenceStringGroups));
    }

    /**
     * Method used to determine if a list of {@link InferenceStringGroup} contains any {@link InferenceString} that represent a non-text
     * value
     *
     * @param inferenceStringGroups the list of {@link InferenceStringGroup} to check
     * @return true if the input list contains any non-text values, false otherwise
     */
    public static boolean containsNonTextEntry(List<InferenceStringGroup> inferenceStringGroups) {
        return inferenceStringGroups.stream().anyMatch(InferenceStringGroup::containsNonTextEntry);
    }

    /**
     * Method used to determine if a list of {@link InferenceStringGroup} contains any with more than one {@link InferenceString} in them
     *
     * @param inferenceStringGroups the list of {@link InferenceStringGroup} to check
     * @return the index of the first {@link InferenceStringGroup} found to contain more than one {@link InferenceString}, or null if no
     * elements in the list contain more than one {@link InferenceString}
     */
    public static Integer indexContainingMultipleInferenceStrings(List<InferenceStringGroup> inferenceStringGroups) {
        for (int i = 0; i < inferenceStringGroups.size(); i++) {
            InferenceStringGroup inferenceStringGroup = inferenceStringGroups.get(i);
            if (inferenceStringGroup.containsMultipleInferenceStrings()) {
                return i;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (InferenceStringGroup) obj;
        return Objects.equals(this.inferenceStrings, that.inferenceStrings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceStrings);
    }

    @Override
    public String toString() {
        return "InferenceStringGroup[" + "inferenceStrings=" + inferenceStrings + ']';
    }

}
