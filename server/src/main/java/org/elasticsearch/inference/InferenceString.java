/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class represents a String which may be raw text, or the String representation of some other data such as an image in base64
 */
public record InferenceString(DataType dataType, DataFormat dataFormat, String value) implements Writeable, ToXContentObject {
    static final String TYPE_FIELD = "type";
    static final String FORMAT_FIELD = "format";
    static final String VALUE_FIELD = "value";

    static final ConstructingObjectParser<InferenceString, Void> PARSER = new ConstructingObjectParser<>(
        InferenceString.class.getSimpleName(),
        args -> new InferenceString((DataType) args[0], (DataFormat) args[1], (String) args[2])
    );
    static {
        PARSER.declareString(constructorArg(), DataType::fromString, new ParseField(TYPE_FIELD));
        PARSER.declareString(optionalConstructorArg(), DataFormat::fromString, new ParseField(FORMAT_FIELD));
        PARSER.declareString(constructorArg(), new ParseField(VALUE_FIELD));
    }

    /**
     * Constructs an {@link InferenceString} with the given value and {@link DataType}, using the
     * default {@link DataFormat} for the data type
     *
     * @param dataType the type of data that the String represents
     * @param value    the String value
     */
    public InferenceString(DataType dataType, String value) {
        this(dataType, null, value);
    }

    /**
     * Constructs an {@link InferenceString} with the given value, {@link DataType} and {@link DataFormat}
     *
     * @param dataType   the type of data that the String represents
     * @param dataFormat the format of the data. If {@code null}, the default data format for the given type is used
     * @param value      the String value
     */
    public InferenceString(DataType dataType, @Nullable DataFormat dataFormat, String value) {
        this.dataType = Objects.requireNonNull(dataType);
        this.dataFormat = Objects.requireNonNullElse(dataFormat, this.dataType.getDefaultFormat());
        validateTypeAndFormat();
        this.value = Objects.requireNonNull(value);
    }

    private void validateTypeAndFormat() {
        if (dataType.getSupportedFormats().contains(dataFormat) == false) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Data type [%s] does not support data format [%s], supported formats are %s",
                    dataType,
                    dataFormat,
                    dataType.getSupportedFormats()
                )
            );
        }
    }

    public InferenceString(StreamInput in) throws IOException {
        this(in.readEnum(DataType.class), in.readEnum(DataFormat.class), in.readString());
    }

    public boolean isImage() {
        return DataType.IMAGE.equals(dataType);
    }

    public boolean isText() {
        return DataType.TEXT.equals(dataType);
    }

    /**
     * Converts a list of {@link InferenceString} to a list of {@link String}.
     * <p>
     * <b>
     * This method should only be called in code paths that do not deal with multimodal inputs, i.e. code paths where all inputs are
     * guaranteed to be raw text, since it discards the {@link DataType} associated with
     * each input.
     *</b>
     * @param inferenceStrings The list of {@link InferenceString} to convert to a list of {@link String}
     * @return a list of String inference inputs that do not contain any non-text inputs
     */
    public static List<String> toStringList(List<InferenceString> inferenceStrings) {
        return inferenceStrings.stream().map(InferenceString::textValue).toList();
    }

    /**
     * Converts a single {@link InferenceString} to a {@link String}.
     * <p>
     * <b>
     * This method should only be called in code paths that do not deal with multimodal inputs, i.e. code paths where all inputs are
     * guaranteed to be raw text, since it discards the {@link DataType} associated with
     * each input.
     *</b>
     * @param inferenceString The {@link InferenceString} to convert to a {@link String}
     * @return a String inference input
     */
    public static String textValue(InferenceString inferenceString) {
        assert inferenceString.isText() : "Non-text input returned from InferenceString.textValue";
        return inferenceString.value();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(dataType);
        out.writeEnum(dataFormat);
        out.writeString(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD, dataType);
        builder.field(FORMAT_FIELD, dataFormat);
        builder.field(VALUE_FIELD, value);
        builder.endObject();
        return builder;
    }
}
