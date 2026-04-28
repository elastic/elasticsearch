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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class represents a String which may be raw text, or the String representation of some other data such as an image in base64
 */
public record InferenceString(DataType dataType, DataFormat dataFormat, String value) implements Writeable, ToXContentObject {
    public static final TransportVersion EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED = TransportVersion.fromName(
        "inference_api_audio_video_pdf_support"
    );

    private static final Pattern DATA_URI_PATTERN = Pattern.compile("^data:.*/.*;base64,");

    static final String TYPE_FIELD = "type";
    static final String FORMAT_FIELD = "format";
    static final String VALUE_FIELD = "value";

    public static final ConstructingObjectParser<InferenceString, Void> PARSER = new ConstructingObjectParser<>(
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
        validateDataURIFormat();
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

    private void validateDataURIFormat() {
        if (dataFormat == DataFormat.BASE64) {
            var endOfURIPart = value.indexOf(',');
            if (endOfURIPart < 0 || DATA_URI_PATTERN.matcher(value.substring(0, endOfURIPart + 1)).matches() == false) {
                throw new IllegalArgumentException(
                    "base64 inputs must be specified as data URIs with the format [data:{MIME-type};base64,...]"
                );
            }
        }
    }

    public InferenceString(StreamInput in) throws IOException {
        this(in.readEnum(DataType.class), in.readEnum(DataFormat.class), in.readString());
    }

    public boolean isText() {
        return DataType.TEXT.equals(dataType);
    }

    public boolean isImage() {
        return DataType.IMAGE.equals(dataType);
    }

    public boolean isAudio() {
        return DataType.AUDIO.equals(dataType);
    }

    public boolean isVideo() {
        return DataType.VIDEO.equals(dataType);
    }

    public boolean isPdf() {
        return DataType.PDF.equals(dataType);
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
        if (out.getTransportVersion().supports(EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED) == false
            && (dataType.equals(DataType.AUDIO) || dataType.equals(DataType.VIDEO) || dataType.equals(DataType.PDF))) {
            throw new ElasticsearchStatusException(
                "Cannot send an inference request with audio, video or pdf inputs to an older node. "
                    + "Please wait until all nodes are upgraded before using audio, video or pdf inputs",
                RestStatus.BAD_REQUEST
            );
        }
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
