/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.DATA_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.FORMAT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.INDEX_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.REASONING_DETAIL_TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.SIGNATURE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.SUMMARY_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TEXT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.getUnrecognizedTypeException;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class represents the reasoning detail for a message. It is a sealed class that has three implementations:
 * <ul>
 *     <li>{@link EncryptedReasoningDetail}: contains encrypted reasoning data.</li>
 *     <li>{@link SummaryReasoningDetail}: contains a summary of the reasoning.</li>
 *     <li>{@link TextReasoningDetail}: may contain the reasoning in text form and/or a signature for the reasoning.</li>
 * </ul>
 * <p>
 * The type of the reasoning detail is determined by the <code>type</code> field, which is required for all reasoning details.
 * Depending on the value of the <code>type</code> field, different fields are required or optional for the reasoning detail.
 */
public abstract sealed class ReasoningDetail implements NamedWriteable, ToXContent permits ReasoningDetail.EncryptedReasoningDetail,
    ReasoningDetail.SummaryReasoningDetail, ReasoningDetail.TextReasoningDetail {

    public static final ConstructingObjectParser<ReasoningDetail, Void> PARSER = new ConstructingObjectParser<>(
        ReasoningDetail.class.getSimpleName(),
        args -> switch (ReasoningDetailType.fromString((String) args[0])) {
            case ENCRYPTED -> new EncryptedReasoningDetail((String) args[1], (String) args[2], (Long) args[3], (String) args[4]);
            case SUMMARY -> new SummaryReasoningDetail((String) args[1], (String) args[2], (Long) args[3], (String) args[5]);
            case TEXT -> new TextReasoningDetail((String) args[1], (String) args[2], (Long) args[3], (String) args[6], (String) args[7]);
        }
    );

    static {
        /*
         * Apart from type, the reasoning detail must contain exactly one of the following field sets:
         * 1. data
         * 2. summary
         * 3. text and/or signature (at least one must be present)
         */
        PARSER.declareRequiredFieldSet(DATA_FIELD, SUMMARY_FIELD, TEXT_FIELD, SIGNATURE_FIELD);
        PARSER.declareExclusiveFieldSet(DATA_FIELD, SUMMARY_FIELD, TEXT_FIELD);
        PARSER.declareExclusiveFieldSet(DATA_FIELD, SUMMARY_FIELD, SIGNATURE_FIELD);

        // common fields
        PARSER.declareString(constructorArg(), new ParseField(TYPE_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(FORMAT_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(ID_FIELD));
        PARSER.declareLong(optionalConstructorArg(), new ParseField(INDEX_FIELD));

        // reasoning.encrypted specific field
        PARSER.declareString(optionalConstructorArg(), new ParseField(DATA_FIELD));

        // reasoning.summary specific field
        PARSER.declareString(optionalConstructorArg(), new ParseField(SUMMARY_FIELD));

        // reasoning.text specific fields
        PARSER.declareString(optionalConstructorArg(), new ParseField(TEXT_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(SIGNATURE_FIELD));
    }

    /**
     * The type of the reasoning detail, which determines the required and optional fields for the detail.
     */
    public enum ReasoningDetailType {
        ENCRYPTED("reasoning.encrypted"),
        SUMMARY("reasoning.summary"),
        TEXT("reasoning.text");

        private final String value;

        ReasoningDetailType(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public static ReasoningDetailType fromString(String value) {
            for (ReasoningDetailType type : values()) {
                if (type.value.equals(value)) {
                    return type;
                }
            }
            throw getUnrecognizedTypeException(value, REASONING_DETAIL_TYPE_FIELD, ReasoningDetailType.class);
        }
    }

    private final ReasoningDetailType type;
    private final String format;
    private final String id;
    private final Long index;

    protected ReasoningDetail(ReasoningDetailType type, @Nullable String format, @Nullable String id, @Nullable Long index) {
        this.type = Objects.requireNonNull(type);
        this.format = format;
        this.id = id;
        this.index = index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(type);
        out.writeOptionalString(format);
        out.writeOptionalString(id);
        out.writeOptionalVLong(index);
    }

    public static ReasoningDetail fromStream(StreamInput in) throws IOException {
        return switch (in.readEnum(ReasoningDetailType.class)) {
            case ENCRYPTED -> new EncryptedReasoningDetail(in);
            case SUMMARY -> new SummaryReasoningDetail(in);
            case TEXT -> new TextReasoningDetail(in);
        };
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TYPE_FIELD, type);
        if (format != null) {
            builder.field(FORMAT_FIELD, format);
        }
        if (id != null) {
            builder.field(ID_FIELD, id);
        }
        if (index != null) {
            builder.field(INDEX_FIELD, index);
        }
        return builder;
    }

    /**
     * This class represents a reasoning detail, which contains encrypted reasoning data.
     */
    public static final class EncryptedReasoningDetail extends ReasoningDetail {

        public static final String NAME = "encrypted_reasoning_detail";

        private final String data;

        public EncryptedReasoningDetail(String format, String id, Long index, String data) {
            super(ReasoningDetailType.ENCRYPTED, format, id, index);
            this.data = Objects.requireNonNull(data);
        }

        public EncryptedReasoningDetail(StreamInput in) throws IOException {
            this(in.readOptionalString(), in.readOptionalString(), in.readOptionalVLong(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(data);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            super.toXContent(builder, params);
            builder.field(DATA_FIELD, data);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    /**
     * This class represents a reasoning detail, which contains a summary of the reasoning.
     */
    public static final class SummaryReasoningDetail extends ReasoningDetail {

        public static final String NAME = "summary_reasoning_detail";

        private final String summary;

        public SummaryReasoningDetail(String format, String id, Long index, String summary) {
            super(ReasoningDetailType.SUMMARY, format, id, index);
            this.summary = Objects.requireNonNull(summary);
        }

        public SummaryReasoningDetail(StreamInput in) throws IOException {
            this(in.readOptionalString(), in.readOptionalString(), in.readOptionalVLong(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(summary);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            super.toXContent(builder, params);
            builder.field(SUMMARY_FIELD, summary);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    /**
     * This class represents a reasoning detail, which may contain the reasoning in text form and/or a signature for the reasoning.
     * At least one of the two fields (text and signature) must be present for this type of reasoning detail.
     */
    public static final class TextReasoningDetail extends ReasoningDetail {

        public static final String NAME = "text_reasoning_detail";

        private final String text;
        private final String signature;

        public TextReasoningDetail(String format, String id, Long index, @Nullable String text, @Nullable String signature) {
            super(ReasoningDetailType.TEXT, format, id, index);
            this.text = text;
            this.signature = signature;
            validate();
        }

        /**
         * Validates that at least one of text or signature is provided for a {@link TextReasoningDetail}.
         * This is because a {@link TextReasoningDetail} must contain at least one of the two fields to be meaningful.
         */
        private void validate() {
            if (text == null && signature == null) {
                throw new IllegalArgumentException(
                    "At least one of [text, signature] must be provided for reasoning details of type [reasoning.text]"
                );
            }
        }

        public TextReasoningDetail(StreamInput in) throws IOException {
            this(
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalVLong(),
                in.readOptionalString(),
                in.readOptionalString()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(text);
            out.writeOptionalString(signature);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            super.toXContent(builder, params);
            if (text != null) {
                builder.field(TEXT_FIELD, text);
            }
            if (signature != null) {
                builder.field(SIGNATURE_FIELD, signature);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
