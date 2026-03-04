/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.optionalField;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.DATA_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FORMAT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INDEX_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_DETAIL_TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SIGNATURE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SUMMARY_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEXT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.getUnrecognizedTypeException;
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
 * Used for both request and response Chat Completion objects.
 */
public abstract sealed class ReasoningDetail implements ToXContentObject, ChunkedToXContentObject, NamedWriteable permits
    ReasoningDetail.EncryptedReasoningDetail, ReasoningDetail.SummaryReasoningDetail, ReasoningDetail.TextReasoningDetail {

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

        private static final Map<String, ReasoningDetailType> VALUE_MAP = Map.of(
            ENCRYPTED.value,
            ENCRYPTED,
            SUMMARY.value,
            SUMMARY,
            TEXT.value,
            TEXT
        );

        public static ReasoningDetailType fromString(String value) {
            var type = VALUE_MAP.get(value);
            if (type != null) {
                return type;
            }
            throw getUnrecognizedTypeException(value, REASONING_DETAIL_TYPE_FIELD, ReasoningDetailType.class);
        }
    }

    private final String format;
    private final String id;
    private final Long index;

    protected ReasoningDetail(@Nullable String format, @Nullable String id, @Nullable Long index) {
        this.format = format;
        this.id = id;
        this.index = index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        out.writeOptionalString(id);
        out.writeOptionalVLong(index);
    }

    protected ReasoningDetail(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalString(), in.readOptionalVLong());
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(optionalField(FORMAT_FIELD, format()), optionalField(ID_FIELD, id()), optionalField(INDEX_FIELD, index()));
    }

    protected String format() {
        return format;
    }

    protected String id() {
        return id;
    }

    protected Long index() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReasoningDetail that = (ReasoningDetail) o;
        return Objects.equals(format, that.format) && Objects.equals(id, that.id) && Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, id, index);
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    /**
     * This class represents a reasoning detail, which contains encrypted reasoning data.
     */
    public static final class EncryptedReasoningDetail extends ReasoningDetail {

        public static final String NAME = "encrypted_reasoning_detail";

        private final String data;

        public EncryptedReasoningDetail(@Nullable String format, @Nullable String id, @Nullable Long index, String data) {
            super(format, id, index);
            this.data = Objects.requireNonNull(data);
        }

        public EncryptedReasoningDetail(StreamInput in) throws IOException {
            super(in);
            this.data = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(data);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EncryptedReasoningDetail that = (EncryptedReasoningDetail) o;
            return super.equals(that) && Objects.equals(data, that.data);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD, ReasoningDetailType.ENCRYPTED.value);
            super.toXContent(builder, params);
            builder.field(DATA_FIELD, data);
            builder.endObject();
            return builder;
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                super.toXContentChunked(params),
                chunk((b, p) -> b.field(DATA_FIELD, data)),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), data);
        }

        @Override
        public String toString() {
            return Strings.format("EncryptedReasoningDetail{data='%s', format='%s', id='%s', index=%d}", data, format(), id(), index());
        }
    }

    /**
     * This class represents a reasoning detail, which contains a summary of the reasoning.
     */
    public static final class SummaryReasoningDetail extends ReasoningDetail {

        public static final String NAME = "summary_reasoning_detail";

        private final String summary;

        public SummaryReasoningDetail(@Nullable String format, @Nullable String id, @Nullable Long index, String summary) {
            super(format, id, index);
            this.summary = Objects.requireNonNull(summary);
        }

        public SummaryReasoningDetail(StreamInput in) throws IOException {
            super(in);
            this.summary = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(summary);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD, ReasoningDetailType.SUMMARY.value);
            super.toXContent(builder, params);
            builder.field(SUMMARY_FIELD, summary);
            builder.endObject();
            return builder;
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                super.toXContentChunked(params),
                chunk((b, p) -> b.field(SUMMARY_FIELD, summary)),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SummaryReasoningDetail that = (SummaryReasoningDetail) o;
            return super.equals(that) && Objects.equals(summary, that.summary);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), summary);
        }

        @Override
        public String toString() {
            return Strings.format("SummaryReasoningDetail{summary='%s', format='%s', id='%s', index=%d}", summary, format(), id(), index());
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

        public TextReasoningDetail(
            @Nullable String format,
            @Nullable String id,
            @Nullable Long index,
            @Nullable String text,
            @Nullable String signature
        ) {
            super(format, id, index);
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
            super(in);
            this.text = in.readOptionalString();
            this.signature = in.readOptionalString();
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
        public Iterator<? extends ToXContent> toXContentChunked(Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                super.toXContentChunked(params),
                optionalField(TEXT_FIELD, text),
                optionalField(SIGNATURE_FIELD, signature),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TextReasoningDetail that = (TextReasoningDetail) o;
            return super.equals(that) && Objects.equals(text, that.text) && Objects.equals(signature, that.signature);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), text, signature);
        }

        @Override
        public String toString() {
            return Strings.format(
                "TextReasoningDetail{text='%s', signature='%s', format='%s', id='%s', index=%d}",
                text,
                signature,
                format(),
                id(),
                index()
            );
        }
    }
}
