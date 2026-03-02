/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.optionalField;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.DATA_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.FORMAT_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.ID_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.INDEX_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.REASONING_DETAIL_TYPE_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.SIGNATURE_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.SUMMARY_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.TEXT_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.TYPE_FIELD;
import static org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResultsUtils.getUnrecognizedTypeException;

/**
 * This class represents the reasoning detail for a delta. It is a sealed class that has three implementations:
 * <ul>
 *     <li>{@link EncryptedReasoningDetail}: contains encrypted reasoning data.</li>
 *     <li>{@link SummaryReasoningDetail}: contains a summary of the reasoning.</li>
 *     <li>{@link TextReasoningDetail}: may contain the reasoning in text form and/or a signature for the reasoning.</li>
 * </ul>
 * <p>
 * The type of the reasoning detail is determined by the <code>type</code> field, which is required for all reasoning details.
 * Depending on the value of the <code>type</code> field, different fields are required or optional for the reasoning detail.
 */
public abstract sealed class ReasoningDetail implements NamedWriteable, ChunkedToXContent permits ReasoningDetail.EncryptedReasoningDetail,
    ReasoningDetail.SummaryReasoningDetail, ReasoningDetail.TextReasoningDetail {

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

    protected ReasoningDetail(
        ReasoningDetail.ReasoningDetailType type,
        @Nullable String format,
        @Nullable String id,
        @Nullable Long index
    ) {
        this.type = Objects.requireNonNull(type);
        this.format = format;
        this.id = id;
        this.index = index;
    }

    protected ReasoningDetail.ReasoningDetailType type() {
        return type;
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(type);
        out.writeOptionalString(format);
        out.writeOptionalString(id);
        out.writeOptionalVLong(index);
    }

    /**
     * Reads a reasoning detail from the given {@link StreamInput},
     * using the <code>type</code> field to determine which subclass of {@link ReasoningDetail} to read.
     * @param in the stream input to read from
     * @return the reasoning detail read from the stream
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public static ReasoningDetail fromStream(StreamInput in) throws IOException {
        return switch (in.readEnum(ReasoningDetail.ReasoningDetailType.class)) {
            case ENCRYPTED -> new ReasoningDetail.EncryptedReasoningDetail(in);
            case SUMMARY -> new ReasoningDetail.SummaryReasoningDetail(in);
            case TEXT -> new ReasoningDetail.TextReasoningDetail(in);
        };
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

        /**
         * Returns an iterator of XContent chunks representing this encrypted reasoning detail with the following structure:
         * <ul>
         *     <li>type: 'reasoning.encrypted'</li>
         *     <li>format?: string</li>
         *     <li>id?: string</li>
         *     <li>index?: number</li>
         *     <li>data: string</li>
         * </ul>
         * @param params XContent parameters to propagate for serialization
         * @return iterator of XContent chunks representing this encrypted reasoning detail
         */
        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                chunk((b, p) -> b.field(TYPE_FIELD, type())),
                optionalField(FORMAT_FIELD, format()),
                optionalField(ID_FIELD, id()),
                optionalField(INDEX_FIELD, index()),
                chunk((b, p) -> b.field(DATA_FIELD, data)),
                ChunkedToXContentHelper.endObject()
            );
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

        /**
         * Returns an iterator of XContent chunks representing this summary reasoning detail with the following structure:
         * <ul>
         *     <li>type: 'reasoning.summary'</li>
         *     <li>format?: string</li>
         *     <li>id?: string</li>
         *     <li>index?: number</li>
         *     <li>summary: string</li>
         * </ul>
         * @param params XContent parameters to propagate for serialization
         * @return iterator of XContent chunks representing this summary reasoning detail
         */
        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                chunk((b, p) -> b.field(TYPE_FIELD, type())),
                optionalField(FORMAT_FIELD, format()),
                optionalField(ID_FIELD, id()),
                optionalField(INDEX_FIELD, index()),
                chunk((b, p) -> b.field(SUMMARY_FIELD, summary)),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    /**
     * This class represents a reasoning detail, which may contain the reasoning in text form and/or a signature for the reasoning.
     */
    public static final class TextReasoningDetail extends ReasoningDetail {

        public static final String NAME = "text_reasoning_detail";

        private final String text;
        private final String signature;

        public TextReasoningDetail(String format, String id, Long index, @Nullable String text, @Nullable String signature) {
            super(ReasoningDetailType.TEXT, format, id, index);
            this.text = text;
            this.signature = signature;
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

        /**
         * Returns an iterator of XContent chunks representing this text reasoning detail with the following structure:
         * <ul>
         *     <li>type: 'reasoning.text'</li>
         *     <li>format?: string</li>
         *     <li>id?: string</li>
         *     <li>index?: number</li>
         *     <li>text?: string</li>
         *     <li>signature?: string</li>
         * </ul>
         * @param params XContent parameters to propagate for serialization
         * @return iterator of XContent chunks representing this text reasoning detail
         */
        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                chunk((b, p) -> b.field(TYPE_FIELD, type())),
                optionalField(FORMAT_FIELD, format()),
                optionalField(ID_FIELD, id()),
                optionalField(INDEX_FIELD, index()),
                optionalField(TEXT_FIELD, text),
                optionalField(SIGNATURE_FIELD, signature),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
