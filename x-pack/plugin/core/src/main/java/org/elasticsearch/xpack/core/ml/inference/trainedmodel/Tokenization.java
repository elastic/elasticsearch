/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public abstract class Tokenization implements NamedXContentObject, NamedWriteable {

    public enum Truncate {
        FIRST,
        SECOND,
        NONE {
            @Override
            public boolean isInCompatibleWithSpan() {
                return false;
            }
        };

        public boolean isInCompatibleWithSpan() {
            return true;
        }

        public static Truncate fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    // TODO add global params like never_split, bos_token, eos_token, mask_token, tokenize_chinese_chars, strip_accents, etc.
    public static final ParseField DO_LOWER_CASE = new ParseField("do_lower_case");
    public static final ParseField WITH_SPECIAL_TOKENS = new ParseField("with_special_tokens");
    public static final ParseField MAX_SEQUENCE_LENGTH = new ParseField("max_sequence_length");
    public static final ParseField TRUNCATE = new ParseField("truncate");
    public static final ParseField SPAN = new ParseField("span");

    private static final int DEFAULT_MAX_SEQUENCE_LENGTH = 512;
    private static final boolean DEFAULT_DO_LOWER_CASE = false;
    private static final boolean DEFAULT_WITH_SPECIAL_TOKENS = true;
    private static final Truncate DEFAULT_TRUNCATION = Truncate.FIRST;
    private static final int UNSET_SPAN_VALUE = -1;

    static <T extends Tokenization> void declareCommonFields(ConstructingObjectParser<T, ?> parser) {
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DO_LOWER_CASE);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), WITH_SPECIAL_TOKENS);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_SEQUENCE_LENGTH);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TRUNCATE);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), SPAN);
    }

    public static BertTokenization createDefault() {
        return new BertTokenization(null, null, null, Tokenization.DEFAULT_TRUNCATION, UNSET_SPAN_VALUE);
    }

    protected final boolean doLowerCase;
    protected final boolean withSpecialTokens;
    protected final int maxSequenceLength;
    protected final Truncate truncate;
    protected final int span;

    Tokenization(
        @Nullable Boolean doLowerCase,
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span
    ) {
        if (maxSequenceLength != null && maxSequenceLength <= 0) {
            throw new IllegalArgumentException("[" + MAX_SEQUENCE_LENGTH.getPreferredName() + "] must be positive");
        }
        this.doLowerCase = Optional.ofNullable(doLowerCase).orElse(DEFAULT_DO_LOWER_CASE);
        this.withSpecialTokens = Optional.ofNullable(withSpecialTokens).orElse(DEFAULT_WITH_SPECIAL_TOKENS);
        this.maxSequenceLength = Optional.ofNullable(maxSequenceLength).orElse(DEFAULT_MAX_SEQUENCE_LENGTH);
        this.truncate = Optional.ofNullable(truncate).orElse(DEFAULT_TRUNCATION);
        this.span = Optional.ofNullable(span).orElse(UNSET_SPAN_VALUE);
        if (this.span < 0 && this.span != UNSET_SPAN_VALUE) {
            throw new IllegalArgumentException(
                "["
                    + SPAN.getPreferredName()
                    + "] must be non-negative to indicate span length or ["
                    + UNSET_SPAN_VALUE
                    + "] to indicate no windowing should occur"
            );
        }
        if (this.span > this.maxSequenceLength) {
            throw new IllegalArgumentException(
                "["
                    + SPAN.getPreferredName()
                    + "] provided ["
                    + this.span
                    + "] must not be greater than ["
                    + MAX_SEQUENCE_LENGTH.getPreferredName()
                    + "] provided ["
                    + this.maxSequenceLength
                    + "]"
            );
        }
        validateSpanAndTruncate(truncate, span);
    }

    public Tokenization(StreamInput in) throws IOException {
        this.doLowerCase = in.readBoolean();
        this.withSpecialTokens = in.readBoolean();
        this.maxSequenceLength = in.readVInt();
        this.truncate = in.readEnum(Truncate.class);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
            this.span = in.readInt();
        } else {
            this.span = UNSET_SPAN_VALUE;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(doLowerCase);
        out.writeBoolean(withSpecialTokens);
        out.writeVInt(maxSequenceLength);
        out.writeEnum(truncate);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
            out.writeInt(span);
        }
    }

    abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DO_LOWER_CASE.getPreferredName(), doLowerCase);
        builder.field(WITH_SPECIAL_TOKENS.getPreferredName(), withSpecialTokens);
        builder.field(MAX_SEQUENCE_LENGTH.getPreferredName(), maxSequenceLength);
        builder.field(TRUNCATE.getPreferredName(), truncate.toString());
        builder.field(SPAN.getPreferredName(), span);
        builder = doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public static void validateSpanAndTruncate(@Nullable Truncate truncate, @Nullable Integer span) {
        if ((span != null && span != UNSET_SPAN_VALUE) && (truncate != null && truncate.isInCompatibleWithSpan())) {
            throw new IllegalArgumentException(
                "[" + SPAN.getPreferredName() + "] must not be provided when [" + TRUNCATE.getPreferredName() + "] is [" + truncate + "]"
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tokenization that = (Tokenization) o;
        return doLowerCase == that.doLowerCase
            && withSpecialTokens == that.withSpecialTokens
            && truncate == that.truncate
            && span == that.span
            && maxSequenceLength == that.maxSequenceLength;
    }

    @Override
    public int hashCode() {
        return Objects.hash(doLowerCase, truncate, withSpecialTokens, maxSequenceLength, span);
    }

    public boolean doLowerCase() {
        return doLowerCase;
    }

    public boolean withSpecialTokens() {
        return withSpecialTokens;
    }

    public int maxSequenceLength() {
        return maxSequenceLength;
    }

    public Truncate getTruncate() {
        return truncate;
    }

    public int getSpan() {
        return span;
    }

    public void validateVocabulary(PutTrainedModelVocabularyAction.Request request) {

    }
}
