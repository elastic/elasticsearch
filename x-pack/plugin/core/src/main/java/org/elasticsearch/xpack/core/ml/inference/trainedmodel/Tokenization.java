/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public abstract class Tokenization implements NamedXContentObject, NamedWriteable {

    public enum Truncate {
        FIRST,
        SECOND,
        NONE;

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

    private static final int DEFAULT_MAX_SEQUENCE_LENGTH = 512;
    private static final boolean DEFAULT_DO_LOWER_CASE = false;
    private static final boolean DEFAULT_WITH_SPECIAL_TOKENS = true;
    private static final Truncate DEFAULT_TRUNCATION = Truncate.FIRST;

    static <T extends Tokenization> void declareCommonFields(ConstructingObjectParser<T, ?> parser) {
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DO_LOWER_CASE);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), WITH_SPECIAL_TOKENS);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_SEQUENCE_LENGTH);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TRUNCATE);
    }

    public static BertTokenization createDefault() {
        return new BertTokenization(null, null, null, Tokenization.DEFAULT_TRUNCATION);
    }

    protected final boolean doLowerCase;
    protected final boolean withSpecialTokens;
    protected final int maxSequenceLength;
    protected final Truncate truncate;

    Tokenization(
        @Nullable Boolean doLowerCase,
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate
    ) {
        if (maxSequenceLength != null && maxSequenceLength <= 0) {
            throw new IllegalArgumentException("[" + MAX_SEQUENCE_LENGTH.getPreferredName() + "] must be positive");
        }
        this.doLowerCase = Optional.ofNullable(doLowerCase).orElse(DEFAULT_DO_LOWER_CASE);
        this.withSpecialTokens = Optional.ofNullable(withSpecialTokens).orElse(DEFAULT_WITH_SPECIAL_TOKENS);
        this.maxSequenceLength = Optional.ofNullable(maxSequenceLength).orElse(DEFAULT_MAX_SEQUENCE_LENGTH);
        this.truncate = Optional.ofNullable(truncate).orElse(DEFAULT_TRUNCATION);
    }

    public Tokenization(StreamInput in) throws IOException {
        this.doLowerCase = in.readBoolean();
        this.withSpecialTokens = in.readBoolean();
        this.maxSequenceLength = in.readVInt();
        this.truncate = in.readEnum(Truncate.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(doLowerCase);
        out.writeBoolean(withSpecialTokens);
        out.writeVInt(maxSequenceLength);
        out.writeEnum(truncate);
    }

    abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DO_LOWER_CASE.getPreferredName(), doLowerCase);
        builder.field(WITH_SPECIAL_TOKENS.getPreferredName(), withSpecialTokens);
        builder.field(MAX_SEQUENCE_LENGTH.getPreferredName(), maxSequenceLength);
        builder.field(TRUNCATE.getPreferredName(), truncate.toString());
        builder = doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tokenization that = (Tokenization) o;
        return doLowerCase == that.doLowerCase
            && withSpecialTokens == that.withSpecialTokens
            && truncate == that.truncate
            && maxSequenceLength == that.maxSequenceLength;
    }

    @Override
    public int hashCode() {
        return Objects.hash(doLowerCase, truncate, withSpecialTokens, maxSequenceLength);
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
}
