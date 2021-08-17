/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

public class TokenizationParams implements ToXContentObject, Writeable {

    public static final ParseField DO_LOWER_CASE = new ParseField("do_lower_case");
    public static final ParseField WITH_SPECIAL_TOKENS = new ParseField("with_special_tokens");
    public static final ParseField MAX_SEQUENCE_LENGTH = new ParseField("max_sequence_length");

    private static final int DEFAULT_MAX_SEQUENCE_LENGTH = 512;

    public static ConstructingObjectParser<TokenizationParams, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TokenizationParams, Void> parser = new ConstructingObjectParser<>("tokenization_params",
            ignoreUnknownFields, a -> new TokenizationParams((Boolean) a[0], (Boolean) a[1], (Integer) a[2]));
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DO_LOWER_CASE);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), WITH_SPECIAL_TOKENS);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_SEQUENCE_LENGTH);
        return parser;
    }

    private final boolean doLowerCase;
    private final boolean withSpecialTokens;
    private final int maxSequenceLength;

    public static TokenizationParams createDefault() {
        return new TokenizationParams(null, null, null);
    }

    public TokenizationParams(@Nullable Boolean doLowerCase, @Nullable Boolean withSpecialTokens, @Nullable Integer maxSequenceLength) {
        if (maxSequenceLength != null && maxSequenceLength <= 0) {
            throw new IllegalArgumentException("[" + MAX_SEQUENCE_LENGTH.getPreferredName() + "] must be positive");
        }
        this.doLowerCase = doLowerCase == null ? false : doLowerCase;
        this.withSpecialTokens = withSpecialTokens == null ? true : withSpecialTokens;
        this.maxSequenceLength = maxSequenceLength == null ? DEFAULT_MAX_SEQUENCE_LENGTH : maxSequenceLength;
    }

    public TokenizationParams(StreamInput in) throws IOException {
        this.doLowerCase = in.readBoolean();
        this.withSpecialTokens = in.readBoolean();
        this.maxSequenceLength = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(doLowerCase);
        out.writeBoolean(withSpecialTokens);
        out.writeVInt(maxSequenceLength);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DO_LOWER_CASE.getPreferredName(), doLowerCase);
        builder.field(WITH_SPECIAL_TOKENS.getPreferredName(), withSpecialTokens);
        builder.field(MAX_SEQUENCE_LENGTH.getPreferredName(), maxSequenceLength);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenizationParams that = (TokenizationParams) o;
        return doLowerCase == that.doLowerCase
            && withSpecialTokens == that.withSpecialTokens
            && maxSequenceLength == that.maxSequenceLength;
    }

    @Override
    public int hashCode() {
        return Objects.hash(doLowerCase, withSpecialTokens, maxSequenceLength);
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
}
