/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class BertTokenizationUpdate implements TokenizationUpdate {

    public static final ParseField NAME = BertTokenization.NAME;

    public static ConstructingObjectParser<BertTokenizationUpdate, Void> PARSER = new ConstructingObjectParser<>(
        "bert_tokenization_update",
        a -> new BertTokenizationUpdate(a[0] == null ? null : Tokenization.Truncate.fromString((String) a[0]), (Integer) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), Tokenization.TRUNCATE);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), Tokenization.SPAN);
    }

    public static BertTokenizationUpdate fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Tokenization.Truncate truncate;
    private final Integer span;

    public BertTokenizationUpdate(@Nullable Tokenization.Truncate truncate, @Nullable Integer span) {
        this.truncate = truncate;
        this.span = span;
    }

    public BertTokenizationUpdate(StreamInput in) throws IOException {
        this.truncate = in.readOptionalEnum(Tokenization.Truncate.class);
        if (in.getVersion().onOrAfter(Version.V_8_2_0)) {
            this.span = in.readOptionalInt();
        } else {
            this.span = null;
        }
    }

    @Override
    public Tokenization apply(Tokenization originalConfig) {
        if (originalConfig instanceof BertTokenization == false) {
            throw ExceptionsHelper.badRequestException(
                "Tokenization config of type [{}] can not be updated with a request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }

        if (isNoop()) {
            return originalConfig;
        }

        return new BertTokenization(
            originalConfig.doLowerCase(),
            originalConfig.withSpecialTokens(),
            originalConfig.maxSequenceLength(),
            Optional.ofNullable(this.truncate).orElse(originalConfig.getTruncate()),
            Optional.ofNullable(this.span).orElse(originalConfig.getSpan())
        );
    }

    @Override
    public boolean isNoop() {
        return truncate == null && span == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (truncate != null) {
            builder.field(Tokenization.TRUNCATE.getPreferredName(), truncate.toString());
        }
        if (span != null) {
            builder.field(Tokenization.SPAN.getPreferredName(), span);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return BertTokenization.NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(truncate);
        if (out.getVersion().onOrAfter(Version.V_8_2_0)) {
            out.writeOptionalInt(span);
        }
    }

    @Override
    public String getName() {
        return BertTokenization.NAME.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BertTokenizationUpdate that = (BertTokenizationUpdate) o;
        return Objects.equals(truncate, that.truncate) && Objects.equals(span, that.span);
    }

    @Override
    public int hashCode() {
        return Objects.hash(truncate, span);
    }
}
