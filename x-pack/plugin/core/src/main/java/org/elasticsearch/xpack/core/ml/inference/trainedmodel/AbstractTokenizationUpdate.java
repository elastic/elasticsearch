/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractTokenizationUpdate implements TokenizationUpdate {

    private final Tokenization.Truncate truncate;
    private final Integer span;

    protected static void declareCommonParserFields(ConstructingObjectParser<? extends AbstractTokenizationUpdate, Void> parser) {
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), Tokenization.TRUNCATE);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), Tokenization.SPAN);
    }

    public AbstractTokenizationUpdate(@Nullable Tokenization.Truncate truncate, @Nullable Integer span) {
        this.truncate = truncate;
        this.span = span;
    }

    public AbstractTokenizationUpdate(StreamInput in) throws IOException {
        this.truncate = in.readOptionalEnum(Tokenization.Truncate.class);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
            this.span = in.readOptionalInt();
        } else {
            this.span = null;
        }
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(truncate);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
            out.writeOptionalInt(span);
        }
    }

    public Integer getSpan() {
        return span;
    }

    public Tokenization.Truncate getTruncate() {
        return truncate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof AbstractTokenizationUpdate == false) {
            return false;
        }
        AbstractTokenizationUpdate that = (AbstractTokenizationUpdate) o;
        return Objects.equals(truncate, that.truncate) && Objects.equals(span, that.span);
    }

    @Override
    public int hashCode() {
        return Objects.hash(truncate, span);
    }
}
