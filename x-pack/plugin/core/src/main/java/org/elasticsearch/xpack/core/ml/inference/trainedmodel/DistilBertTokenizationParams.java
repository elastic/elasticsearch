/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

public class DistilBertTokenizationParams extends TokenizationParams {

    public static final ParseField NAME = new ParseField("distil_bert");

    public static ConstructingObjectParser<DistilBertTokenizationParams, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<DistilBertTokenizationParams, Void> parser = new ConstructingObjectParser<>(
            "distil_bert_tokenization_params",
            ignoreUnknownFields,
            a -> new DistilBertTokenizationParams((Boolean) a[0], (Boolean) a[1], (Integer) a[2])
        );
        TokenizationParams.declareCommonFields(parser);
        return parser;
    }

    private static final ConstructingObjectParser<DistilBertTokenizationParams, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<DistilBertTokenizationParams, Void> STRICT_PARSER = createParser(false);

    public static DistilBertTokenizationParams fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public DistilBertTokenizationParams(
        @Nullable Boolean doLowerCase,
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength
    ) {
        super(doLowerCase, withSpecialTokens, maxSequenceLength);
    }

    public DistilBertTokenizationParams(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
