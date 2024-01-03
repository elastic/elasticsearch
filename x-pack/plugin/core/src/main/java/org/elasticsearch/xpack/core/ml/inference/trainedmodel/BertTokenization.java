/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class BertTokenization extends Tokenization {

    public static final ParseField NAME = new ParseField("bert");

    public static final String MASK_TOKEN = "[MASK]";

    public static ConstructingObjectParser<BertTokenization, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<BertTokenization, Void> parser = new ConstructingObjectParser<>(
            "bert_tokenization",
            ignoreUnknownFields,
            a -> new BertTokenization(
                (Boolean) a[0],
                (Boolean) a[1],
                (Integer) a[2],
                a[3] == null ? null : Truncate.fromString((String) a[3]),
                (Integer) a[4]
            )
        );
        Tokenization.declareCommonFields(parser);
        return parser;
    }

    private static final ConstructingObjectParser<BertTokenization, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<BertTokenization, Void> STRICT_PARSER = createParser(false);

    public static BertTokenization fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public BertTokenization(
        @Nullable Boolean doLowerCase,
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span
    ) {
        super(doLowerCase, withSpecialTokens, maxSequenceLength, truncate, span);
    }

    public BertTokenization(StreamInput in) throws IOException {
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
    public String getMaskToken() {
        return MASK_TOKEN;
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
