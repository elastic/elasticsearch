/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DebertaV2Tokenization extends Tokenization {

    public static final String NAME = "deberta_v2";
    public static final String MASK_TOKEN = "[MASK]";

    public static ConstructingObjectParser<DebertaV2Tokenization, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<DebertaV2Tokenization, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new DebertaV2Tokenization(
                (Boolean) a[0],
                (Boolean) a[1],
                (Integer) a[2],
                a[3] == null ? null : Truncate.fromString((String) a[3]),
                (Integer) a[4]
            )
        );
        declareCommonFields(parser);
        return parser;
    }

    private static final ConstructingObjectParser<DebertaV2Tokenization, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<DebertaV2Tokenization, Void> STRICT_PARSER = createParser(false);

    public static DebertaV2Tokenization fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public DebertaV2Tokenization(
        Boolean doLowerCase,
        Boolean withSpecialTokens,
        Integer maxSequenceLength,
        Truncate truncate,
        Integer span
    ) {
        super(doLowerCase, withSpecialTokens, maxSequenceLength, truncate, span);
    }

    public DebertaV2Tokenization(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    Tokenization buildWindowingTokenization(int updatedMaxSeqLength, int updatedSpan) {
        return new DebertaV2Tokenization(doLowerCase, withSpecialTokens, updatedMaxSeqLength, truncate, updatedSpan);
    }

    @Override
    public String getMaskToken() {
        return MASK_TOKEN;
    }

    @Override
    XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
