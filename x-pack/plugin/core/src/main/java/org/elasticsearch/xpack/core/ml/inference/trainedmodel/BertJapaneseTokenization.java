/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class BertJapaneseTokenization extends Tokenization {

    public static final ParseField NAME = new ParseField("bert_ja");

    public static ConstructingObjectParser<BertJapaneseTokenization, Void> createJpParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<BertJapaneseTokenization, Void> parser = new ConstructingObjectParser<>(
            "bert_japanese_tokenization",
            ignoreUnknownFields,
            a -> new BertJapaneseTokenization(
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

    private static final ConstructingObjectParser<BertJapaneseTokenization, Void> JP_LENIENT_PARSER = createJpParser(true);
    private static final ConstructingObjectParser<BertJapaneseTokenization, Void> JP_STRICT_PARSER = createJpParser(false);

    public static BertJapaneseTokenization fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? JP_LENIENT_PARSER.apply(parser, null) : JP_STRICT_PARSER.apply(parser, null);
    }

    public BertJapaneseTokenization(
        @Nullable Boolean doLowerCase,
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span
    ) {
        super(doLowerCase, withSpecialTokens, maxSequenceLength, truncate, span);
    }

    public BertJapaneseTokenization(StreamInput in) throws IOException {
        super(in);
    }

    XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getWriteableName() {
        return BertJapaneseTokenization.NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return BertJapaneseTokenization.NAME.getPreferredName();
    }
}
