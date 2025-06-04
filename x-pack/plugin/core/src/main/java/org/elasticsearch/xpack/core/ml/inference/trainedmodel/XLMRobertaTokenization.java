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
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;

import java.io.IOException;

public class XLMRobertaTokenization extends Tokenization {
    public static final String NAME = "xlm_roberta";
    public static final String MASK_TOKEN = "<mask>";

    public static ConstructingObjectParser<XLMRobertaTokenization, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<XLMRobertaTokenization, Void> parser = new ConstructingObjectParser<>(
            "xlm_roberta_tokenization",
            ignoreUnknownFields,
            a -> new XLMRobertaTokenization(
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

    private static final ConstructingObjectParser<XLMRobertaTokenization, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<XLMRobertaTokenization, Void> STRICT_PARSER = createParser(false);

    public static XLMRobertaTokenization fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private XLMRobertaTokenization(
        @Nullable Boolean doLowerCase,
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span
    ) {
        this(withSpecialTokens, maxSequenceLength, truncate, span);
        if (doLowerCase != null && doLowerCase) {
            throw new IllegalArgumentException("unable to set [do_lower_case] to [true] for XLMRoberta tokenizer");
        }
    }

    public XLMRobertaTokenization(
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span
    ) {
        super(false, withSpecialTokens, maxSequenceLength, truncate, span);
    }

    public XLMRobertaTokenization(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected Tokenization buildWindowingTokenization(int maxSeqLength, int span) {
        return new XLMRobertaTokenization(withSpecialTokens, maxSeqLength, Truncate.NONE, span);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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
    public String getName() {
        return NAME;
    }

    @Override
    public void validateVocabulary(PutTrainedModelVocabularyAction.Request request) {
        if (request.getScores().isEmpty()) {
            throw new ElasticsearchStatusException(
                "cannot put vocabulary for model [{}] as tokenizer type [{}] requires [{}] to be provided and non-empty",
                RestStatus.BAD_REQUEST,
                request.getModelId(),
                getName(),
                PutTrainedModelVocabularyAction.Request.SCORES.getPreferredName()
            );
        }
    }
}
