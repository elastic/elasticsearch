/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;

import java.io.IOException;
import java.util.Optional;

public class RobertaTokenization extends Tokenization {
    public static final String NAME = "roberta";
    public static final String MASK_TOKEN = "<mask>";
    private static final boolean DEFAULT_ADD_PREFIX_SPACE = false;

    private static final ParseField ADD_PREFIX_SPACE = new ParseField("add_prefix_space");

    public static ConstructingObjectParser<RobertaTokenization, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<RobertaTokenization, Void> parser = new ConstructingObjectParser<>(
            "roberta_tokenization",
            ignoreUnknownFields,
            a -> new RobertaTokenization(
                (Boolean) a[0],
                (Boolean) a[1],
                (Integer) a[2],
                a[3] == null ? null : Truncate.fromString((String) a[3]),
                (Integer) a[4],
                (Boolean) a[5]
            )
        );
        declareCommonFields(parser);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ADD_PREFIX_SPACE);
        return parser;
    }

    private static final ConstructingObjectParser<RobertaTokenization, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<RobertaTokenization, Void> STRICT_PARSER = createParser(false);

    public static RobertaTokenization fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final boolean addPrefixSpace;

    private RobertaTokenization(
        @Nullable Boolean doLowerCase,
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span,
        @Nullable Boolean addPrefixSpace
    ) {
        this(withSpecialTokens, addPrefixSpace, maxSequenceLength, truncate, span);
        if (doLowerCase != null && doLowerCase) {
            throw new IllegalArgumentException("unable to set [do_lower_case] to [true] for roberta tokenizer");
        }
    }

    public RobertaTokenization(
        @Nullable Boolean withSpecialTokens,
        @Nullable Boolean addPrefixSpace,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span
    ) {
        super(false, withSpecialTokens, maxSequenceLength, truncate, span);
        this.addPrefixSpace = Optional.ofNullable(addPrefixSpace).orElse(DEFAULT_ADD_PREFIX_SPACE);
    }

    public RobertaTokenization(StreamInput in) throws IOException {
        super(in);
        this.addPrefixSpace = in.readBoolean();
    }

    @Override
    Tokenization buildWindowingTokenization(int updatedMaxSeqLength, int updatedSpan) {
        return new RobertaTokenization(
            this.doLowerCase,
            this.withSpecialTokens,
            updatedMaxSeqLength,
            Truncate.NONE,
            updatedSpan,
            this.addPrefixSpace
        );
    }

    public boolean isAddPrefixSpace() {
        return addPrefixSpace;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(addPrefixSpace);
    }

    @Override
    public String getMaskToken() {
        return MASK_TOKEN;
    }

    @Override
    XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ADD_PREFIX_SPACE.getPreferredName(), addPrefixSpace);
        return builder;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void validateVocabulary(PutTrainedModelVocabularyAction.Request request) {
        if (request.getMerges().isEmpty()) {
            throw new ElasticsearchStatusException(
                "cannot put vocabulary for model [{}] as tokenizer type [{}] requires [{}] to be provided and non-empty",
                RestStatus.BAD_REQUEST,
                request.getModelId(),
                getName(),
                PutTrainedModelVocabularyAction.Request.MERGES.getPreferredName()
            );
        }
    }
}
