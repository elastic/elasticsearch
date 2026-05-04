/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Optional;

public class ByteLevelBpeTokenizationUpdate extends AbstractTokenizationUpdate {
    public static final ParseField NAME = new ParseField(ByteLevelBpeTokenization.NAME);

    public static final ConstructingObjectParser<ByteLevelBpeTokenizationUpdate, Void> PARSER = new ConstructingObjectParser<>(
        "byte_level_bpe_tokenization_update",
        a -> new ByteLevelBpeTokenizationUpdate(a[0] == null ? null : Tokenization.Truncate.fromString((String) a[0]), (Integer) a[1])
    );

    static {
        declareCommonParserFields(PARSER);
    }

    public static ByteLevelBpeTokenizationUpdate fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ByteLevelBpeTokenizationUpdate(@Nullable Tokenization.Truncate truncate, @Nullable Integer span) {
        super(truncate, span);
    }

    public ByteLevelBpeTokenizationUpdate(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ByteLevelBpeTokenization.ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED) == false) {
            throw new ElasticsearchStatusException(
                "Cannot send byte_level_bpe tokenization to an older node. "
                    + "Please wait until all nodes are upgraded before using byte_level_bpe tokenization",
                RestStatus.BAD_REQUEST
            );
        }
        super.writeTo(out);
    }

    @Override
    public Tokenization apply(Tokenization originalConfig) {
        if (originalConfig instanceof ByteLevelBpeTokenization byteLevelBpeTokenization) {
            if (isNoop()) {
                return byteLevelBpeTokenization;
            }

            Tokenization.validateSpanAndTruncate(getTruncate(), getSpan());

            if (getTruncate() != null && getTruncate().isInCompatibleWithSpan() == false) {
                return new ByteLevelBpeTokenization(
                    false,
                    byteLevelBpeTokenization.withSpecialTokens(),
                    byteLevelBpeTokenization.maxSequenceLength(),
                    getTruncate(),
                    null,
                    byteLevelBpeTokenization.isAddPrefixSpace(),
                    byteLevelBpeTokenization.getUnkToken(),
                    byteLevelBpeTokenization.getPadToken(),
                    byteLevelBpeTokenization.getBosToken(),
                    byteLevelBpeTokenization.getEosToken(),
                    byteLevelBpeTokenization.getMaskToken()
                );
            }

            return new ByteLevelBpeTokenization(
                false,
                byteLevelBpeTokenization.withSpecialTokens(),
                byteLevelBpeTokenization.maxSequenceLength(),
                Optional.ofNullable(this.getTruncate()).orElse(originalConfig.getTruncate()),
                Optional.ofNullable(this.getSpan()).orElse(originalConfig.getSpan()),
                byteLevelBpeTokenization.isAddPrefixSpace(),
                byteLevelBpeTokenization.getUnkToken(),
                byteLevelBpeTokenization.getPadToken(),
                byteLevelBpeTokenization.getBosToken(),
                byteLevelBpeTokenization.getEosToken(),
                byteLevelBpeTokenization.getMaskToken()
            );
        }
        throw ExceptionsHelper.badRequestException(
            "Tokenization config of type [{}] can not be updated with a request of type [{}]",
            originalConfig.getName(),
            getName()
        );
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }
}
