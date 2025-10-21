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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Optional;

public class RobertaTokenizationUpdate extends AbstractTokenizationUpdate {
    public static final ParseField NAME = new ParseField(RobertaTokenization.NAME);

    public static final ConstructingObjectParser<RobertaTokenizationUpdate, Void> PARSER = new ConstructingObjectParser<>(
        "roberta_tokenization_update",
        a -> new RobertaTokenizationUpdate(a[0] == null ? null : Tokenization.Truncate.fromString((String) a[0]), (Integer) a[1])
    );

    static {
        declareCommonParserFields(PARSER);
    }

    public static RobertaTokenizationUpdate fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RobertaTokenizationUpdate(@Nullable Tokenization.Truncate truncate, @Nullable Integer span) {
        super(truncate, span);
    }

    public RobertaTokenizationUpdate(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Tokenization apply(Tokenization originalConfig) {
        if (originalConfig instanceof RobertaTokenization robertaTokenization) {
            if (isNoop()) {
                return robertaTokenization;
            }

            Tokenization.validateSpanAndTruncate(getTruncate(), getSpan());

            if (getTruncate() != null && getTruncate().isInCompatibleWithSpan() == false) {
                // When truncate value is incompatible with span wipe out
                // the existing span setting to avoid an invalid combination of settings.
                // This avoids the user have to set span to the special unset value
                return new RobertaTokenization(
                    robertaTokenization.withSpecialTokens(),
                    robertaTokenization.isAddPrefixSpace(),
                    robertaTokenization.maxSequenceLength(),
                    getTruncate(),
                    null
                );
            }

            return new RobertaTokenization(
                robertaTokenization.withSpecialTokens(),
                robertaTokenization.isAddPrefixSpace(),
                robertaTokenization.maxSequenceLength(),
                Optional.ofNullable(this.getTruncate()).orElse(originalConfig.getTruncate()),
                Optional.ofNullable(this.getSpan()).orElse(originalConfig.getSpan())
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
