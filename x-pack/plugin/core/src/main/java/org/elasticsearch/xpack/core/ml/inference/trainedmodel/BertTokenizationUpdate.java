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

public class BertTokenizationUpdate extends AbstractTokenizationUpdate {

    public static final ParseField NAME = BertTokenization.NAME;

    public static final ConstructingObjectParser<BertTokenizationUpdate, Void> PARSER = new ConstructingObjectParser<>(
        "bert_tokenization_update",
        a -> new BertTokenizationUpdate(a[0] == null ? null : Tokenization.Truncate.fromString((String) a[0]), (Integer) a[1])
    );

    static {
        declareCommonParserFields(PARSER);
    }

    public static BertTokenizationUpdate fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public BertTokenizationUpdate(@Nullable Tokenization.Truncate truncate, @Nullable Integer span) {
        super(truncate, span);
    }

    public BertTokenizationUpdate(StreamInput in) throws IOException {
        super(in);
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

        Tokenization.validateSpanAndTruncate(getTruncate(), getSpan());

        if (isNoop()) {
            return originalConfig;
        }

        if (getTruncate() != null && getTruncate().isInCompatibleWithSpan() == false) {
            // When truncate value is incompatible with span wipe out
            // the existing span setting to avoid an invalid combination of settings.
            // This avoids the user have to set span to the special unset value
            return new BertTokenization(
                originalConfig.doLowerCase(),
                originalConfig.withSpecialTokens(),
                originalConfig.maxSequenceLength(),
                getTruncate(),
                null
            );
        }

        return new BertTokenization(
            originalConfig.doLowerCase(),
            originalConfig.withSpecialTokens(),
            originalConfig.maxSequenceLength(),
            Optional.ofNullable(getTruncate()).orElse(originalConfig.getTruncate()),
            Optional.ofNullable(getSpan()).orElse(originalConfig.getSpan())
        );
    }

    @Override
    public String getWriteableName() {
        return BertTokenization.NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return BertTokenization.NAME.getPreferredName();
    }
}
