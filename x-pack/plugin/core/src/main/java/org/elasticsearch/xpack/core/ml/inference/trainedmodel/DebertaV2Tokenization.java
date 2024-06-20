/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class DebertaV2Tokenization extends Tokenization {

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
        return null;
    }

    @Override
    public String getMaskToken() {
        return "";
    }

    @Override
    XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public String getWriteableName() {
        return "";
    }

    @Override
    public String getName() {
        return "";
    }
}
