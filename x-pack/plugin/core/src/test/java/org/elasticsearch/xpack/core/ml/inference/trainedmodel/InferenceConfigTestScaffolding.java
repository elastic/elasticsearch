/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

public final class InferenceConfigTestScaffolding {

    static Tokenization cloneWithNewTruncation(Tokenization tokenization, Tokenization.Truncate truncate) {
        return tokenization instanceof MPNetTokenization
            ? new MPNetTokenization(
                tokenization.doLowerCase(),
                tokenization.withSpecialTokens(),
                tokenization.maxSequenceLength(),
                truncate
            )
            : new BertTokenization(
                tokenization.doLowerCase(),
                tokenization.withSpecialTokens(),
                tokenization.maxSequenceLength(),
                truncate
            );
    }

    static TokenizationUpdate createTokenizationUpdate(Tokenization tokenization, Tokenization.Truncate truncate) {
        return tokenization instanceof MPNetTokenization ? new MPNetTokenizationUpdate(truncate) : new BertTokenizationUpdate(truncate);
    }

}
