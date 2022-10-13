/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;

public final class InferenceConfigTestScaffolding {

    static Tokenization mutateTokenizationForVersion(Tokenization tokenization, Version version) {
        if (tokenization instanceof BertTokenization bertTokenization) {
            return BertTokenizationTests.mutateForVersion(bertTokenization, version);
        } else if (tokenization instanceof MPNetTokenization mpNetTokenization) {
            return MPNetTokenizationTests.mutateForVersion(mpNetTokenization, version);
        } else if (tokenization instanceof RobertaTokenization robertaTokenization) {
            return RobertaTokenizationTests.mutateForVersion(robertaTokenization, version);
        } else {
            throw new IllegalArgumentException("unknown tokenization [" + tokenization.getName() + "]");
        }
    }

    static Tokenization cloneWithNewTruncation(Tokenization tokenization, Tokenization.Truncate truncate) {
        if (tokenization instanceof MPNetTokenization) {
            return new MPNetTokenization(
                tokenization.doLowerCase(),
                tokenization.withSpecialTokens(),
                tokenization.maxSequenceLength(),
                truncate,
                tokenization.getSpan()
            );
        } else if (tokenization instanceof RobertaTokenization robertaTokenization) {
            return new RobertaTokenization(
                robertaTokenization.withSpecialTokens,
                robertaTokenization.isAddPrefixSpace(),
                robertaTokenization.maxSequenceLength,
                truncate,
                robertaTokenization.span
            );
        } else if (tokenization instanceof BertTokenization) {
            return new BertTokenization(
                tokenization.doLowerCase(),
                tokenization.withSpecialTokens(),
                tokenization.maxSequenceLength(),
                truncate,
                tokenization.getSpan()
            );
        }
        throw new IllegalArgumentException("unknown tokenization [" + tokenization.getName() + "] for truncate update tests");

    }

    static TokenizationUpdate createTokenizationUpdate(Tokenization tokenization, Tokenization.Truncate truncate, Integer span) {
        if (tokenization instanceof MPNetTokenization) {
            return new MPNetTokenizationUpdate(truncate, span);
        } else if (tokenization instanceof RobertaTokenization) {
            return new RobertaTokenizationUpdate(truncate, span);
        } else if (tokenization instanceof BertTokenization) {
            return new BertTokenizationUpdate(truncate, span);
        }
        throw new IllegalArgumentException("unknown tokenization [" + tokenization.getName() + "] for truncate update tests");
    }

}
