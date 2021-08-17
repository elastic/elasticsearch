/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenizationParams;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.DistilBertTokenizationParams;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationParams;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;
import org.elasticsearch.xpack.ml.inference.nlp.Vocabulary;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION_PARAMS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.VOCABULARY;

public interface NlpTokenizer {

    TokenizationResult tokenize(String text);

    NlpTask.RequestBuilder requestBuilder(NlpConfig config, NlpTask.ResultProcessorFactory factory);

    static NlpTokenizer build(Vocabulary vocabulary, TokenizationParams params) {
        ExceptionsHelper.requireNonNull(params, TOKENIZATION_PARAMS);
        ExceptionsHelper.requireNonNull(vocabulary, VOCABULARY);
        if (params instanceof BertTokenizationParams) {
            return BertTokenizer.builder(vocabulary.get(), params).build();
        }
        if (params instanceof DistilBertTokenizationParams) {
            return DistilBertTokenizer.builder(vocabulary.get(), params).build();
        }
        throw new IllegalArgumentException("unknown tokenization_params type [" + params.getName() + "]");
    }
}
