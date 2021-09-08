/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.DistilBertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.BertRequestBuilder;
import org.elasticsearch.xpack.ml.inference.nlp.DistilBertRequestBuilder;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;
import org.elasticsearch.xpack.ml.inference.nlp.Vocabulary;

import java.util.List;
import java.util.OptionalInt;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.VOCABULARY;

public interface NlpTokenizer {

    TokenizationResult tokenize(List<String> text);

    NlpTask.RequestBuilder requestBuilder();

    OptionalInt getPadToken();

    static NlpTokenizer build(Vocabulary vocabulary, Tokenization params) {
        ExceptionsHelper.requireNonNull(params, TOKENIZATION);
        ExceptionsHelper.requireNonNull(vocabulary, VOCABULARY);
        if (params instanceof BertTokenization) {
            return BertTokenizer.builder(vocabulary.get(), params).setRequestBuilderFactory(BertRequestBuilder::new).build();
        }
        if (params instanceof DistilBertTokenization) {
            return BertTokenizer.builder(vocabulary.get(), params).setRequestBuilderFactory(DistilBertRequestBuilder::new).build();
        }
        throw new IllegalArgumentException("unknown tokenization type [" + params.getName() + "]");
    }
}
