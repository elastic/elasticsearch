/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertJapaneseTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.MPNetTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RobertaTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.Vocabulary;

import java.util.List;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class NlpTokenizerTests extends ESTestCase {

    public static final List<String> BERT_REQUIRED_VOCAB = List.of(
        BertTokenizer.CLASS_TOKEN,
        BertTokenizer.SEPARATOR_TOKEN,
        BertTokenizer.MASK_TOKEN,
        BertTokenizer.UNKNOWN_TOKEN,
        BertTokenizer.PAD_TOKEN
    );
    public static final List<String> MPNET_REQUIRED_VOCAB = List.of(
        MPNetTokenizer.UNKNOWN_TOKEN,
        MPNetTokenizer.SEPARATOR_TOKEN,
        MPNetTokenizer.PAD_TOKEN,
        MPNetTokenizer.CLASS_TOKEN,
        MPNetTokenizer.MASK_TOKEN
    );
    public static final List<String> ROBERTA_REQUIRED_VOCAB = List.of(
        RobertaTokenizer.UNKNOWN_TOKEN,
        RobertaTokenizer.SEPARATOR_TOKEN,
        RobertaTokenizer.PAD_TOKEN,
        RobertaTokenizer.CLASS_TOKEN,
        RobertaTokenizer.MASK_TOKEN
    );

    void validateBuilder(List<String> vocab, Tokenization tokenization, Class<?> expectedClass) {
        Vocabulary vocabulary = new Vocabulary(vocab, "model-name", null);
        NlpTokenizer tokenizer = NlpTokenizer.build(vocabulary, tokenization);
        assertThat(tokenizer, instanceOf(expectedClass));
    }

    public void testBuildTokenizer() {
        Tokenization bert = new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1);
        validateBuilder(BERT_REQUIRED_VOCAB, bert, BertTokenizer.class);

        Tokenization bertjp = new BertJapaneseTokenization(null, false, null, Tokenization.Truncate.NONE, -1);
        validateBuilder(BERT_REQUIRED_VOCAB, bertjp, BertJapaneseTokenizer.class);

        Tokenization mpnet = new MPNetTokenization(null, false, null, Tokenization.Truncate.NONE, -1);
        validateBuilder(MPNET_REQUIRED_VOCAB, mpnet, MPNetTokenizer.class);

        Tokenization roberta = new RobertaTokenization(null, false, null, Tokenization.Truncate.NONE, -1);
        validateBuilder(ROBERTA_REQUIRED_VOCAB, roberta, RobertaTokenizer.class);
    }
}
