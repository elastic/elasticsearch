/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;

import java.util.List;

public class JapaneseWordPieceAnalyzer extends WordPieceAnalyzer {

    public JapaneseWordPieceAnalyzer(
        List<String> vocabulary,
        List<String> neverSplit,
        boolean doLowerCase,
        boolean doStripAccents,
        String unknownToken
    ) {
        // For Japanese text with JapaneseTokenizer(morphological analyzer), always disable the punctuation (doTokenizeCjKChars=false)
        super(vocabulary, neverSplit, doLowerCase, false, doStripAccents, unknownToken);
    }

    protected Tokenizer createTokenizer() {
        return new JapaneseTokenizer(null, false, JapaneseTokenizer.Mode.SEARCH);
    }
}
