/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.example.analysis.lucene.CharSkippingTokenizer;
import org.elasticsearch.plugin.analysis.TokenizerFactory;
import org.elasticsearch.plugin.Inject;
import org.elasticsearch.plugin.NamedComponent;

import java.util.List;

@NamedComponent("example_tokenizer_factory")
public class CharacterSkippingTokenizerFactory implements TokenizerFactory {
    private final List<String> tokenizerListOfChars;

    @Inject
    public CharacterSkippingTokenizerFactory(ExampleAnalysisSettings settings) {
        this.tokenizerListOfChars = settings.singleCharsToSkipInTokenizer();
    }

    @Override
    public Tokenizer create() {
        return new CharSkippingTokenizer(tokenizerListOfChars);
    }
}

