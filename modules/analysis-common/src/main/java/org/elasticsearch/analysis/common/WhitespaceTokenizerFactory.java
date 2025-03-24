/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

public class WhitespaceTokenizerFactory extends AbstractTokenizerFactory {

    static final String MAX_TOKEN_LENGTH = "max_token_length";
    private Integer maxTokenLength;

    WhitespaceTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        maxTokenLength = settings.getAsInt(MAX_TOKEN_LENGTH, StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
    }

    @Override
    public Tokenizer create() {
        return new WhitespaceTokenizer(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, maxTokenLength);
    }
}
