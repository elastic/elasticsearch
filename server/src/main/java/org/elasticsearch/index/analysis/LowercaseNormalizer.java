/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

import java.util.function.Supplier;

/** Normalizer used to lowercase values */
public final class LowercaseNormalizer extends Analyzer {

    private final Supplier<Tokenizer> tokenizerSupplier;

    public LowercaseNormalizer() {
        this(KeywordTokenizer::new);
    }

    /**
     * @param tokenizerSupplier tokenizer to lowercase the output of; a whitespace tokenizer backs
     *                          {@code split_queries_on_whitespace}
     */
    public LowercaseNormalizer(Supplier<Tokenizer> tokenizerSupplier) {
        this.tokenizerSupplier = tokenizerSupplier;
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        final Tokenizer tokenizer = tokenizerSupplier.get();
        TokenStream stream = new LowerCaseFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, stream);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        return new LowerCaseFilter(in);
    }
}
