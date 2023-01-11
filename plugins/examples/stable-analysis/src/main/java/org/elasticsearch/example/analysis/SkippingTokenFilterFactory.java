/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.example.analysis.lucene.SkipStartingWithDigitTokenFilter;
import org.elasticsearch.plugin.analysis.api.AnalysisMode;
import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugin.api.Inject;

@NamedComponent( "example_token_filter_factory")
public class SkippingTokenFilterFactory implements org.elasticsearch.plugin.analysis.api.TokenFilterFactory {
    private final long tokenFilterNumber;

    @Inject
    public SkippingTokenFilterFactory(ExampleAnalysisSettings settings) {
        this.tokenFilterNumber = settings.digitToSkipInTokenFilter();
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new SkipStartingWithDigitTokenFilter(tokenStream, tokenFilterNumber);
    }

    @Override
    public TokenStream normalize(TokenStream tokenStream) {
        return new SkipStartingWithDigitTokenFilter(tokenStream, tokenFilterNumber);
    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return org.elasticsearch.plugin.analysis.api.TokenFilterFactory.super.getAnalysisMode();
    }

}

