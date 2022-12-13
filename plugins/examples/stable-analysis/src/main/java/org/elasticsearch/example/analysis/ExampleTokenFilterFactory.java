/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.example.analysis.lucene.AppendTokenFilter;
import org.elasticsearch.example.analysis.lucene.SkipTokenFilter;
import org.elasticsearch.plugin.analysis.api.AnalysisMode;
import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugin.api.Inject;

@NamedComponent( "example_token_filter_factory")
public class ExampleTokenFilterFactory implements org.elasticsearch.plugin.analysis.api.TokenFilterFactory {
    private final long tokenFilterNumber;

    @Inject
    public ExampleTokenFilterFactory(ExampleAnalysisSettings settings) {
        this.tokenFilterNumber = settings.tokenFilterNumber();
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new SkipTokenFilter(tokenStream, tokenFilterNumber);
    }

    @Override
    public TokenStream normalize(TokenStream tokenStream) {
        return new AppendTokenFilter(tokenStream, String.valueOf(tokenFilterNumber));
    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return org.elasticsearch.plugin.analysis.api.TokenFilterFactory.super.getAnalysisMode();
    }

}

