/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.xcontent.ParseField;

/**
 * Factory for ASCIIFoldingFilter.
 */
public class ASCIIFoldingTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

    public static final ParseField PRESERVE_ORIGINAL = new ParseField("preserve_original");
    public static final boolean DEFAULT_PRESERVE_ORIGINAL = false;

    private final boolean preserveOriginal;

    public ASCIIFoldingTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        preserveOriginal = settings.getAsBoolean(PRESERVE_ORIGINAL.getPreferredName(), DEFAULT_PRESERVE_ORIGINAL);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new ASCIIFoldingFilter(tokenStream, preserveOriginal);
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        if (preserveOriginal == false) {
            return this;
        } else {
            // See https://issues.apache.org/jira/browse/LUCENE-7536 for the reasoning
            return new TokenFilterFactory() {
                @Override
                public String name() {
                    return ASCIIFoldingTokenFilterFactory.this.name();
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return new ASCIIFoldingFilter(tokenStream, false);
                }
            };
        }
    }

    public TokenStream normalize(TokenStream tokenStream) {
        // Normalization should only emit a single token, so always turn off preserveOriginal
        return new ASCIIFoldingFilter(tokenStream, false);
    }

}
