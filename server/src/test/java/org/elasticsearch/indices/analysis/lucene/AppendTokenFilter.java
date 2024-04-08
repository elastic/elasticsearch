/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis.lucene;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.index.analysis.TokenFilterFactory;

import java.io.IOException;

// Simple token filter that appends text to the term
public final class AppendTokenFilter extends TokenFilter {
    public static TokenFilterFactory factoryForSuffix(String suffix) {
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return suffix;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new AppendTokenFilter(tokenStream, suffix);
            }
        };
    }

    private final CharTermAttribute term = addAttribute(CharTermAttribute.class);
    private final char[] appendMe;

    public AppendTokenFilter(TokenStream input, String appendMe) {
        super(input);
        this.appendMe = appendMe.toCharArray();
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (false == input.incrementToken()) {
            return false;
        }
        term.resizeBuffer(term.length() + appendMe.length);
        System.arraycopy(appendMe, 0, term.buffer(), term.length(), appendMe.length);
        term.setLength(term.length() + appendMe.length);
        return true;
    }
}
