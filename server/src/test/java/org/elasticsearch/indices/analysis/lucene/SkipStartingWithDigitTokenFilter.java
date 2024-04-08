/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis.lucene;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;

public final class SkipStartingWithDigitTokenFilter extends FilteringTokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final long asciiDigitsToSkip;

    public SkipStartingWithDigitTokenFilter(TokenStream in, long asciiDigitsToSkip) {
        super(in);
        this.asciiDigitsToSkip = asciiDigitsToSkip;
    }

    @Override
    protected boolean accept() throws IOException {
        return termAtt.buffer()[0] != (char) (asciiDigitsToSkip + '0');
    }
}
