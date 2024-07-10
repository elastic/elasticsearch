/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.SetOnce;

import java.io.IOException;

public class StopRemoteWordListFilter extends FilteringTokenFilter {
    private final SetOnce<CharArraySet> stopWordsHolder;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public StopRemoteWordListFilter(TokenStream in, SetOnce<CharArraySet> stopWordsHolder) {
        super(in);
        this.stopWordsHolder = stopWordsHolder;
    }

    @Override
    protected boolean accept() throws IOException {
        // TODO: Would be nice if we could wrap a StopFilter instance and call its accept method, but it has protected access
        CharArraySet stopWords = stopWordsHolder.get();
        if (stopWords == null) {
            throw new IllegalStateException("Stop filter not initialized yet");
        }

        return stopWords.contains(termAtt.buffer(), 0, termAtt.length()) == false;
    }
}
