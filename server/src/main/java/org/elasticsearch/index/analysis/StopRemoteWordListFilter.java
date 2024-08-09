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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class StopRemoteWordListFilter extends FilteringTokenFilter {
    private final SetOnce<CharArraySet> stopWordsHolder;
    private final AtomicReference<Exception> asyncInitException;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public StopRemoteWordListFilter(TokenStream in, SetOnce<CharArraySet> stopWordsHolder, AtomicReference<Exception> asyncInitException) {
        super(in);
        this.stopWordsHolder = stopWordsHolder;
        this.asyncInitException = asyncInitException;
    }

    @Override
    protected boolean accept() throws IOException {
        // TODO: Would be nice if we could wrap a StopFilter instance and call its accept method, but it has protected access
        CharArraySet stopWords = stopWordsHolder.get();
        Exception asyncInitException = this.asyncInitException.get();
        if (stopWords == null) {
            throw asyncInitException != null
                ? new ElasticsearchStatusException(
                    "Stop filter async initialization failed",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    asyncInitException
                )
                : new ElasticsearchStatusException("Stop filter not initialized yet", RestStatus.CONFLICT);
        }

        return stopWords.contains(termAtt.buffer(), 0, termAtt.length()) == false;
    }
}
