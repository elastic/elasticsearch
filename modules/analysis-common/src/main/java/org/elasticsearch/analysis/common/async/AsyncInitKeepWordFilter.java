/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common.async;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

public class AsyncInitKeepWordFilter extends FilteringTokenFilter {
    private final Supplier<CharArraySet> wordsSupplier;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public AsyncInitKeepWordFilter(TokenStream in, Supplier<CharArraySet> wordsSupplier) {
        super(in);
        this.wordsSupplier = Objects.requireNonNull(wordsSupplier);
    }

    @Override
    protected boolean accept() throws IOException {
        CharArraySet words = wordsSupplier.get();
        if (words == null) {
            throw new ElasticsearchStatusException("keep words supplier not yet initialized", RestStatus.TOO_MANY_REQUESTS);
        }

        return words.contains(termAtt.buffer(), 0, termAtt.length());
    }
}
