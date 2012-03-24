/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.Fieldable;

import java.io.IOException;
import java.io.Reader;

/**
 *
 */
public final class CustomAnalyzer extends Analyzer {

    private final TokenizerFactory tokenizerFactory;

    private final CharFilterFactory[] charFilters;

    private final TokenFilterFactory[] tokenFilters;

    private final int positionIncrementGap;
    private final int offsetGap;

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters, TokenFilterFactory[] tokenFilters) {
        this(tokenizerFactory, charFilters, tokenFilters, 0, -1);
    }

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters, TokenFilterFactory[] tokenFilters,
                          int positionOffsetGap, int offsetGap) {
        this.tokenizerFactory = tokenizerFactory;
        this.charFilters = charFilters;
        this.tokenFilters = tokenFilters;
        this.positionIncrementGap = positionOffsetGap;
        this.offsetGap = offsetGap;
    }


    public TokenizerFactory tokenizerFactory() {
        return tokenizerFactory;
    }

    public TokenFilterFactory[] tokenFilters() {
        return tokenFilters;
    }

    public CharFilterFactory[] charFilters() {
        return charFilters;
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return this.positionIncrementGap;
    }

    @Override
    public int getOffsetGap(Fieldable field) {
        if (offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return this.offsetGap;
    }

    @Override
    public final TokenStream tokenStream(String fieldName, Reader reader) {
        return buildHolder(reader).tokenStream;
    }

    @Override
    public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
        Holder holder = (Holder) getPreviousTokenStream();
        if (holder == null) {
            holder = buildHolder(charFilterIfNeeded(reader));
            setPreviousTokenStream(holder);
        } else {
            holder.tokenizer.reset(charFilterIfNeeded(reader));
        }
        return holder.tokenStream;
    }

    private Holder buildHolder(Reader input) {
        Tokenizer tokenizer = tokenizerFactory.create(input);
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : tokenFilters) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new Holder(tokenizer, tokenStream);
    }

    private Reader charFilterIfNeeded(Reader reader) {
        if (charFilters != null && charFilters.length > 0) {
            CharStream charStream = CharReader.get(reader);
            for (CharFilterFactory charFilter : charFilters) {
                charStream = charFilter.create(charStream);
            }
            reader = charStream;
        }
        return reader;
    }

    static class Holder {
        final Tokenizer tokenizer;
        final TokenStream tokenStream;

        private Holder(Tokenizer tokenizer, TokenStream tokenStream) {
            this.tokenizer = tokenizer;
            this.tokenStream = tokenStream;
        }
    }
}
