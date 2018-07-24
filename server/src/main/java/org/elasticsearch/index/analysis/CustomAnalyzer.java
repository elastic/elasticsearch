/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.Reader;

public final class CustomAnalyzer extends Analyzer {

    private final String tokenizerName;
    private final TokenizerFactory tokenizerFactory;

    private final CharFilterFactory[] charFilters;

    private final TokenFilterFactory[] tokenFilters;

    private final int positionIncrementGap;
    private final int offsetGap;

    public CustomAnalyzer(String tokenizerName, TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
            TokenFilterFactory[] tokenFilters) {
        this(tokenizerName, tokenizerFactory, charFilters, tokenFilters, 0, -1);
    }

    public CustomAnalyzer(String tokenizerName, TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
            TokenFilterFactory[] tokenFilters, int positionIncrementGap, int offsetGap) {
        this.tokenizerName = tokenizerName;
        this.tokenizerFactory = tokenizerFactory;
        this.charFilters = charFilters;
        this.tokenFilters = tokenFilters;
        this.positionIncrementGap = positionIncrementGap;
        this.offsetGap = offsetGap;
    }

    /**
     * The name of the tokenizer as configured by the user.
     */
    public String getTokenizerName() {
        return tokenizerName;
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
    public int getOffsetGap(String field) {
        if (offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return this.offsetGap;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = tokenizerFactory.create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : tokenFilters) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        if (charFilters != null && charFilters.length > 0) {
            for (CharFilterFactory charFilter : charFilters) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }

    @Override
    protected Reader initReaderForNormalization(String fieldName, Reader reader) {
      for (CharFilterFactory charFilter : charFilters) {
        if (charFilter instanceof MultiTermAwareComponent) {
          charFilter = (CharFilterFactory) ((MultiTermAwareComponent) charFilter).getMultiTermComponent();
          reader = charFilter.create(reader);
        }
      }
      return reader;
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
      TokenStream result = in;
      for (TokenFilterFactory filter : tokenFilters) {
        if (filter instanceof MultiTermAwareComponent) {
          filter = (TokenFilterFactory) ((MultiTermAwareComponent) filter).getMultiTermComponent();
          result = filter.create(result);
        }
      }
      return result;
    }
}
