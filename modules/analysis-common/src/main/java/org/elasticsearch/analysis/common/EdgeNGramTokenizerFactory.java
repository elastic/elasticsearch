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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

import static org.elasticsearch.analysis.common.NGramTokenizerFactory.parseTokenChars;

public class EdgeNGramTokenizerFactory extends AbstractTokenizerFactory {

    private final int minGram;
    private final int maxGram;
    private final CharMatcher matcher;

    EdgeNGramTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, settings, name);
        this.minGram = settings.getAsInt("min_gram", NGramTokenizer.DEFAULT_MIN_NGRAM_SIZE);
        this.maxGram = settings.getAsInt("max_gram", NGramTokenizer.DEFAULT_MAX_NGRAM_SIZE);
        this.matcher = parseTokenChars(settings);
    }

    @Override
    public Tokenizer create() {
        if (matcher == null) {
            return new EdgeNGramTokenizer(minGram, maxGram);
        } else {
            return new EdgeNGramTokenizer(minGram, maxGram) {
                @Override
                protected boolean isTokenChar(int chr) {
                    return matcher.isTokenChar(chr);
                }
            };
        }
    }
}
