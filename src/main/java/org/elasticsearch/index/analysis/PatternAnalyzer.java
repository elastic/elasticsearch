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
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;

import java.util.regex.Pattern;

/** Simple regex-based analyzer based on PatternTokenizer + lowercase + stopwords */
// nocommit: does this have tests?
public final class PatternAnalyzer extends Analyzer {
    private final Pattern pattern;
    private final boolean lowercase;
    private final CharArraySet stopWords;

    public PatternAnalyzer(Pattern pattern, boolean lowercase, CharArraySet stopWords) {
        this.pattern = pattern;
        this.lowercase = lowercase;
        this.stopWords = stopWords;
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        final TokenStreamComponents source = new TokenStreamComponents(new PatternTokenizer(pattern, -1));
        TokenStream result = null;
        if (lowercase) {
             result = new LowerCaseFilter(source.getTokenStream());
        }
        result = new StopFilter((result == null) ? source.getTokenStream() : result, stopWords);
        return new TokenStreamComponents(source.getTokenizer(), result);
    }
}