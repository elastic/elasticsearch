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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.std40.StandardTokenizer40;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

public class StandardHtmlStripAnalyzer extends StopwordAnalyzerBase {

    /**
     * @deprecated use {@link StandardHtmlStripAnalyzer#StandardHtmlStripAnalyzer(org.apache.lucene.util.Version,
     * org.apache.lucene.analysis.util.CharArraySet)} instead
     */
    @Deprecated
    public StandardHtmlStripAnalyzer() {
        super(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    }

    public StandardHtmlStripAnalyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final Tokenizer src;
        if (getVersion().onOrAfter(Version.LUCENE_4_7_0)) {
            src = new StandardTokenizer();
        } else {
            src = new StandardTokenizer40();
        }
        TokenStream tok = new StandardFilter(src);
        tok = new LowerCaseFilter(tok);
        if (!stopwords.isEmpty()) {
            tok = new StopFilter(tok, stopwords);
        }
        return new TokenStreamComponents(src, tok);
    }

}