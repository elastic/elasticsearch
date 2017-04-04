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
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.FingerprintFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/** OpenRefine Fingerprinting, which uses a Standard tokenizer and lowercase + stop + fingerprint + asciifolding filters */
public final class FingerprintAnalyzer extends Analyzer {
    private final char separator;
    private final int maxOutputSize;
    private final CharArraySet stopWords;

    public FingerprintAnalyzer(CharArraySet stopWords, char separator, int maxOutputSize) {
        this.separator = separator;
        this.maxOutputSize = maxOutputSize;
        this.stopWords = stopWords;
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        final Tokenizer tokenizer = new StandardTokenizer();
        TokenStream stream = tokenizer;
        stream = new LowerCaseFilter(stream);
        stream = new ASCIIFoldingFilter(stream, false);
        stream = new StopFilter(stream, stopWords);
        stream = new FingerprintFilter(stream, maxOutputSize, separator);
        return new TokenStreamComponents(tokenizer, stream);
    }
}
