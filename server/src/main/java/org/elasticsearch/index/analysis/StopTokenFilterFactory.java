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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.suggest.analyzing.SuggestStopFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.util.Set;

public class StopTokenFilterFactory extends AbstractTokenFilterFactory {

    private final CharArraySet stopWords;

    private final boolean ignoreCase;

    private final boolean removeTrailing;

    public StopTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        this.removeTrailing = settings.getAsBoolean("remove_trailing", true);
        this.stopWords = Analysis.parseStopWords(env, settings, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, ignoreCase);
        if (settings.get("enable_position_increments") != null) {
            throw new IllegalArgumentException("enable_position_increments is not supported anymore. Please fix your analysis chain");
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (removeTrailing) {
            return new StopFilter(tokenStream, stopWords);
        } else {
            return new SuggestStopFilter(tokenStream, stopWords);
        }
    }

    public Set<?> stopWords() {
        return stopWords;
    }

    public boolean ignoreCase() {
        return ignoreCase;
    }

}
