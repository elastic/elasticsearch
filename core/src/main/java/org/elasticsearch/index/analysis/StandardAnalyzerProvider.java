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

import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.elasticsearch.Version;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public class StandardAnalyzerProvider extends AbstractIndexAnalyzerProvider<StandardAnalyzer> {

    private final StandardAnalyzer standardAnalyzer;
    private final Version esVersion;

    @Inject
    public StandardAnalyzerProvider(Index index, @IndexSettings Settings indexSettings, Environment env, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.esVersion = Version.indexCreated(indexSettings);
        final CharArraySet defaultStopwords;
        if (esVersion.onOrAfter(Version.V_1_0_0_Beta1)) {
            defaultStopwords = CharArraySet.EMPTY_SET;
        } else {
            defaultStopwords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
        }

        CharArraySet stopWords = Analysis.parseStopWords(env, settings, defaultStopwords);
        int maxTokenLength = settings.getAsInt("max_token_length", StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
        standardAnalyzer = new StandardAnalyzer(stopWords);
        standardAnalyzer.setVersion(version);
        standardAnalyzer.setMaxTokenLength(maxTokenLength);
    }

    @Override
    public StandardAnalyzer get() {
        return this.standardAnalyzer;
    }
}
