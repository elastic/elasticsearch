/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Set;

/**
 * Creates a SnowballAnalyzer initialized with stopwords and Snowball filter. Only
 * supports Dutch, English (default), French, German and German2 where stopwords
 * are readily available. For other languages available with the Lucene Snowball
 * Stemmer, use them directly with the SnowballFilter and a CustomAnalyzer.
 * Configuration of language is done with the "language" attribute or the analyzer.
 * Also supports additional stopwords via "stopwords" attribute
 *
 * The SnowballAnalyzer comes with a StandardFilter, LowerCaseFilter, StopFilter
 * and the SnowballFilter.
 *
 * @author kimchy (Shay Banon)
 * @author harryf (Harry Fuecks)
 */
public class SnowballAnalyzerProvider extends AbstractIndexAnalyzerProvider<SnowballAnalyzer> {

    private static final ImmutableMap<String, Set<?>> defaultLanguageStopwords = MapBuilder.<String, Set<?>>newMapBuilder()
            .put("English", StopAnalyzer.ENGLISH_STOP_WORDS_SET)
            .put("Dutch", DutchAnalyzer.getDefaultStopSet())
            .put("German", GermanAnalyzer.getDefaultStopSet())
            .put("German2", GermanAnalyzer.getDefaultStopSet())
            .put("French", FrenchAnalyzer.getDefaultStopSet())
            .immutableMap();

    private final SnowballAnalyzer analyzer;

    @Inject public SnowballAnalyzerProvider(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);

        String language = settings.get("language", settings.get("name", "English"));
        Set<?> defaultStopwords = defaultLanguageStopwords.containsKey(language) ? defaultLanguageStopwords.get(language) : ImmutableSet.<Set<?>>of();
        Set<?> stopWords = Analysis.parseStopWords(settings, defaultStopwords);

        analyzer = new SnowballAnalyzer(version, language, stopWords);
    }

    @Override public SnowballAnalyzer get() {
        return this.analyzer;
    }
}