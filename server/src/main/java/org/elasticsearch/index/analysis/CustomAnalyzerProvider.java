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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.TextFieldMapper;

import java.util.Map;

import static org.elasticsearch.index.analysis.AnalyzerComponents.createComponents;

/**
 * A custom analyzer that is built out of a single {@link org.apache.lucene.analysis.Tokenizer} and a list
 * of {@link org.apache.lucene.analysis.TokenFilter}s.
 */
public class CustomAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final Settings analyzerSettings;

    private Analyzer customAnalyzer;

    public CustomAnalyzerProvider(IndexSettings indexSettings,
                                  String name, Settings settings) {
        super(indexSettings, name, settings);
        this.analyzerSettings = settings;
    }

    void build(final Map<String, TokenizerFactory> tokenizers,
               final Map<String, CharFilterFactory> charFilters,
               final Map<String, TokenFilterFactory> tokenFilters) {
        customAnalyzer = create(name(), analyzerSettings, tokenizers, charFilters, tokenFilters);
    }

    /**
     * Factory method that either returns a plain {@link ReloadableCustomAnalyzer} if the components used for creation are supporting index
     * and search time use, or a {@link ReloadableCustomAnalyzer} if the components are intended for search time use only.
     */
    private static Analyzer create(String name, Settings analyzerSettings, Map<String, TokenizerFactory> tokenizers,
            Map<String, CharFilterFactory> charFilters,
            Map<String, TokenFilterFactory> tokenFilters) {
        int positionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
        positionIncrementGap = analyzerSettings.getAsInt("position_increment_gap", positionIncrementGap);
        int offsetGap = analyzerSettings.getAsInt("offset_gap", -1);
        AnalyzerComponents components = createComponents(name, analyzerSettings, tokenizers, charFilters, tokenFilters);
        if (components.analysisMode().equals(AnalysisMode.SEARCH_TIME)) {
            return new ReloadableCustomAnalyzer(components, positionIncrementGap, offsetGap);
        } else {
            return new CustomAnalyzer(components.getTokenizerFactory(), components.getCharFilters(),
                    components.getTokenFilters(), positionIncrementGap, offsetGap);
        }
    }

    @Override
    public Analyzer get() {
        return this.customAnalyzer;
    }
}
