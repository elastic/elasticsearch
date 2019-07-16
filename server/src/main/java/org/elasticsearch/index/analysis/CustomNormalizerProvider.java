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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A custom normalizer that is built out of a char and token filters. On the
 * contrary to analyzers, it does not support tokenizers and only supports a
 * subset of char and token filters.
 */
public final class CustomNormalizerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    private final Settings analyzerSettings;

    private CustomAnalyzer customAnalyzer;

    public CustomNormalizerProvider(IndexSettings indexSettings,
                                    String name, Settings settings) {
        super(indexSettings, name, settings);
        this.analyzerSettings = settings;
    }

    public void build(final TokenizerFactory tokenizerFactory, final Map<String, CharFilterFactory> charFilters,
            final Map<String, TokenFilterFactory> tokenFilters) {
        if (analyzerSettings.get("tokenizer") != null) {
            throw new IllegalArgumentException("Custom normalizer [" + name() + "] cannot configure a tokenizer");
        }

        List<String> charFilterNames = analyzerSettings.getAsList("char_filter");
        List<CharFilterFactory> charFiltersList = new ArrayList<>(charFilterNames.size());
        for (String charFilterName : charFilterNames) {
            CharFilterFactory charFilter = charFilters.get(charFilterName);
            if (charFilter == null) {
                throw new IllegalArgumentException("Custom normalizer [" + name() + "] failed to find char_filter under name ["
                        + charFilterName + "]");
            }
            if (charFilter instanceof NormalizingCharFilterFactory == false) {
                throw new IllegalArgumentException("Custom normalizer [" + name() + "] may not use char filter ["
                        + charFilterName + "]");
            }
            charFiltersList.add(charFilter);
        }

        List<String> tokenFilterNames = analyzerSettings.getAsList("filter");
        List<TokenFilterFactory> tokenFilterList = new ArrayList<>(tokenFilterNames.size());
        for (String tokenFilterName : tokenFilterNames) {
            TokenFilterFactory tokenFilter = tokenFilters.get(tokenFilterName);
            if (tokenFilter == null) {
                throw new IllegalArgumentException("Custom Analyzer [" + name() + "] failed to find filter under name ["
                        + tokenFilterName + "]");
            }
            if (tokenFilter instanceof NormalizingTokenFilterFactory == false) {
                throw new IllegalArgumentException("Custom normalizer [" + name() + "] may not use filter [" + tokenFilterName + "]");
            }
            tokenFilterList.add(tokenFilter);
        }

        this.customAnalyzer = new CustomAnalyzer(
                tokenizerFactory,
                charFiltersList.toArray(new CharFilterFactory[charFiltersList.size()]),
                tokenFilterList.toArray(new TokenFilterFactory[tokenFilterList.size()])
        );
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
