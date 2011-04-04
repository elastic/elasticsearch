/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * A custom analyzer that is built out of a single {@link org.apache.lucene.analysis.Tokenizer} and a list
 * of {@link org.apache.lucene.analysis.TokenFilter}s.
 *
 * @author kimchy (shay.banon)
 */
public class CustomAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    private final TokenizerFactory tokenizerFactory;

    private final CharFilterFactory[] charFilterFactories;

    private final TokenFilterFactory[] tokenFilterFactories;

    private final CustomAnalyzer customAnalyzer;

    @Inject public CustomAnalyzerProvider(Index index,
                                          Map<String, TokenizerFactoryFactory> tokenizerFactories,
                                          Map<String, CharFilterFactoryFactory> charFilterFactories,
                                          Map<String, TokenFilterFactoryFactory> tokenFilterFactories,
                                          @IndexSettings Settings indexSettings,
                                          @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        String tokenizerName = settings.get("tokenizer");
        if (tokenizerName == null) {
            throw new IllegalArgumentException("Custom Analyzer [" + name + "] must be configured with a tokenizer");
        }
        TokenizerFactoryFactory tokenizerFactoryFactory = tokenizerFactories.get(tokenizerName);
        if (tokenizerFactoryFactory == null) {
            throw new IllegalArgumentException("Custom Analyzer [" + name + "] failed to find tokenizer under name [" + tokenizerName + "]");
        }
        Settings tokenizerSettings = indexSettings.getGroups("index.analysis.tokenizer").get(tokenizerName);
        if (tokenizerSettings == null) {
            tokenizerSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        }
        tokenizerFactory = tokenizerFactoryFactory.create(tokenizerName, tokenizerSettings);

        List<CharFilterFactory> charFilters = newArrayList();
        String[] charFilterNames = settings.getAsArray("char_filter");
        for (String charFilterName : charFilterNames) {
            CharFilterFactoryFactory charFilterFactoryFactory = charFilterFactories.get(charFilterName);
            if (charFilterFactoryFactory == null) {
                throw new IllegalArgumentException("Custom Analyzer [" + name + "] failed to find char filter under name [" + charFilterName + "]");
            }
            Settings charFilterSettings = indexSettings.getGroups("index.analysis.char_filter").get(charFilterName);
            if (charFilterSettings == null) {
                charFilterSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            charFilters.add(charFilterFactoryFactory.create(charFilterName, charFilterSettings));
        }
        this.charFilterFactories = charFilters.toArray(new CharFilterFactory[charFilters.size()]);

        List<TokenFilterFactory> tokenFilters = newArrayList();
        String[] tokenFilterNames = settings.getAsArray("filter");
        for (String tokenFilterName : tokenFilterNames) {
            TokenFilterFactoryFactory tokenFilterFactoryFactory = tokenFilterFactories.get(tokenFilterName);
            if (tokenFilterFactoryFactory == null) {
                throw new IllegalArgumentException("Custom Analyzer [" + name + "] failed to find token filter under name [" + tokenFilterName + "]");
            }
            Settings tokenFilterSettings = indexSettings.getGroups("index.analysis.filter").get(tokenFilterName);
            if (tokenFilterSettings == null) {
                tokenFilterSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            tokenFilters.add(tokenFilterFactoryFactory.create(tokenFilterName, tokenFilterSettings));
        }
        this.tokenFilterFactories = tokenFilters.toArray(new TokenFilterFactory[tokenFilters.size()]);

        this.customAnalyzer = new CustomAnalyzer(this.tokenizerFactory, this.charFilterFactories, this.tokenFilterFactories);
    }

    @Override public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
