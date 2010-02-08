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

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;

/**
 * A custom analyzer that is built out of a single {@link org.apache.lucene.analysis.Tokenizer} and a list
 * of {@link org.apache.lucene.analysis.TokenFilter}s.
 *
 * @author kimchy (Shay Banon)
 */
public class CustomAnalyzerProvider extends AbstractAnalyzerProvider<CustomAnalyzer> {

    private final TokenizerFactory tokenizerFactory;

    private final TokenFilterFactory[] tokenFilterFactories;

    private final CustomAnalyzer customAnalyzer;

    @Inject public CustomAnalyzerProvider(Index index,
                                          Map<String, TokenizerFactoryFactory> tokenizerFactories,
                                          Map<String, TokenFilterFactoryFactory> tokenFilterFactories,
                                          @IndexSettings Settings indexSettings,
                                          @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);
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

        this.customAnalyzer = new CustomAnalyzer(this.tokenizerFactory, this.tokenFilterFactories);
    }

    @Override public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
