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

package org.elasticsearch.indices.analysis.smartcn;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.HMMChineseTokenizer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.*;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.io.Reader;

/**
 * Registers indices level analysis components so, if not explicitly configured, will be shared
 * among all indices.
 */
public class SmartChineseIndicesAnalysis extends AbstractComponent {

    @Inject
    public SmartChineseIndicesAnalysis(Settings settings, IndicesAnalysisService indicesAnalysisService) {
        super(settings);

        // Register smartcn analyzer
        indicesAnalysisService.analyzerProviderFactories().put("smartcn", new PreBuiltAnalyzerProviderFactory("smartcn", AnalyzerScope.INDICES, new SmartChineseAnalyzer()));

        // Register smartcn_tokenizer tokenizer
        indicesAnalysisService.tokenizerFactories().put("smartcn_tokenizer", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "smartcn_tokenizer";
            }

            @Override
            public Tokenizer create() {
                return new HMMChineseTokenizer();
            }
        }));

        // Register smartcn_sentence tokenizer -- for backwards compat an alias to smartcn_tokenizer
        indicesAnalysisService.tokenizerFactories().put("smartcn_sentence", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "smartcn_sentence";
            }

            @Override
            public Tokenizer create() {
                return new HMMChineseTokenizer();
            }
        }));

        // Register smartcn_word token filter -- noop
        indicesAnalysisService.tokenFilterFactories().put("smartcn_word", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "smartcn_word";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }
        }));
    }
}
