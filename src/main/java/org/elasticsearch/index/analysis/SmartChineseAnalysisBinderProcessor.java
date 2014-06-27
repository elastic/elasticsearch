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

/**
 */
public class SmartChineseAnalysisBinderProcessor extends AnalysisModule.AnalysisBinderProcessor {

    @Override
    public void processAnalyzers(AnalyzersBindings analyzersBindings) {
        analyzersBindings.processAnalyzer("smartcn", SmartChineseAnalyzerProvider.class);
    }

    @Override
    public void processTokenizers(TokenizersBindings tokenizersBindings) {
        // TODO Remove it in 2.3.0 (was deprecated: see https://github.com/elasticsearch/elasticsearch-analysis-smartcn/issues/22)
        tokenizersBindings.processTokenizer("smartcn_sentence", SmartChineseSentenceTokenizerFactory.class);
        tokenizersBindings.processTokenizer("smartcn_tokenizer", SmartChineseTokenizerTokenizerFactory.class);
    }

    @Override
    public void processTokenFilters(TokenFiltersBindings tokenFiltersBindings) {
        // TODO Remove it in 2.3.0 (was deprecated: see https://github.com/elasticsearch/elasticsearch-analysis-smartcn/issues/22)
        tokenFiltersBindings.processTokenFilter("smartcn_word", SmartChineseWordTokenFilterFactory.class);
    }
}
