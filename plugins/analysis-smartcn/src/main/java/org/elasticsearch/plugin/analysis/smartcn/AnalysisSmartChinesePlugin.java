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

package org.elasticsearch.plugin.analysis.smartcn;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.SmartChineseAnalyzerProvider;
import org.elasticsearch.index.analysis.SmartChineseNoOpTokenFilterFactory;
import org.elasticsearch.index.analysis.SmartChineseStopTokenFilterFactory;
import org.elasticsearch.index.analysis.SmartChineseTokenizerTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class AnalysisSmartChinesePlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters = new HashMap<>();
        tokenFilters.put("smartcn_stop", SmartChineseStopTokenFilterFactory::new);
        // TODO: deprecate and remove, this is a noop token filter; it's here for backwards compat before we had "smartcn_tokenizer"
        tokenFilters.put("smartcn_word", SmartChineseNoOpTokenFilterFactory::new);
        return tokenFilters;
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        Map<String, AnalysisProvider<TokenizerFactory>> extra = new HashMap<>();
        extra.put("smartcn_tokenizer", SmartChineseTokenizerTokenizerFactory::new);
        // TODO: deprecate and remove, this is an alias to "smartcn_tokenizer"; it's here for backwards compat
        extra.put("smartcn_sentence", SmartChineseTokenizerTokenizerFactory::new);
        return extra;
    }

    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("smartcn", SmartChineseAnalyzerProvider::new);
    }
}
