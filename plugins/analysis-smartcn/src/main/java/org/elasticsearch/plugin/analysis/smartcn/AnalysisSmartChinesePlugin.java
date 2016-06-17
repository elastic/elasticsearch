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

import org.elasticsearch.index.analysis.SmartChineseAnalyzerProvider;
import org.elasticsearch.index.analysis.SmartChineseNoOpTokenFilterFactory;
import org.elasticsearch.index.analysis.SmartChineseTokenizerTokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.Plugin;

public class AnalysisSmartChinesePlugin extends Plugin {

    public void onModule(AnalysisModule module) {
            module.registerAnalyzer("smartcn", SmartChineseAnalyzerProvider::new);
            module.registerTokenizer("smartcn_tokenizer", SmartChineseTokenizerTokenizerFactory::new);
            // This is an alias to "smartcn_tokenizer"; it's here for backwards compat
        module.registerTokenizer("smartcn_sentence", SmartChineseTokenizerTokenizerFactory::new);
            // This is a noop token filter; it's here for backwards compat before we had "smartcn_tokenizer"
        module.registerTokenFilter("smartcn_word", SmartChineseNoOpTokenFilterFactory::new);
    }
}
