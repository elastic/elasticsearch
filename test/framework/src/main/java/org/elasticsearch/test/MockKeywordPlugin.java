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
package org.elasticsearch.test;

import org.apache.lucene.analysis.MockTokenizer;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Some tests rely on the keyword tokenizer, but this tokenizer isn't part of lucene-core and therefor not available
 * in some modules. What this test plugin does, is use the mock tokenizer and advertise that as the keyword tokenizer.
 *
 * Most tests that need this test plugin use normalizers. When normalizers are constructed they try to resolve the
 * keyword tokenizer, but if the keyword tokenizer isn't available then constructing normalizers will fail.
 */
public class MockKeywordPlugin extends Plugin implements AnalysisPlugin {

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return singletonMap("keyword", (indexSettings, environment, name, settings) ->
            TokenizerFactory.newFactory(name, () -> new MockTokenizer(MockTokenizer.KEYWORD, false)));
    }
}
