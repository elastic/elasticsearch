/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.apache.lucene.tests.analysis.MockTokenizer;
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
        return singletonMap(
            "keyword",
            (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                name,
                () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
            )
        );
    }
}
