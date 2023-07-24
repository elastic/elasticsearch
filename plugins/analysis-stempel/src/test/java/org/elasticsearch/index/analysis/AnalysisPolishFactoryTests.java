/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.pl.PolishStemTokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;
import org.elasticsearch.plugin.analysis.stempel.AnalysisStempelPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AnalysisPolishFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisPolishFactoryTests() {
        super(new AnalysisStempelPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("stempelpolishstem", PolishStemTokenFilterFactory.class);
        return filters;
    }

    public void testThreadSafety() throws IOException {
        // TODO: is this the right boilerplate? I forked this out of TransportAnalyzeAction.java:
        Settings settings = indexSettings(Version.CURRENT, 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        IndexMetadata metadata = IndexMetadata.builder(IndexMetadata.INDEX_UUID_NA_VALUE).settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        testThreadSafety(new PolishStemTokenFilterFactory(indexSettings, environment, "stempelpolishstem", settings));
    }

    // TODO: move to AnalysisFactoryTestCase so we can more easily test thread safety for all factories
    private void testThreadSafety(TokenFilterFactory factory) throws IOException {
        final Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new MockTokenizer();
                return new TokenStreamComponents(tokenizer, factory.create(tokenizer));
            }
        };
        BaseTokenStreamTestCase.checkRandomData(random(), analyzer, 100);
    }
}
