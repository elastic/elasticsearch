/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class EdgeNGramTokenizerTests extends ESTokenStreamTestCase {

    private IndexAnalyzers buildAnalyzers(IndexVersion version, String tokenizer) throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put("index.analysis.analyzer.my_analyzer.tokenizer", tokenizer)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        return new AnalysisModule(
            TestEnvironment.newEnvironment(settings),
            Collections.singletonList(new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry().build(IndexCreationContext.CREATE_INDEX, idxSettings);
    }

    public void testPreConfiguredTokenizer() throws IOException {
        // Before 7.3 we return ngrams of length 1 only
        {
            IndexVersion version = IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.MINIMUM_READONLY_COMPATIBLE,
                IndexVersionUtils.getPreviousVersion(IndexVersions.V_7_3_0)
            );
            try (IndexAnalyzers indexAnalyzers = buildAnalyzers(version, "edge_ngram")) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t" });
            }
        }

        // Check deprecated name as well
        {
            IndexVersion version = IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.MINIMUM_READONLY_COMPATIBLE,
                IndexVersionUtils.getPreviousVersion(IndexVersions.V_7_3_0)
            );
            try (IndexAnalyzers indexAnalyzers = buildAnalyzers(version, "edgeNGram")) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t" });
            }
        }

        // Afterwards, we return ngrams of length 1 and 2, to match the default factory settings
        {
            try (IndexAnalyzers indexAnalyzers = buildAnalyzers(IndexVersion.current(), "edge_ngram")) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t", "te" });
            }
        }

        // Check deprecated name as well, needs version before 8.0 because throws IAE after that
        {
            IndexVersion version = IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.V_7_3_0,
                IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0)
            );
            try (IndexAnalyzers indexAnalyzers = buildAnalyzers(version, "edge_ngram")) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t", "te" });
            }
        }
    }

    public void testCustomTokenChars() throws IOException {
        final Index index = new Index("test", "_na_");
        final String name = "engr";
        final Settings indexSettings = newAnalysisSettingsBuilder().put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 2).build();

        final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2)
            .put("max_gram", 3)
            .putList("token_chars", "letter", "custom")
            .put("custom_token_chars", "_-")
            .build();
        Tokenizer tokenizer = new EdgeNGramTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            name,
            settings
        ).create();
        tokenizer.setReader(new StringReader("Abc -gh _jk =lm"));
        assertTokenStreamContents(tokenizer, new String[] { "Ab", "Abc", "-g", "-gh", "_j", "_jk", "lm" });
    }

}
