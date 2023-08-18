/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class EdgeNGramTokenizerTests extends ESTokenStreamTestCase {

    private IndexAnalyzers buildAnalyzers(Version version, String tokenizer) throws IOException {
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
            Version version = VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_0_0,
                VersionUtils.getPreviousVersion(Version.V_7_3_0)
            );
            try (IndexAnalyzers indexAnalyzers = buildAnalyzers(version, "edge_ngram")) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t" });
            }
        }

        // Check deprecated name as well
        {
            Version version = VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_0_0,
                VersionUtils.getPreviousVersion(Version.V_7_3_0)
            );
            try (IndexAnalyzers indexAnalyzers = buildAnalyzers(version, "edgeNGram")) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t" });
            }
        }

        // Afterwards, we return ngrams of length 1 and 2, to match the default factory settings
        {
            try (IndexAnalyzers indexAnalyzers = buildAnalyzers(Version.CURRENT, "edge_ngram")) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t", "te" });
            }
        }

        // Check deprecated name as well, needs version before 8.0 because throws IAE after that
        {
            try (
                IndexAnalyzers indexAnalyzers = buildAnalyzers(
                    VersionUtils.randomVersionBetween(random(), Version.V_7_3_0, VersionUtils.getPreviousVersion(Version.V_8_0_0)),
                    "edgeNGram"
                )
            ) {
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
