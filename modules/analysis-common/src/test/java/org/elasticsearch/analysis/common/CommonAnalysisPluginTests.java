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
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Map;

public class CommonAnalysisPluginTests extends ESTestCase {

    /**
     * Check that the deprecated "nGram" filter throws exception for indices created since 7.0.0 and
     * logs a warning for earlier indices when the filter is used as a custom filter
     */
    public void testNGramFilterInCustomAnalyzerDeprecationError() throws IOException {
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT))
            .put("index.analysis.analyzer.custom_analyzer.type", "custom")
            .put("index.analysis.analyzer.custom_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_analyzer.filter", "my_ngram")
            .put("index.analysis.filter.my_ngram.type", "nGram")
            .build();

        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> createTestAnalysis(IndexSettingsModule.newIndexSettings("index", settings), settings, commonAnalysisPlugin)
            );
            assertEquals(
                "The [nGram] token filter name was deprecated in 6.4 and cannot be used in new indices. "
                    + "Please change the filter name to [ngram] instead.",
                ex.getMessage()
            );
        }

        final Settings settingsPre7 = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_6_0))
            .put("index.analysis.analyzer.custom_analyzer.type", "custom")
            .put("index.analysis.analyzer.custom_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_analyzer.filter", "my_ngram")
            .put("index.analysis.filter.my_ngram.type", "nGram")
            .build();
        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            createTestAnalysis(IndexSettingsModule.newIndexSettings("index", settingsPre7), settingsPre7, commonAnalysisPlugin);
            assertWarnings(
                "The [nGram] token filter name is deprecated and will be removed in a future version. "
                    + "Please change the filter name to [ngram] instead."
            );
        }
    }

    /**
     * Check that the deprecated "edgeNGram" filter throws exception for indices created since 7.0.0 and
     * logs a warning for earlier indices when the filter is used as a custom filter
     */
    public void testEdgeNGramFilterInCustomAnalyzerDeprecationError() throws IOException {
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT))
            .put("index.analysis.analyzer.custom_analyzer.type", "custom")
            .put("index.analysis.analyzer.custom_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_analyzer.filter", "my_ngram")
            .put("index.analysis.filter.my_ngram.type", "edgeNGram")
            .build();

        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> createTestAnalysis(IndexSettingsModule.newIndexSettings("index", settings), settings, commonAnalysisPlugin)
            );
            assertEquals(
                "The [edgeNGram] token filter name was deprecated in 6.4 and cannot be used in new indices. "
                    + "Please change the filter name to [edge_ngram] instead.",
                ex.getMessage()
            );
        }

        final Settings settingsPre7 = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_6_0))
            .put("index.analysis.analyzer.custom_analyzer.type", "custom")
            .put("index.analysis.analyzer.custom_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_analyzer.filter", "my_ngram")
            .put("index.analysis.filter.my_ngram.type", "edgeNGram")
            .build();

        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            createTestAnalysis(IndexSettingsModule.newIndexSettings("index", settingsPre7), settingsPre7, commonAnalysisPlugin);
            assertWarnings(
                "The [edgeNGram] token filter name is deprecated and will be removed in a future version. "
                    + "Please change the filter name to [edge_ngram] instead."
            );
        }
    }

    /**
     * Check that we log a deprecation warning for "nGram" and "edgeNGram" tokenizer names with 7.6 and
     * disallow usages for indices created after 8.0
     */
    public void testNGramTokenizerDeprecation() throws IOException {
        // tests for prebuilt tokenizer
        doTestPrebuiltTokenizerDeprecation(
            "nGram",
            "ngram",
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_2),
            false
        );
        doTestPrebuiltTokenizerDeprecation(
            "edgeNGram",
            "edge_ngram",
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_2),
            false
        );
        doTestPrebuiltTokenizerDeprecation(
            "nGram",
            "ngram",
            VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_6_0,
                Version.max(Version.V_7_6_0, VersionUtils.getPreviousVersion(Version.V_8_0_0))
            ),
            true
        );
        doTestPrebuiltTokenizerDeprecation(
            "edgeNGram",
            "edge_ngram",
            VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_6_0,
                Version.max(Version.V_7_6_0, VersionUtils.getPreviousVersion(Version.V_8_0_0))
            ),
            true
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> doTestPrebuiltTokenizerDeprecation(
                "nGram",
                "ngram",
                VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT),
                true
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> doTestPrebuiltTokenizerDeprecation(
                "edgeNGram",
                "edge_ngram",
                VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT),
                true
            )
        );

        // same batch of tests for custom tokenizer definition in the settings
        doTestCustomTokenizerDeprecation(
            "nGram",
            "ngram",
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_2),
            false
        );
        doTestCustomTokenizerDeprecation(
            "edgeNGram",
            "edge_ngram",
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_2),
            false
        );
        doTestCustomTokenizerDeprecation(
            "nGram",
            "ngram",
            VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_6_0,
                Version.max(Version.V_7_6_0, VersionUtils.getPreviousVersion(Version.V_8_0_0))
            ),
            true
        );
        doTestCustomTokenizerDeprecation(
            "edgeNGram",
            "edge_ngram",
            VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_6_0,
                Version.max(Version.V_7_6_0, VersionUtils.getPreviousVersion(Version.V_8_0_0))
            ),
            true
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> doTestCustomTokenizerDeprecation(
                "nGram",
                "ngram",
                VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT),
                true
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> doTestCustomTokenizerDeprecation(
                "edgeNGram",
                "edge_ngram",
                VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT),
                true
            )
        );
    }

    public void doTestPrebuiltTokenizerDeprecation(String deprecatedName, String replacement, Version version, boolean expectWarning)
        throws IOException {
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .build();

        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            Map<String, TokenizerFactory> tokenizers = createTestAnalysis(
                IndexSettingsModule.newIndexSettings("index", settings),
                settings,
                commonAnalysisPlugin
            ).tokenizer;
            TokenizerFactory tokenizerFactory = tokenizers.get(deprecatedName);

            Tokenizer tokenizer = tokenizerFactory.create();
            assertNotNull(tokenizer);
            if (expectWarning) {
                assertWarnings(
                    "The ["
                        + deprecatedName
                        + "] tokenizer name is deprecated and will be removed in a future version. "
                        + "Please change the tokenizer name to ["
                        + replacement
                        + "] instead."
                );
            }
        }
    }

    public void doTestCustomTokenizerDeprecation(String deprecatedName, String replacement, Version version, boolean expectWarning)
        throws IOException {
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put("index.analysis.analyzer.custom_analyzer.type", "custom")
            .put("index.analysis.analyzer.custom_analyzer.tokenizer", "my_tokenizer")
            .put("index.analysis.tokenizer.my_tokenizer.type", deprecatedName)
            .build();

        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            createTestAnalysis(IndexSettingsModule.newIndexSettings("index", settings), settings, commonAnalysisPlugin);

            if (expectWarning) {
                assertWarnings(
                    "The ["
                        + deprecatedName
                        + "] tokenizer name is deprecated and will be removed in a future version. "
                        + "Please change the tokenizer name to ["
                        + replacement
                        + "] instead."
                );
            }
        }
    }
}
