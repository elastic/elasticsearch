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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

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
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_8_0_0, IndexVersion.current())
            )
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
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersions.V_7_6_0)
            )
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
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_8_0_0, IndexVersion.current())
            )
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
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersions.V_7_6_0)
            )
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
            IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersions.V_7_5_2),
            false
        );
        doTestPrebuiltTokenizerDeprecation(
            "edgeNGram",
            "edge_ngram",
            IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersions.V_7_5_2),
            false
        );
        doTestPrebuiltTokenizerDeprecation(
            "nGram",
            "ngram",
            IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.V_7_6_0,
                IndexVersion.max(IndexVersions.V_7_6_0, IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0))
            ),
            true
        );
        doTestPrebuiltTokenizerDeprecation(
            "edgeNGram",
            "edge_ngram",
            IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.V_7_6_0,
                IndexVersion.max(IndexVersions.V_7_6_0, IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0))
            ),
            true
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> doTestPrebuiltTokenizerDeprecation(
                "nGram",
                "ngram",
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_8_0_0, IndexVersion.current()),
                true
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> doTestPrebuiltTokenizerDeprecation(
                "edgeNGram",
                "edge_ngram",
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_8_0_0, IndexVersion.current()),
                true
            )
        );

        // same batch of tests for custom tokenizer definition in the settings
        doTestCustomTokenizerDeprecation(
            "nGram",
            "ngram",
            IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersions.V_7_5_2),
            false
        );
        doTestCustomTokenizerDeprecation(
            "edgeNGram",
            "edge_ngram",
            IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersions.V_7_5_2),
            false
        );
        doTestCustomTokenizerDeprecation(
            "nGram",
            "ngram",
            IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.V_7_6_0,
                IndexVersion.max(IndexVersions.V_7_6_0, IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0))
            ),
            true
        );
        doTestCustomTokenizerDeprecation(
            "edgeNGram",
            "edge_ngram",
            IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.V_7_6_0,
                IndexVersion.max(IndexVersions.V_7_6_0, IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0))
            ),
            true
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> doTestCustomTokenizerDeprecation(
                "nGram",
                "ngram",
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_8_0_0, IndexVersion.current()),
                true
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> doTestCustomTokenizerDeprecation(
                "edgeNGram",
                "edge_ngram",
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_8_0_0, IndexVersion.current()),
                true
            )
        );
    }

    public void doTestPrebuiltTokenizerDeprecation(String deprecatedName, String replacement, IndexVersion version, boolean expectWarning)
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

    public void doTestCustomTokenizerDeprecation(String deprecatedName, String replacement, IndexVersion version, boolean expectWarning)
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
