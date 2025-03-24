/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;

/**
 * Verifies the behavior of Romanian analyzer.
 */
public class RomanianAnalyzerTests extends ESTokenStreamTestCase {

    public void testRomanianAnalyzerPostLucene10() throws IOException {
        IndexVersion postLucene10Version = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.UPGRADE_TO_LUCENE_10_0_0,
            IndexVersion.current()
        );
        Settings settings = ESTestCase.indexSettings(1, 1)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, postLucene10Version)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        Environment environment = new Environment(settings, null);

        RomanianAnalyzerProvider romanianAnalyzerProvider = new RomanianAnalyzerProvider(
            idxSettings,
            environment,
            "my-analyzer",
            Settings.EMPTY
        );
        Analyzer analyzer = romanianAnalyzerProvider.get();
        assertAnalyzesTo(analyzer, "absenţa", new String[] { "absenț" });
        assertAnalyzesTo(analyzer, "cunoştinţă", new String[] { "cunoștinț" });
    }

    public void testRomanianAnalyzerPreLucene10() throws IOException {
        IndexVersion preLucene10Version = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersionUtils.getLowestReadCompatibleVersion(),
            IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
        );
        Settings settings = ESTestCase.indexSettings(1, 1)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, preLucene10Version)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        Environment environment = new Environment(settings, null);

        RomanianAnalyzerProvider romanianAnalyzerProvider = new RomanianAnalyzerProvider(
            idxSettings,
            environment,
            "my-analyzer",
            Settings.EMPTY
        );
        Analyzer analyzer = romanianAnalyzerProvider.get();
        assertAnalyzesTo(analyzer, "absenţa", new String[] { "absenţ" });
        assertAnalyzesTo(analyzer, "cunoştinţă", new String[] { "cunoştinţ" });
    }
}
