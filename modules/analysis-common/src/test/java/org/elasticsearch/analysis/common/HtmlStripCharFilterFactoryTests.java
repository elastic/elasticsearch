/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

public class HtmlStripCharFilterFactoryTests extends ESTestCase {

    /**
     * Check that the deprecated name "htmlStrip" issues a deprecation warning for indices created since 6.3.0
     */
    public void testDeprecationWarning() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_6_3_0, Version.CURRENT))
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            Map<String, CharFilterFactory> charFilters = createTestAnalysis(idxSettings, settings, commonAnalysisPlugin).charFilter;
            CharFilterFactory charFilterFactory = charFilters.get("htmlStrip");
            assertNotNull(charFilterFactory.create(new StringReader("input")));
            assertWarnings(
                "The [htmpStrip] char filter name is deprecated and will be removed in a future version. "
                    + "Please change the filter name to [html_strip] instead."
            );
        }
    }

    /**
     * Check that the deprecated name "htmlStrip" does NOT issues a deprecation warning for indices created before 6.3.0
     */
    public void testNoDeprecationWarningPre6_3() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.V_6_2_4))
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            Map<String, CharFilterFactory> charFilters = createTestAnalysis(idxSettings, settings, commonAnalysisPlugin).charFilter;
            CharFilterFactory charFilterFactory = charFilters.get("htmlStrip");
            assertNotNull(charFilterFactory.create(new StringReader("")));
        }
    }
}
