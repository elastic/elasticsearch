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

package org.elasticsearch.analysis.common;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
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
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_6_3_0, Version.CURRENT))
                .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            Map<String, CharFilterFactory> charFilters = createTestAnalysis(idxSettings, settings, commonAnalysisPlugin).charFilter;
            CharFilterFactory charFilterFactory = charFilters.get("htmlStrip");
            assertNotNull(charFilterFactory.create(new StringReader("input")));
            assertWarnings("The [htmpStrip] char filter name is deprecated and will be removed in a future version. "
                    + "Please change the filter name to [html_strip] instead.");
        }
    }

    /**
     * Check that the deprecated name "htmlStrip" does NOT issues a deprecation warning for indices created before 6.3.0
     */
    public void testNoDeprecationWarningPre6_3() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(IndexMetaData.SETTING_VERSION_CREATED,
                        VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.V_6_2_4))
                .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try (CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin()) {
            Map<String, CharFilterFactory> charFilters = createTestAnalysis(idxSettings, settings, commonAnalysisPlugin).charFilter;
            CharFilterFactory charFilterFactory = charFilters.get("htmlStrip");
            assertNotNull(charFilterFactory.create(new StringReader("")));
        }
    }
}
