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

package org.elasticsearch.index.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;
import org.elasticsearch.plugin.analysis.AnalysisPhoneticPlugin;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AnalysisPhoneticFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisPhoneticFactoryTests() {
        super(new AnalysisPhoneticPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("beidermorse", PhoneticTokenFilterFactory.class);
        filters.put("doublemetaphone", PhoneticTokenFilterFactory.class);
        filters.put("phonetic", PhoneticTokenFilterFactory.class);
        return filters;
    }

    public void testDisallowedWithSynonyms() throws IOException {

        AnalysisPhoneticPlugin plugin = new AnalysisPhoneticPlugin();

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.CURRENT))
            .put("path.home", createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        TokenFilterFactory tff
            = plugin.getTokenFilters().get("phonetic").get(idxSettings, null, "phonetic", settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, tff::getSynonymFilter);
        assertEquals("Token filter [phonetic] cannot be used to parse synonyms", e.getMessage());
    }

}
