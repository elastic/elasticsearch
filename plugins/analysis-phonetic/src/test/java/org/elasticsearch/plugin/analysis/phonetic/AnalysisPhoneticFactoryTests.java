/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.phonetic;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

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
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersion.current())
            )
            .put("path.home", createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        TokenFilterFactory tff = plugin.getTokenFilters().get("phonetic").get(idxSettings, null, "phonetic", settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, tff::getSynonymFilter);
        assertEquals("Token filter [phonetic] cannot be used to parse synonyms", e.getMessage());
    }

}
