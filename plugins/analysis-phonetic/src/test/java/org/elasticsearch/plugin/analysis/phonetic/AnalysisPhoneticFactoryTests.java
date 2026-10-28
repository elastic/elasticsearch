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
import java.util.List;
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

    @Override
    protected Map<String, FactorySettings> tokenFilterSettings() {
        // max_code_len and max_variations are each read by only some encoders, so they are probed against
        // those encoders' own bases rather than the beider_morse one.
        return Map.of(
            "phonetic",
            settings(Map.of("encoder", "beider_morse")).ignored("encoder", "bm", "BeiderMorse")
                .affects(
                    "encoder",
                    "metaphone",
                    "soundex",
                    "caverphone1",
                    "caverphone2",
                    "refined_soundex",
                    "cologne",
                    "double_metaphone",
                    "koelnerphonetik",
                    "haasephonetik",
                    "nysiis",
                    "daitch_mokotoff"
                )
                .affects("languageset", List.of("english"))
                .affects("rule_type", "exact")
                .affects("name_type", "ashkenazi")
                .affects("replace", "false")
                .alsoWith(settings(Map.of("encoder", "double_metaphone")).affects("max_code_len", 6))
                .alsoWith(settings(Map.of("encoder", "koelnerphonetik")).affects("max_variations", 8))
                .alsoWith(
                    settings(Map.of("encoder", "haasephonetik")).affects("max_variations", 8)
                        .gatedAt(IndexVersions.HAASE_PHONETIK_ANCHORED_VARIATIONS)
                )
        );
    }

    public void testDoubleMetaphoneMaxCodeLenIsKeyed() {
        Object fourChars = phoneticKey(Settings.builder().put("encoder", "double_metaphone").build());
        assertEquals(fourChars, phoneticKey(Settings.builder().put("encoder", "doubleMetaphone").put("max_code_len", 4).build()));
        assertNotEquals(fourChars, phoneticKey(Settings.builder().put("encoder", "double_metaphone").put("max_code_len", 6).build()));
    }

    /** caverphone is an alias for caverphone2, and the language set is used as a set. */
    public void testEquivalentSettingsShareKey() {
        assertEquals(
            phoneticKey(Settings.builder().put("encoder", "caverphone2").build()),
            phoneticKey(Settings.builder().put("encoder", "caverphone").build())
        );
        assertEquals(
            phoneticKey(Settings.builder().put("encoder", "bm").putList("languageset", "english", "french").build()),
            phoneticKey(Settings.builder().put("encoder", "bm").putList("languageset", "french", "english", "french").build())
        );
    }

    private Object phoneticKey(Settings settings) {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
        return new PhoneticTokenFilterFactory(idxSettings, null, "phonetic", settings).sharingKey();
    }

    public void testDisallowedWithSynonyms() throws IOException {

        AnalysisPhoneticPlugin plugin = new AnalysisPhoneticPlugin();

        Settings settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersion.current())
            )
            .put("path.home", createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        TokenFilterFactory tff = plugin.getTokenFilters().get("phonetic").get(idxSettings, null, "phonetic", settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, tff::getSynonymFilter);
        assertEquals("Token filter [phonetic] cannot be used to parse synonyms", e.getMessage());
    }

}
