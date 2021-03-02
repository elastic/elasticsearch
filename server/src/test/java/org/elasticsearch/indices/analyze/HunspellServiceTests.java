/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.analyze;

import org.apache.lucene.analysis.hunspell.Dictionary;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.analysis.HunspellService;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.indices.analysis.HunspellService.HUNSPELL_IGNORE_CASE;
import static org.elasticsearch.indices.analysis.HunspellService.HUNSPELL_LAZY_LOAD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.notNullValue;

public class HunspellServiceTests extends ESTestCase {
    public void testLocaleDirectoryWithNodeLevelConfig() throws Exception {
        Settings settings = Settings.builder()
                .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
                .put(HUNSPELL_IGNORE_CASE.getKey(), true)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();

        final Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        Dictionary dictionary = new HunspellService(settings, environment, emptyMap()).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
        assertTrue(dictionary.getIgnoreCase());
    }

    public void testLocaleDirectoryWithLocaleSpecificConfig() throws Exception {
        Settings settings = Settings.builder()
                .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
                .put(HUNSPELL_IGNORE_CASE.getKey(), true)
                .put("indices.analysis.hunspell.dictionary.en_US.strict_affix_parsing", false)
                .put("indices.analysis.hunspell.dictionary.en_US.ignore_case", false)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();

        final Path configPath = getDataPath("/indices/analyze/conf_dir");
        final Environment environment = new Environment(settings, configPath);
        Dictionary dictionary = new HunspellService(settings, environment, emptyMap()).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
        assertFalse(dictionary.getIgnoreCase());

        // testing that dictionary specific settings override node level settings
        dictionary = new HunspellService(settings, new Environment(settings, configPath), emptyMap()).getDictionary("en_US_custom");
        assertThat(dictionary, notNullValue());
        assertTrue(dictionary.getIgnoreCase());
    }

    public void testDicWithNoAff() throws Exception {
        Settings settings = Settings.builder()
                .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();

        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> {
                    final Environment environment = new Environment(settings, getDataPath("/indices/analyze/no_aff_conf_dir"));
                    new HunspellService(settings, environment, emptyMap()).getDictionary("en_US");
                });
        assertEquals("failed to load hunspell dictionary for locale: en_US", e.getMessage());
        assertThat(e.getCause(), hasToString(containsString("Missing affix file")));
    }

    public void testDicWithTwoAffs() throws Exception {
        Settings settings = Settings.builder()
                .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();

        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> {
                    final Environment environment = new Environment(settings, getDataPath("/indices/analyze/two_aff_conf_dir"));
                    new HunspellService(settings, environment, emptyMap()).getDictionary("en_US");
                });
        assertEquals("failed to load hunspell dictionary for locale: en_US", e.getMessage());
        assertThat(e.getCause(), hasToString(containsString("Too many affix files")));
    }
}
