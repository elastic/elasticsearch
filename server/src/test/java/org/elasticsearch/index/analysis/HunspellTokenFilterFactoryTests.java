/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class HunspellTokenFilterFactoryTests extends ESTestCase {
    public void testDedup() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.en_US.type", "hunspell")
            .put("index.analysis.filter.en_US.locale", "en_US")
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("en_US");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        assertThat(hunspellTokenFilter.dedup(), is(true));

        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.en_US.type", "hunspell")
            .put("index.analysis.filter.en_US.dedup", false)
            .put("index.analysis.filter.en_US.locale", "en_US")
            .build();

        analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        tokenFilter = analysis.tokenFilter.get("en_US");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        assertThat(hunspellTokenFilter.dedup(), is(false));
    }
}
