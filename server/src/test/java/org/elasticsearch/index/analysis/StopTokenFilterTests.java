/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.search.suggest.analyzing.SuggestStopFilter;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class StopTokenFilterTests extends ESTokenStreamTestCase {
    public void testPositionIncrementSetting() throws IOException {
        boolean versionSet = false;
        Builder builder = Settings.builder()
            .put("index.analysis.filter.my_stop.type", "stop")
            .put("index.analysis.filter.my_stop.enable_position_increments", false);
        if (random().nextBoolean()) {
            builder.put("index.analysis.filter.my_stop.version", "5.0");
            versionSet = true;
        }
        builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString());
        Settings settings = builder.build();
        try {
            AnalysisTestsHelper.createTestAnalysisFromSettings(settings);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("enable_position_increments is not supported anymore"));
        }
        if (versionSet) {
            assertWarnings("Setting [version] on analysis component [my_stop] has no effect and is deprecated");
        }
    }

    public void testCorrectPositionIncrementSetting() throws IOException {
        Builder builder = Settings.builder().put("index.analysis.filter.my_stop.type", "stop");
        boolean versionSet = false;
        if (random().nextBoolean()) {
            builder.put("index.analysis.filter.my_stop.version", Version.LATEST);
            versionSet = true;
        }
        builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString());
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(builder.build());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_stop");
        assertThat(tokenFilter, instanceOf(StopTokenFilterFactory.class));
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo bar"));
        TokenStream create = tokenFilter.create(tokenizer);
        assertThat(create, instanceOf(StopFilter.class));
        if (versionSet) {
            assertWarnings("Setting [version] on analysis component [my_stop] has no effect and is deprecated");
        }
    }

    public void testThatSuggestStopFilterWorks() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.my_stop.type", "stop")
            .put("index.analysis.filter.my_stop.remove_trailing", false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings);
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_stop");
        assertThat(tokenFilter, instanceOf(StopTokenFilterFactory.class));
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo an"));
        TokenStream create = tokenFilter.create(tokenizer);
        assertThat(create, instanceOf(SuggestStopFilter.class));
    }
}
