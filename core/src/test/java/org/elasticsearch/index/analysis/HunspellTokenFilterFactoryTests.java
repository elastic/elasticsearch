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
                .put(Environment.PATH_CONF_SETTING.getKey(), getDataPath("/indices/analyze/conf_dir"))
                .put("index.analysis.filter.en_US.type", "hunspell")
                .put("index.analysis.filter.en_US.locale", "en_US")
                .build();

        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("en_US");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        assertThat(hunspellTokenFilter.dedup(), is(true));

        settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(Environment.PATH_CONF_SETTING.getKey(), getDataPath("/indices/analyze/conf_dir"))
                .put("index.analysis.filter.en_US.type", "hunspell")
                .put("index.analysis.filter.en_US.dedup", false)
                .put("index.analysis.filter.en_US.locale", "en_US")
                .build();

        analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        tokenFilter = analysisService.tokenFilter("en_US");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        assertThat(hunspellTokenFilter.dedup(), is(false));
    }
}
