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
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class HunspellTokenFilterFactoryTests extends ElasticsearchTestCase {

    @Test
    public void testDedup() throws IOException {
        Settings settings = settingsBuilder()
                .put("path.conf", getResource("/indices/analyze/conf_dir"))
                .put("index.analysis.filter.en_US.type", "hunspell")
                .put("index.analysis.filter.en_US.locale", "en_US")
                .build();

        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("en_US");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        assertThat(hunspellTokenFilter.dedup(), is(true));

        settings = settingsBuilder()
                .put("path.conf", getResource("/indices/analyze/conf_dir"))
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
