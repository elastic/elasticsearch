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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.Lucene43StopFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.search.suggest.analyzing.SuggestStopFilter;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.inject.ProvisionException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;


public class StopTokenFilterTests extends ElasticsearchTokenStreamTestCase {

    @Test(expected = ProvisionException.class)
    public void testPositionIncrementSetting() throws IOException {
        Builder builder = ImmutableSettings.settingsBuilder().put("index.analysis.filter.my_stop.type", "stop")
                .put("index.analysis.filter.my_stop.enable_position_increments", false);
        if (random().nextBoolean()) {
            builder.put("index.analysis.filter.my_stop.version", "5.0");
        }
        Settings settings = builder.build();
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        analysisService.tokenFilter("my_stop");
    }

    @Test
    public void testCorrectPositionIncrementSetting() throws IOException {
        Builder builder = ImmutableSettings.settingsBuilder().put("index.analysis.filter.my_stop.type", "stop");
        int thingToDo = random().nextInt(3);
        if (thingToDo == 0) {
            builder.put("index.analysis.filter.my_stop.version", Version.LATEST);
        } else if (thingToDo == 1) {
            builder.put("index.analysis.filter.my_stop.version", Version.LUCENE_4_0);
            if (random().nextBoolean()) {
                builder.put("index.analysis.filter.my_stop.enable_position_increments", true);
            }
        } else {
            // don't specify
        }
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(builder.build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_stop");
        assertThat(tokenFilter, instanceOf(StopTokenFilterFactory.class));
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo bar"));
        TokenStream create = tokenFilter.create(tokenizer);
        if (thingToDo == 1) {
            assertThat(create, instanceOf(Lucene43StopFilter.class));
        } else {
            assertThat(create, instanceOf(StopFilter.class));
        }
    }

    @Test
    public void testDeprecatedPositionIncrementSettingWithVersions() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.my_stop.type", "stop")
                .put("index.analysis.filter.my_stop.enable_position_increments", false).put("index.analysis.filter.my_stop.version", "4.3")
                .build();
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_stop");
        assertThat(tokenFilter, instanceOf(StopTokenFilterFactory.class));
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo bar"));
        TokenStream create = tokenFilter.create(tokenizer);
        assertThat(create, instanceOf(Lucene43StopFilter.class));
    }

    @Test
    public void testThatSuggestStopFilterWorks() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.my_stop.type", "stop")
                .put("index.analysis.filter.my_stop.remove_trailing", false)
                .build();
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_stop");
        assertThat(tokenFilter, instanceOf(StopTokenFilterFactory.class));
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo an"));
        TokenStream create = tokenFilter.create(tokenizer);
        assertThat(create, instanceOf(SuggestStopFilter.class));
    }
}
