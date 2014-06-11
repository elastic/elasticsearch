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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static com.carrotsearch.randomizedtesting.RandomizedTest.scaledRandomIntBetween;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class StemmerTokenFilterFactoryTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testEnglishBackwardsCompatibility() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {

            Version v = ElasticsearchTestCase.randomVersion(random());
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("index.analysis.filter.my_english.type", "stemmer")
                    .put("index.analysis.filter.my_english.language", "english")
                    .put("index.analysis.analyzer.my_english.tokenizer","whitespace")
                    .put("index.analysis.analyzer.my_english.filter","my_english")
                    .put(SETTING_VERSION_CREATED,v)
                    .build();

            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_english");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            TokenStream create = tokenFilter.create(new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("foo bar")));
            NamedAnalyzer analyzer = analysisService.analyzer("my_english");

            if (v.onOrAfter(Version.V_1_3_0)) {
                assertThat(create, instanceOf(PorterStemFilter.class));
                assertAnalyzesTo(analyzer, "consolingly", new String[]{"consolingli"});
            } else {
                assertThat(create, instanceOf(SnowballFilter.class));
                assertAnalyzesTo(analyzer, "consolingly", new String[]{"consol"});
            }
        }

    }

    @Test
    public void testPorter2BackwardsCompatibility() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {

            Version v = ElasticsearchTestCase.randomVersion(random());
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("index.analysis.filter.my_porter2.type", "stemmer")
                    .put("index.analysis.filter.my_porter2.language", "porter2")
                    .put("index.analysis.analyzer.my_porter2.tokenizer","whitespace")
                    .put("index.analysis.analyzer.my_porter2.filter","my_porter2")
                    .put(SETTING_VERSION_CREATED,v)
                    .build();

            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_porter2");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            TokenStream create = tokenFilter.create(new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("foo bar")));
            NamedAnalyzer analyzer = analysisService.analyzer("my_porter2");
            assertThat(create, instanceOf(SnowballFilter.class));

            if (v.onOrAfter(Version.V_1_3_0)) {
                assertAnalyzesTo(analyzer, "possibly", new String[]{"possibl"});
            } else {
                assertAnalyzesTo(analyzer, "possibly", new String[]{"possibli"});
            }
        }

    }

}
