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
package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.io.StringReader;

import static com.carrotsearch.randomizedtesting.RandomizedTest.scaledRandomIntBetween;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.instanceOf;

public class StemmerTokenFilterFactoryTests extends ESTokenStreamTestCase {

    private static final CommonAnalysisPlugin PLUGIN = new CommonAnalysisPlugin();

    public void testEnglishFilterFactory() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            Version v = VersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                    .put("index.analysis.filter.my_english.type", "stemmer")
                    .put("index.analysis.filter.my_english.language", "english")
                    .put("index.analysis.analyzer.my_english.tokenizer","whitespace")
                    .put("index.analysis.analyzer.my_english.filter","my_english")
                    .put(SETTING_VERSION_CREATED,v)
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build();

            ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_english");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_english");
            assertThat(create, instanceOf(PorterStemFilter.class));
            assertAnalyzesTo(analyzer, "consolingly", new String[]{"consolingli"});
        }
    }

    public void testPorter2FilterFactory() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {

            Version v = VersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                    .put("index.analysis.filter.my_porter2.type", "stemmer")
                    .put("index.analysis.filter.my_porter2.language", "porter2")
                    .put("index.analysis.analyzer.my_porter2.tokenizer","whitespace")
                    .put("index.analysis.analyzer.my_porter2.filter","my_porter2")
                    .put(SETTING_VERSION_CREATED,v)
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build();

            ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_porter2");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_porter2");
            assertThat(create, instanceOf(SnowballFilter.class));
            assertAnalyzesTo(analyzer, "possibly", new String[]{"possibl"});
        }
    }
    
    public void testEnglishPluralFilter() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {

            Version v = VersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                    .put("index.analysis.filter.my_plurals.type", "stemmer")
                    .put("index.analysis.filter.my_plurals.language", "plural_english")
                    .put("index.analysis.analyzer.my_plurals.tokenizer","whitespace")
                    .put("index.analysis.analyzer.my_plurals.filter","my_plurals")
                    .put(SETTING_VERSION_CREATED,v)
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build();

            ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_plurals");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("dresses"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_plurals");
            assertThat(create, instanceOf(EnglishPluralStemFilter.class));

            // Check old EnglishMinimalStemmer ("S" stemmer) logic
            assertAnalyzesTo(analyzer, "phones", new String[]{"phone"});
            assertAnalyzesTo(analyzer, "horses", new String[]{"horse"});
            assertAnalyzesTo(analyzer, "cameras", new String[]{"camera"});
            
            // TODO The orginal s stemmer gives up on stemming oes words because English has no fixed rule for the stem
            // (see https://howtospell.co.uk/making-O-words-plural )
            // Would be good to find a heuristic for stemming oes words.
            assertAnalyzesTo(analyzer, "toes", new String[]{"toes"});
            assertAnalyzesTo(analyzer, "shoes", new String[]{"shoes"});
            assertAnalyzesTo(analyzer, "heroes", new String[]{"heroes"});

            // Check improved EnglishPluralStemFilter logic
            //sses
            assertAnalyzesTo(analyzer, "dresses", new String[]{"dress"});
            assertAnalyzesTo(analyzer, "possess", new String[]{"possess"});
            assertAnalyzesTo(analyzer, "possesses", new String[]{"possess"});
            // xes
            assertAnalyzesTo(analyzer, "boxes", new String[]{"box"});
            assertAnalyzesTo(analyzer, "axes", new String[]{"axe"});
            //shes
            assertAnalyzesTo(analyzer, "dishes", new String[]{"dish"});
            assertAnalyzesTo(analyzer, "washes", new String[]{"wash"});
            //ees
            assertAnalyzesTo(analyzer, "employees", new String[]{"employee"});
            assertAnalyzesTo(analyzer, "bees", new String[]{"bee"});
            //tch
            assertAnalyzesTo(analyzer, "watches", new String[]{"watch"});
            assertAnalyzesTo(analyzer, "itches", new String[]{"itch"});
            // ies->y but only for length >4
            assertAnalyzesTo(analyzer, "spies", new String[]{"spy"});
            assertAnalyzesTo(analyzer, "ties", new String[]{"tie"});
            assertAnalyzesTo(analyzer, "lies", new String[]{"lie"});
            assertAnalyzesTo(analyzer, "pies", new String[]{"pie"});
            assertAnalyzesTo(analyzer, "dies", new String[]{"die"});

            
            // *CHES - would be good to find a simple rule that solves lunches, churches but doesn't break aches
            // documenting current behaviour here as a known issue:
            assertAnalyzesTo(analyzer, "lunches", new String[]{"lunche"});
            assertAnalyzesTo(analyzer, "avalanches", new String[]{"avalanche"});
            assertAnalyzesTo(analyzer, "headaches", new String[]{"headache"});
        }
    }    

    public void testMultipleLanguagesThrowsException() throws IOException {
        Version v = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder().put("index.analysis.filter.my_english.type", "stemmer")
                .putList("index.analysis.filter.my_english.language", "english", "light_english").put(SETTING_VERSION_CREATED, v)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN));
        assertEquals("Invalid stemmer class specified: [english, light_english]", e.getMessage());
    }
}
