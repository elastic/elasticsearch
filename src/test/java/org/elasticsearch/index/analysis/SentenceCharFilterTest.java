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

import org.apache.lucene.util._TestUtil;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class SentenceCharFilterTest extends ElasticsearchTokenStreamTestCase {
    @Test
    public void sentenceCharFilter() throws Exception {
        Index index = new Index("test");
        Settings settings = settingsBuilder()
                .put("index.analysis.char_filter.first_sentence.type", "sentence")
                .put("index.analysis.char_filter.first_sentence.sentences", "1")
                .put("index.analysis.char_filter.first_two_sentences.type", "sentence")
                .put("index.analysis.char_filter.first_two_sentences.sentences", "2")
                .put("index.analysis.char_filter.first_sentence.sentences", "1")
                .put("index.analysis.char_filter.short_sentence.type", "sentence")
                .put("index.analysis.char_filter.short_sentence.analyzed_chars", "10")
                .put("index.analysis.analyzer.one_sentence.tokenizer", "standard")
                .put("index.analysis.analyzer.one_sentence.char_filter", "first_sentence")
                .put("index.analysis.analyzer.two_sentences.tokenizer", "standard")
                .put("index.analysis.analyzer.two_sentences.char_filter", "first_two_sentences")
                .put("index.analysis.analyzer.short_sentence.tokenizer", "standard")
                .put("index.analysis.analyzer.short_sentence.char_filter", "short_sentence")
                .build();
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings)), new IndicesAnalysisModule()).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)))
                .createChildInjector(parentInjector);

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        NamedAnalyzer oneSentence = analysisService.analyzer("one_sentence");
        NamedAnalyzer twoSentences = analysisService.analyzer("two_sentences");
        NamedAnalyzer shortSentence = analysisService.analyzer("short_sentence");

        // One sentence
        assertTokenStreamContents(oneSentence.tokenStream("test", "The quick brown fox jumped."),
                new String[]{"The", "quick", "brown", "fox", "jumped"});
        assertTokenStreamContents(twoSentences.tokenStream("test", "The quick brown fox jumped."),
                new String[]{"The", "quick", "brown", "fox", "jumped"});

        // Two sentences
        assertTokenStreamContents(oneSentence.tokenStream("test", "The quick brown fox jumped.  Then came down."),
                new String[]{"The", "quick", "brown", "fox", "jumped"});
        assertTokenStreamContents(twoSentences.tokenStream("test", "The quick brown fox jumped.  Then came down."),
                new String[]{"The", "quick", "brown", "fox", "jumped", "Then", "came", "down"});

        // Three Sentences
        assertTokenStreamContents(oneSentence.tokenStream("test", "The quick brown fox jumped.  Then came down.  The cow jumped over the moon."),
                new String[]{"The", "quick", "brown", "fox", "jumped"});
        assertTokenStreamContents(twoSentences.tokenStream("test", "The quick brown fox jumped.  Then came down.  The cow jumped over the moon."),
                new String[]{"The", "quick", "brown", "fox", "jumped", "Then", "came", "down"});

        // Empty
        assertTokenStreamContents(oneSentence.tokenStream("test", ""),
                new String[]{});
        assertTokenStreamContents(twoSentences.tokenStream("test", ""),
                new String[]{});

        // Incomplete sentences
        assertTokenStreamContents(oneSentence.tokenStream("test", "The quick brown fox jumped"),
                new String[]{"The", "quick", "brown", "fox", "jumped"});
        assertTokenStreamContents(twoSentences.tokenStream("test", "The quick brown fox jumped"),
                new String[]{"The", "quick", "brown", "fox", "jumped"});
        assertTokenStreamContents(oneSentence.tokenStream("test", "The quick brown fox jumped.  Then came down"),
                new String[]{"The", "quick", "brown", "fox", "jumped"});
        assertTokenStreamContents(twoSentences.tokenStream("test", "The quick brown fox jumped.  Then came down"),
                new String[]{"The", "quick", "brown", "fox", "jumped", "Then", "came", "down"});

        // String longer then analyzed chars
        StringBuilder tooLong = new StringBuilder("The quick brown fox jumped.  Then came down.  Capitalletter ");
        while (tooLong.length() < SentenceCharFilterFactory.DEFAULT_ANALYZED_CHARS) {
            tooLong.append(_TestUtil.randomAnalysisString(random(), SentenceCharFilterFactory.DEFAULT_ANALYZED_CHARS * 2, true));
        }
        assertTokenStreamContents(oneSentence.tokenStream("test", tooLong.toString()),
                new String[]{"The", "quick", "brown", "fox", "jumped"});
        assertTokenStreamContents(twoSentences.tokenStream("test", tooLong.toString()),
                new String[]{"The", "quick", "brown", "fox", "jumped", "Then", "came", "down"});

        // First sentence longer then analyzed chars
        assertTokenStreamContents(shortSentence.tokenStream("test", "The quick brown fox jumped."), new String[]{"The", "quick"});
        assertTokenStreamContents(shortSentence.tokenStream("test", "The quickest brown fox jumped."), new String[]{"The"});
        //                                            Only analyze up to -----^ (inclusive)

        // Super degenerate case: one word for all analyzed chars
        assertTokenStreamContents(shortSentence.tokenStream("test", "Thequickestbrownfoxjumped."), new String[]{});
    }

}
