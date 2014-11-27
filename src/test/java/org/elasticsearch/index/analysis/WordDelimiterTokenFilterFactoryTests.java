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
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class WordDelimiterTokenFilterFactoryTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testDefault() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[]{"Power", "Shot", "500", "42", "wi", "fi", "wi", "fi", "4000", "j", "2", "se", "O", "Neil"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testCatenateWords() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "false")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[]{"PowerShot", "500", "42", "wifi", "wifi", "4000", "j", "2", "se", "ONeil"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testCatenateNumbers() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .put("index.analysis.filter.my_word_delimiter.generate_number_parts", "false")
                .put("index.analysis.filter.my_word_delimiter.catenate_numbers", "true")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[]{"Power", "Shot", "50042", "wi", "fi", "wi", "fi", "4000", "j", "2", "se", "O", "Neil"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testCatenateAll() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "false")
                .put("index.analysis.filter.my_word_delimiter.generate_number_parts", "false")
                .put("index.analysis.filter.my_word_delimiter.catenate_all", "true")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[]{"PowerShot", "50042", "wifi", "wifi4000", "j2se", "ONeil"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testSplitOnCaseChange() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .put("index.analysis.filter.my_word_delimiter.split_on_case_change", "false")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot";
        String[] expected = new String[]{"PowerShot"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testPreserveOriginal() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .put("index.analysis.filter.my_word_delimiter.preserve_original", "true")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[]{"PowerShot", "Power", "Shot", "500-42", "500", "42", "wi-fi", "wi", "fi", "wi-fi-4000", "wi", "fi", "4000", "j2se", "j", "2", "se", "O'Neil's", "O", "Neil"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testStemEnglishPossessive() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .put("index.analysis.filter.my_word_delimiter.stem_english_possessive", "false")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[]{"Power", "Shot", "500", "42", "wi", "fi", "wi", "fi", "4000", "j", "2", "se", "O", "Neil", "s"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    /** Correct offset order when doing both parts and concatenation: PowerShot is a synonym of Power */
    @Test
    public void testPartsAndCatenate() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "true")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot";
        String[] expected = new String[]{"Power", "PowerShot", "Shot" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }
     
    /** Back compat: 
     * old offset order when doing both parts and concatenation: PowerShot is a synonym of Shot */
    @Test
    public void testDeprecatedPartsAndCatenate() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settingsBuilder()
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter")
                .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "true")
                .put("index.analysis.filter.my_word_delimiter.version", "4.7")
                .build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_word_delimiter");
        String source = "PowerShot";
        String[] expected = new String[]{"Power", "Shot", "PowerShot" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

}
