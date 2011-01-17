/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllTokenStream;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.Test;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.ArrayList;

import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.hamcrest.Matchers.*;

/**
 * @author Edward Dale (scompt@scompt.com)
 */
public class CompoundAnalysisTests {

    private File generateWordList() throws Exception {
        File wordListFile = File.createTempFile("wordlist", ".txt");
        wordListFile.deleteOnExit();

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(wordListFile));
            writer.write("donau\ndampf\nschiff\n");
            writer.write("spargel\ncreme\nsuppe");
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        return wordListFile;
    }

    private Settings generateSettings(File wordListFile) throws Exception {
        StringBuilder settingsStr = new StringBuilder();

        settingsStr.append("index : \n");
        settingsStr.append("  analysis :\n");
        settingsStr.append("    analyzer :\n");
        settingsStr.append("      myAnalyzer2 :\n");
        settingsStr.append("        tokenizer : standard\n");
        settingsStr.append("        filter : [dict_dec, standard, lowercase, stop]\n");
        settingsStr.append("    filter :\n");
        settingsStr.append("      dict_dec :\n");
        settingsStr.append("        type : dictionary_decompounder\n");
        settingsStr.append("        word_list_path : ").append(wordListFile.getAbsolutePath()).append('\n');

        return settingsBuilder().loadFromSource(settingsStr.toString()).build();
    }

    @Test public void testDefaultsCompoundAnalysis() throws Exception {
        Index index = new Index("test");
        Settings settings = generateSettings(generateWordList());

        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(settings),
                new IndexNameModule(index),
                new AnalysisModule(settings)).createInjector();

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        TokenFilterFactory filterFactory = analysisService.tokenFilter("dict_dec");
        MatcherAssert.assertThat(filterFactory, instanceOf(DictionaryCompoundWordTokenFilterFactory.class));
    }

    @Test public void testDictionaryDecompounder() throws Exception {
        Index index = new Index("test");
        Settings settings = generateSettings(generateWordList());

        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(settings),
                new IndexNameModule(index),
                new AnalysisModule(settings)).createInjector();

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        Analyzer analyzer = analysisService.analyzer("myAnalyzer2").analyzer();

        AllEntries allEntries = new AllEntries();
        allEntries.addText("field1", "donaudampfschiff", 1.0f);
        allEntries.addText("field2", "spargelcremesuppe", 1.0f);
        allEntries.reset();

        TokenStream stream = AllTokenStream.allTokenStream("_all", allEntries, analyzer);
        TermAttribute termAtt = stream.addAttribute(TermAttribute.class);

        List<String> terms = new ArrayList<String>();
        while (stream.incrementToken()) {
            String tokText = termAtt.term();
            terms.add(tokText);
        }
        MatcherAssert.assertThat(terms.size(), equalTo(8));
        MatcherAssert.assertThat(terms, hasItems("donau", "dampf", "schiff", "donaudampfschiff", "spargel", "creme", "suppe", "spargelcremesuppe"));
    }
}
