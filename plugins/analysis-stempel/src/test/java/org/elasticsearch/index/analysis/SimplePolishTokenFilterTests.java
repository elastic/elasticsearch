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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugin.analysis.stempel.AnalysisStempelPlugin;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class SimplePolishTokenFilterTests extends ESTestCase {
    public void testBasicUsage() throws Exception {
        testToken("kwiaty", "kwć");
        testToken("canona", "ć");
        testToken("wirtualna", "wirtualny");
        testToken("polska", "polski");

        testAnalyzer("wirtualna polska", "wirtualny", "polski");
    }

    private void testToken(String source, String expected) throws IOException {
        Index index = new Index("test", "_na_");
        Settings settings = Settings.builder()
                .put("index.analysis.filter.myStemmer.type", "polish_stem")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings, new AnalysisStempelPlugin()::onModule);

        TokenFilterFactory filterFactory = analysisService.tokenFilter("myStemmer");

        Tokenizer tokenizer = new KeywordTokenizer();
        tokenizer.setReader(new StringReader(source));
        TokenStream ts = filterFactory.create(tokenizer);

        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        assertThat(ts.incrementToken(), equalTo(true));

        assertThat(term1.toString(), equalTo(expected));
    }

    private void testAnalyzer(String source, String... expected_terms) throws IOException {
        AnalysisService analysisService = createAnalysisService(new Index("test", "_na_"), Settings.EMPTY,
            new AnalysisStempelPlugin()::onModule);

        Analyzer analyzer = analysisService.analyzer("polish").analyzer();

        TokenStream ts = analyzer.tokenStream("test", source);

        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();

        for (String expected : expected_terms) {
            assertThat(ts.incrementToken(), equalTo(true));
            assertThat(term1.toString(), equalTo(expected));
        }
    }

}
