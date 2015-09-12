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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

@ThreadLeakScope(Scope.NONE)
public class ShingleTokenFilterFactoryTests extends ESTokenStreamTestCase {

    private static final String RESOURCE = "/org/elasticsearch/index/analysis/shingle_analysis.json";

    @Test
    public void testDefault() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("shingle");
        String source = "the quick brown fox";
        String[] expected = new String[]{"the", "the quick", "quick", "quick brown", "brown", "brown fox", "fox"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testInverseMapping() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("shingle_inverse");
        assertThat(tokenFilter, instanceOf(ShingleTokenFilterFactory.class));
        String source = "the quick brown fox";
        String[] expected = new String[]{"the_quick_brown", "quick_brown_fox"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testInverseMappingNoShingles() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("shingle_inverse");
        assertThat(tokenFilter, instanceOf(ShingleTokenFilterFactory.class));
        String source = "the quick";
        String[] expected = new String[]{"the", "quick"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testFillerToken() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("shingle_filler");
        String source = "simon the sorcerer";
        String[] expected = new String[]{"simon FILLER", "simon FILLER sorcerer", "FILLER sorcerer"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        TokenStream stream = new StopFilter(tokenizer, StopFilter.makeStopSet("the"));
        assertTokenStreamContents(tokenFilter.create(stream), expected);
    }
}
