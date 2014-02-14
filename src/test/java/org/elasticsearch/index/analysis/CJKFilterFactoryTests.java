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
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class CJKFilterFactoryTests extends ElasticsearchTokenStreamTestCase {

    private static final String RESOURCE = "org/elasticsearch/index/analysis/cjk_analysis.json";

    @Test
    public void testDefault() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("cjk_bigram");
        String source = "多くの学生が試験に落ちた。";
        String[] expected = new String[]{"多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた" };
        Tokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    @Test
    public void testNoFlags() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("cjk_no_flags");
        String source = "多くの学生が試験に落ちた。";
        String[] expected = new String[]{"多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた" };
        Tokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }
    
    @Test
    public void testHanOnly() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("cjk_han_only");
        String source = "多くの学生が試験に落ちた。";
        String[] expected = new String[]{"多", "く", "の",  "学生", "が",  "試験", "に",  "落", "ち", "た"  };
        Tokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }
    
    @Test
    public void testHanUnigramOnly() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("cjk_han_unigram_only");
        String source = "多くの学生が試験に落ちた。";
        String[] expected = new String[]{"多", "く", "の",  "学", "学生", "生", "が",  "試", "試験", "験", "に",  "落", "ち", "た"  };
        Tokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }
    


}
