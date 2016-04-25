package org.elasticsearch.index.analysis;

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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MinHashTokenFilterTests extends ESTokenStreamTestCase {

    public void testMinHash() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put("index.analysis.filter.my_minhash.type", "minhash")
            .put("index.analysis.filter.my_minhash.num_hashes", 5);

        builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString());
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(builder.build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_minhash");
        assertThat(tokenFilter, instanceOf(MinHashTokenFilterFactory.class));

        String source = "Hello 123 world";
        String[] expected = new String[]{"0_915bcf01", "1_db6bf2d", "2_88e874da", "3_24315c2c", "4_8b60acfe"};
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testBadSettingsZeroHashes() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put("index.analysis.filter.my_minhash.type", "minhash")
            .put("index.analysis.filter.my_minhash.num_hashes", 0);

        builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString());
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(builder.build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_minhash");
        assertThat(tokenFilter, instanceOf(MinHashTokenFilterFactory.class));

        String source = "Hello 123 world";
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        Exception e = LuceneTestCase.expectThrows(IllegalArgumentException.class, () -> tokenFilter.create(tokenizer));
        assertEquals(e.getMessage(), "Must use one or more hashes for MinHash Token Filter");
    }

    public void testBadSettingsTooManyHashes() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put("index.analysis.filter.my_minhash.type", "minhash")
            .put("index.analysis.filter.my_minhash.num_hashes", 1000);

        builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString());
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(builder.build());
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_minhash");
        assertThat(tokenFilter, instanceOf(MinHashTokenFilterFactory.class));

        String source = "Hello 123 world";
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        Exception e = LuceneTestCase.expectThrows(IllegalArgumentException.class, () -> tokenFilter.create(tokenizer));
        assertEquals(e.getMessage(), "Cannot use more than 100 hashes for MinHash Token Filter");
    }
}
