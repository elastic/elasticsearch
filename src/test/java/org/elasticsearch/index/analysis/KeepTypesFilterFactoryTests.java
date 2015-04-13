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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

public class KeepTypesFilterFactoryTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testKeepTypes() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.keep_numbers.type", "keep_types")
                .putArray("index.analysis.filter.keep_numbers.types", new String[] {"<NUM>", "<SOMETHINGELSE>"})
                .build();
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("keep_numbers");
        assertThat(tokenFilter, instanceOf(KeepTypesFilterFactory.class));
        String source = "Hello 123 world";
        String[] expected = new String[]{"123"};
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[]{2});
    }
}
