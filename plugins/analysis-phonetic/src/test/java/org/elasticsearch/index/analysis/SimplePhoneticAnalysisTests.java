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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugin.analysis.AnalysisPhoneticPlugin;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

public class SimplePhoneticAnalysisTests extends ESTestCase {

    private TestAnalysis analysis;

    @Before
    public void setup() throws IOException {
        String yaml = "/org/elasticsearch/index/analysis/phonetic-1.yml";
        Settings settings = Settings.builder().loadFromStream(yaml, getClass().getResourceAsStream(yaml), false)
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        this.analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisPhoneticPlugin());
    }

    public void testPhoneticTokenFilterFactory() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("phonetic");
        MatcherAssert.assertThat(filterFactory, instanceOf(PhoneticTokenFilterFactory.class));
    }

    public void testPhoneticTokenFilterBeiderMorseNoLanguage() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("beidermorsefilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("ABADIAS"));
        String[] expected = new String[] { "abYdias", "abYdios", "abadia", "abadiaS", "abadias", "abadio", "abadioS", "abadios", "abodia",
                "abodiaS", "abodias", "abodio", "abodioS", "abodios", "avadias", "avadios", "avodias", "avodios", "obadia", "obadiaS",
                "obadias", "obadio", "obadioS", "obadios", "obodia", "obodiaS", "obodias", "obodioS" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterBeiderMorseWithLanguage() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("beidermorsefilterfrench");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("Rimbault"));
        String[] expected = new String[] { "rimbD", "rimbDlt", "rimba", "rimbalt", "rimbo", "rimbolt", "rimbu", "rimbult", "rmbD", "rmbDlt",
                "rmba", "rmbalt", "rmbo", "rmbolt", "rmbu", "rmbult" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }
}
