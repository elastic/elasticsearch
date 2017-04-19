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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;


public class CharGroupTokenizerFactoryTests extends ESTokenStreamTestCase {
    public void testParseTokenChars() {
        final Index index = new Index("test", "_na_");
        final String name = "cg";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);
        for (String conf : Arrays.asList("\\v", "abc\\$")) {
            final Settings settings = newAnalysisSettingsBuilder().put("tokenize_on_chars", conf).build();
            try {
                new CharGroupTokenizerFactory(indexProperties, null, name, settings).create();
                fail();
            } catch (RuntimeException expected) {
                // OK
            }
        }

        for (String conf : Arrays.asList("", "\\s", "abc", "abc\\s", "\\w", "foo\\d")) {
            final Settings settings = newAnalysisSettingsBuilder().put("tokenize_on_chars", conf).build();
            indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);

            new CharGroupTokenizerFactory(indexProperties, null, name, settings).create();
            // no exception
        }
    }

    public void testTokenization() throws IOException {
        final Index index = new Index("test", "_na_");
        final String name = "cg";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        final Settings settings = newAnalysisSettingsBuilder().put("tokenize_on_chars", "\\s:<>$").build();
        Tokenizer tokenizer = new CharGroupTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings),
                null, name, settings).create();
        tokenizer.setReader(new StringReader("foo bar $34 test:test2"));
        assertTokenStreamContents(tokenizer, new String[] {"foo", "bar", "34", "test", "test2"});
    }
}
