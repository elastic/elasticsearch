/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.index.analysis;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.EdgeNGramTokenizerFactory;
import org.elasticsearch.index.analysis.NGramTokenizerFactory;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

@ThreadLeakScope(Scope.NONE)
public class NGramTokenizerFactoryTests extends BaseTokenStreamTestCase {

    @Test
    public void testParseTokenChars() {
        final Index index = new Index("test");
        final String name = "ngr";
        final Settings indexSettings = ImmutableSettings.EMPTY;
        for (String tokenChars : Arrays.asList("letters", "number", "DIRECTIONALITY_UNDEFINED")) {
            final Settings settings = ImmutableSettings.builder().put("min_gram", 2).put("max_gram", 3).put("token_chars", tokenChars).build();
            try {
                new NGramTokenizerFactory(index, indexSettings, name, settings).create(new StringReader(""));
                fail();
            } catch (ElasticSearchIllegalArgumentException expected) {
                // OK
            }
        }
        for (String tokenChars : Arrays.asList("letter", " digit ", "punctuation", "DIGIT", "CoNtRoL", "dash_punctuation")) {
            final Settings settings = ImmutableSettings.builder().put("min_gram", 2).put("max_gram", 3).put("token_chars", tokenChars).build();
            new NGramTokenizerFactory(index, indexSettings, name, settings).create(new StringReader(""));
            // no exception
        }
    }

    @Test
    public void testPreTokenization() throws IOException {
        // Make sure that pretokenization works well and that it can be used even with token chars which are supplementary characters
        final Index index = new Index("test");
        final String name = "ngr";
        final Settings indexSettings = ImmutableSettings.EMPTY;
        Settings settings = ImmutableSettings.builder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit").build();
        assertTokenStreamContents(new NGramTokenizerFactory(index, indexSettings, name, settings).create(new StringReader("Åbc déf g\uD801\uDC00f ")),
                new String[] {"Åb", "Åbc", "bc", "dé", "déf", "éf", "g\uD801\uDC00", "g\uD801\uDC00f", "\uD801\uDC00f"});
        settings = ImmutableSettings.builder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit,punctuation,whitespace,symbol").build();
        assertTokenStreamContents(new NGramTokenizerFactory(index, indexSettings, name, settings).create(new StringReader(" a!$ 9")),
            new String[] {" a", " a!", "a!", "a!$", "!$", "!$ ", "$ ", "$ 9", " 9"});
    }

    @Test
    public void testPreTokenizationEdge() throws IOException {
        // Make sure that pretokenization works well and that it can be used even with token chars which are supplementary characters
        final Index index = new Index("test");
        final String name = "ngr";
        final Settings indexSettings = ImmutableSettings.EMPTY;
        Settings settings = ImmutableSettings.builder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit").build();
        assertTokenStreamContents(new EdgeNGramTokenizerFactory(index, indexSettings, name, settings).create(new StringReader("Åbc déf g\uD801\uDC00f ")),
                new String[] {"Åb", "Åbc", "dé", "déf", "g\uD801\uDC00", "g\uD801\uDC00f"});
        settings = ImmutableSettings.builder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit,punctuation,whitespace,symbol").build();
        assertTokenStreamContents(new EdgeNGramTokenizerFactory(index, indexSettings, name, settings).create(new StringReader(" a!$ 9")),
                new String[] {" a", " a!"});
    }

}
