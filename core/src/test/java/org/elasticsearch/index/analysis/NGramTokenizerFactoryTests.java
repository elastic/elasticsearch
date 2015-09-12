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

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.*;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.carrotsearch.randomizedtesting.RandomizedTest.scaledRandomIntBetween;
import static org.hamcrest.Matchers.instanceOf;

public class NGramTokenizerFactoryTests extends ESTokenStreamTestCase {


    @Test
    public void testParseTokenChars() {
        final Index index = new Index("test");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        for (String tokenChars : Arrays.asList("letters", "number", "DIRECTIONALITY_UNDEFINED")) {
            final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", tokenChars).build();
            try {
                new NGramTokenizerFactory(index, indexSettings, name, settings).create();
                fail();
            } catch (IllegalArgumentException expected) {
                // OK
            }
        }
        for (String tokenChars : Arrays.asList("letter", " digit ", "punctuation", "DIGIT", "CoNtRoL", "dash_punctuation")) {
            final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", tokenChars).build();
            new NGramTokenizerFactory(index, indexSettings, name, settings).create();
            // no exception
        }
    }

    @Test
    public void testNoTokenChars() throws IOException {
        final Index index = new Index("test");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 4).putArray("token_chars", new String[0]).build();
        Tokenizer tokenizer = new NGramTokenizerFactory(index, indexSettings, name, settings).create();
        tokenizer.setReader(new StringReader("1.34"));
        assertTokenStreamContents(tokenizer, new String[] {"1.", "1.3", "1.34", ".3", ".34", "34"});
    }

    @Test
    public void testPreTokenization() throws IOException {
        // Make sure that pretokenization works well and that it can be used even with token chars which are supplementary characters
        final Index index = new Index("test");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit").build();
        Tokenizer tokenizer = new NGramTokenizerFactory(index, indexSettings, name, settings).create();
        tokenizer.setReader(new StringReader("Åbc déf g\uD801\uDC00f "));
        assertTokenStreamContents(tokenizer,
                new String[] {"Åb", "Åbc", "bc", "dé", "déf", "éf", "g\uD801\uDC00", "g\uD801\uDC00f", "\uD801\uDC00f"});
        settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit,punctuation,whitespace,symbol").build();
        tokenizer = new NGramTokenizerFactory(index, indexSettings, name, settings).create();
        tokenizer.setReader(new StringReader(" a!$ 9"));
        assertTokenStreamContents(tokenizer,
            new String[] {" a", " a!", "a!", "a!$", "!$", "!$ ", "$ ", "$ 9", " 9"});
    }

    @Test
    public void testPreTokenizationEdge() throws IOException {
        // Make sure that pretokenization works well and that it can be used even with token chars which are supplementary characters
        final Index index = new Index("test");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit").build();
        Tokenizer tokenizer = new EdgeNGramTokenizerFactory(index, indexSettings, name, settings).create();
        tokenizer.setReader(new StringReader("Åbc déf g\uD801\uDC00f "));
        assertTokenStreamContents(tokenizer,
                new String[] {"Åb", "Åbc", "dé", "déf", "g\uD801\uDC00", "g\uD801\uDC00f"});
        settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit,punctuation,whitespace,symbol").build();
        tokenizer = new EdgeNGramTokenizerFactory(index, indexSettings, name, settings).create();
        tokenizer.setReader(new StringReader(" a!$ 9"));
        assertTokenStreamContents(tokenizer,
                new String[] {" a", " a!"});
    }
    
    @Test
    public void testBackwardsCompatibilityEdgeNgramTokenizer() throws Exception {
        int iters = scaledRandomIntBetween(20, 100);
        final Index index = new Index("test");
        final String name = "ngr";
        for (int i = 0; i < iters; i++) {
            Version v = randomVersion(random());
            if (v.onOrAfter(Version.V_0_90_2)) {
                Builder builder = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit");
                boolean compatVersion = false;
                if ((compatVersion = random().nextBoolean())) {
                    builder.put("version", "4." + random().nextInt(3));
                    builder.put("side", "back");
                }
                Settings settings = builder.build();
                Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, v.id).build();
                Tokenizer edgeNGramTokenizer = new EdgeNGramTokenizerFactory(index, indexSettings, name, settings).create();
                edgeNGramTokenizer.setReader(new StringReader("foo bar"));
                if (compatVersion) {
                    assertThat(edgeNGramTokenizer, instanceOf(Lucene43EdgeNGramTokenizer.class));
                } else {
                    assertThat(edgeNGramTokenizer, instanceOf(EdgeNGramTokenizer.class));
                }

            } else {
                Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("side", "back").build();
                Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, v.id).build();
                Tokenizer edgeNGramTokenizer = new EdgeNGramTokenizerFactory(index, indexSettings, name, settings).create();
                edgeNGramTokenizer.setReader(new StringReader("foo bar"));
                assertThat(edgeNGramTokenizer, instanceOf(Lucene43EdgeNGramTokenizer.class));
            }
        }
        Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("side", "back").build();
        Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        try {
            new EdgeNGramTokenizerFactory(index, indexSettings, name, settings).create();
            fail("should fail side:back is not supported anymore");
        } catch (IllegalArgumentException ex) {
        }
        
    }
    
    @Test
    public void testBackwardsCompatibilityNgramTokenizer() throws Exception {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            final Index index = new Index("test");
            final String name = "ngr";
            Version v = randomVersion(random());
            if (v.onOrAfter(Version.V_0_90_2)) {
                Builder builder = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit");
                boolean compatVersion = false;
                if ((compatVersion = random().nextBoolean())) {
                    builder.put("version", "4." + random().nextInt(3));
                }
                Settings settings = builder.build();
                Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, v.id).build();
                Tokenizer nGramTokenizer = new NGramTokenizerFactory(index, indexSettings, name, settings).create();
                nGramTokenizer.setReader(new StringReader("foo bar"));
                if (compatVersion) { 
                    assertThat(nGramTokenizer, instanceOf(Lucene43NGramTokenizer.class));
                } else {
                    assertThat(nGramTokenizer, instanceOf(NGramTokenizer.class));
                }

            } else {
                Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).build();
                Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, v.id).build();
                Tokenizer nGramTokenizer = new NGramTokenizerFactory(index, indexSettings, name, settings).create();
                nGramTokenizer.setReader(new StringReader("foo bar"));
                assertThat(nGramTokenizer, instanceOf(Lucene43NGramTokenizer.class));
            }
        }
    }
    
    @Test
    public void testBackwardsCompatibilityEdgeNgramTokenFilter() throws Exception {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            final Index index = new Index("test");
            final String name = "ngr";
            Version v = randomVersion(random());
            if (v.onOrAfter(Version.V_0_90_2)) {
                Builder builder = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3);
                boolean compatVersion = false;
                if ((compatVersion = random().nextBoolean())) {
                    builder.put("version", "4." + random().nextInt(3));
                }
                boolean reverse = random().nextBoolean();
                if (reverse) {
                    builder.put("side", "back");
                }
                Settings settings = builder.build();
                Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, v.id).build();
                Tokenizer tokenizer = new MockTokenizer();
                tokenizer.setReader(new StringReader("foo bar"));
                TokenStream edgeNGramTokenFilter = new EdgeNGramTokenFilterFactory(index, indexSettings, name, settings).create(tokenizer);
                if (reverse) {
                    assertThat(edgeNGramTokenFilter, instanceOf(ReverseStringFilter.class));
                } else if (compatVersion) { 
                    assertThat(edgeNGramTokenFilter, instanceOf(Lucene43EdgeNGramTokenFilter.class));
                } else {
                    assertThat(edgeNGramTokenFilter, instanceOf(EdgeNGramTokenFilter.class));
                }

            } else {
                Builder builder = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3);
                boolean reverse = random().nextBoolean();
                if (reverse) {
                    builder.put("side", "back");
                }
                Settings settings = builder.build();
                Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, v.id).build();
                Tokenizer tokenizer = new MockTokenizer();
                tokenizer.setReader(new StringReader("foo bar"));
                TokenStream edgeNGramTokenFilter = new EdgeNGramTokenFilterFactory(index, indexSettings, name, settings).create(tokenizer);
                if (reverse) {
                    assertThat(edgeNGramTokenFilter, instanceOf(ReverseStringFilter.class));
                } else {
                    assertThat(edgeNGramTokenFilter, instanceOf(Lucene43EdgeNGramTokenFilter.class));
                }
            }
        }
    }

    
    private Version randomVersion(Random random) throws IllegalArgumentException, IllegalAccessException {
        Field[] declaredFields = Version.class.getDeclaredFields();
        List<Field> versionFields = new ArrayList<>();
        for (Field field : declaredFields) {
            if ((field.getModifiers() & Modifier.STATIC) != 0 && field.getName().startsWith("V_") && field.getType() == Version.class) {
                versionFields.add(field);
            }
        }
        return (Version) versionFields.get(random.nextInt(versionFields.size())).get(Version.class);
    }

}
