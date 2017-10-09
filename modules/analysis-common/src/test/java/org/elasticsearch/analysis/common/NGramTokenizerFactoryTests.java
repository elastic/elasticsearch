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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.EdgeNGramTokenizerFactory;
import org.elasticsearch.index.analysis.NGramTokenizerFactory;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

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
    public void testParseTokenChars() {
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);
        for (String tokenChars : Arrays.asList("letters", "number", "DIRECTIONALITY_UNDEFINED")) {
            final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3)
                .put("token_chars", tokenChars).build();
            try {
                new NGramTokenizerFactory(indexProperties, null, name, settings).create();
                fail();
            } catch (IllegalArgumentException expected) {
                // OK
            }
        }
        for (String tokenChars : Arrays.asList("letter", " digit ", "punctuation", "DIGIT", "CoNtRoL", "dash_punctuation")) {
            final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3)
                .put("token_chars", tokenChars).build();
            indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);

            new NGramTokenizerFactory(indexProperties, null, name, settings).create();
            // no exception
        }
    }

    public void testNoTokenChars() throws IOException {
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 4)
            .putList("token_chars", new String[0]).build();
        Tokenizer tokenizer = new NGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings)
            .create();
        tokenizer.setReader(new StringReader("1.34"));
        assertTokenStreamContents(tokenizer, new String[] {"1.", "1.3", "1.34", ".3", ".34", "34"});
    }

    public void testPreTokenization() throws IOException {
        // Make sure that pretokenization works well and that it can be used even with token chars which are supplementary characters
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3)
            .put("token_chars", "letter,digit").build();
        Tokenizer tokenizer = new NGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings)
            .create();
        tokenizer.setReader(new StringReader("Åbc déf g\uD801\uDC00f "));
        assertTokenStreamContents(tokenizer,
                new String[] {"Åb", "Åbc", "bc", "dé", "déf", "éf", "g\uD801\uDC00", "g\uD801\uDC00f", "\uD801\uDC00f"});
        settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3)
            .put("token_chars", "letter,digit,punctuation,whitespace,symbol").build();
        tokenizer = new NGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings).create();
        tokenizer.setReader(new StringReader(" a!$ 9"));
        assertTokenStreamContents(tokenizer,
            new String[] {" a", " a!", "a!", "a!$", "!$", "!$ ", "$ ", "$ 9", " 9"});
    }

    public void testPreTokenizationEdge() throws IOException {
        // Make sure that pretokenization works well and that it can be used even with token chars which are supplementary characters
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit").build();
        Tokenizer tokenizer =
            new EdgeNGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings).create();
        tokenizer.setReader(new StringReader("Åbc déf g\uD801\uDC00f "));
        assertTokenStreamContents(tokenizer,
                new String[] {"Åb", "Åbc", "dé", "déf", "g\uD801\uDC00", "g\uD801\uDC00f"});
        settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3)
            .put("token_chars", "letter,digit,punctuation,whitespace,symbol").build();
        tokenizer = new EdgeNGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings)
            .create();
        tokenizer.setReader(new StringReader(" a!$ 9"));
        assertTokenStreamContents(tokenizer,
                new String[] {" a", " a!"});
    }

    public void testBackwardsCompatibilityEdgeNgramTokenFilter() throws Exception {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            final Index index = new Index("test", "_na_");
            final String name = "ngr";
            Version v = randomVersion(random());
            Builder builder = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3);
            boolean reverse = random().nextBoolean();
            if (reverse) {
                builder.put("side", "back");
            }
            Settings settings = builder.build();
            Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, v.id).build();
            Tokenizer tokenizer = new MockTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream edgeNGramTokenFilter =
                new EdgeNGramTokenFilterFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings)
                    .create(tokenizer);
            if (reverse) {
                assertThat(edgeNGramTokenFilter, instanceOf(ReverseStringFilter.class));
            } else {
                assertThat(edgeNGramTokenFilter, instanceOf(EdgeNGramTokenFilter.class));
            }
        }
    }


    private Version randomVersion(Random random) throws IllegalArgumentException, IllegalAccessException {
        Field[] declaredFields = Version.class.getFields();
        List<Field> versionFields = new ArrayList<>();
        for (Field field : declaredFields) {
            if ((field.getModifiers() & Modifier.STATIC) != 0 && field.getName().startsWith("V_") && field.getType() == Version.class) {
                versionFields.add(field);
            }
        }
        return (Version) versionFields.get(random.nextInt(versionFields.size())).get(Version.class);
    }

}
