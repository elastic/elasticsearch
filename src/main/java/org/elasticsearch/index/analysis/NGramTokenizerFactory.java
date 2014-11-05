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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.Lucene43NGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class NGramTokenizerFactory extends AbstractTokenizerFactory {

    private final int minGram;
    private final int maxGram;
    private final CharMatcher matcher;
    private org.elasticsearch.Version esVersion;

    static final Map<String, CharMatcher> MATCHERS;

    static {
        ImmutableMap.Builder<String, CharMatcher> builder = ImmutableMap.builder();
        builder.put("letter", CharMatcher.Basic.LETTER);
        builder.put("digit", CharMatcher.Basic.DIGIT);
        builder.put("whitespace", CharMatcher.Basic.WHITESPACE);
        builder.put("punctuation", CharMatcher.Basic.PUNCTUATION);
        builder.put("symbol", CharMatcher.Basic.SYMBOL);
        // Populate with unicode categories from java.lang.Character
        for (Field field : Character.class.getFields()) {
            if (!field.getName().startsWith("DIRECTIONALITY")
                    && Modifier.isPublic(field.getModifiers())
                    && Modifier.isStatic(field.getModifiers())
                    && field.getType() == byte.class) {
                try {
                    builder.put(field.getName().toLowerCase(Locale.ROOT), CharMatcher.ByUnicodeCategory.of(field.getByte(null)));
                } catch (Exception e) {
                    // just ignore
                    continue;
                }
            }
        }
        MATCHERS = builder.build();
    }

    static CharMatcher parseTokenChars(String[] characterClasses) {
        if (characterClasses == null || characterClasses.length == 0) {
            return null;
        }
        CharMatcher.Builder builder = new CharMatcher.Builder();
        for (String characterClass : characterClasses) {
            characterClass = characterClass.toLowerCase(Locale.ROOT).trim();
            CharMatcher matcher = MATCHERS.get(characterClass);
            if (matcher == null) {
                throw new ElasticsearchIllegalArgumentException("Unknown token type: '" + characterClass + "', must be one of " + MATCHERS.keySet());
            }
            builder.or(matcher);
        }
        return builder.build();
    }

    @Inject
    public NGramTokenizerFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.minGram = settings.getAsInt("min_gram", NGramTokenizer.DEFAULT_MIN_NGRAM_SIZE);
        this.maxGram = settings.getAsInt("max_gram", NGramTokenizer.DEFAULT_MAX_NGRAM_SIZE);
        this.matcher = parseTokenChars(settings.getAsArray("token_chars"));
        this.esVersion = org.elasticsearch.Version.indexCreated(indexSettings);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Tokenizer create() {
        if (version.onOrAfter(Version.LUCENE_4_3) && esVersion.onOrAfter(org.elasticsearch.Version.V_0_90_2)) {
            /*
             * We added this in 0.90.2 but 0.90.1 used LUCENE_43 already so we can not rely on the lucene version.
             * Yet if somebody uses 0.90.2 or higher with a prev. lucene version we should also use the deprecated version.
             */
            final Version version = this.version == Version.LUCENE_4_3 ? Version.LUCENE_4_4 : this.version; // always use 4.4 or higher
            if (matcher == null) {
                return new NGramTokenizer(minGram, maxGram);
            } else {
                return new NGramTokenizer(minGram, maxGram) {
                    @Override
                    protected boolean isTokenChar(int chr) {
                        return matcher.isTokenChar(chr);
                    }
                };
            }
        } else {
            return new Lucene43NGramTokenizer(minGram, maxGram);
        }
    }

}