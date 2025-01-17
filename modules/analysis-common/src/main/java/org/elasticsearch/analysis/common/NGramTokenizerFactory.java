/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;

public class NGramTokenizerFactory extends AbstractTokenizerFactory {

    private final int minGram;
    private final int maxGram;
    private final CharMatcher matcher;

    static final Map<String, CharMatcher> MATCHERS;

    static {
        Map<String, CharMatcher> matchers = new HashMap<>();
        matchers.put("letter", CharMatcher.Basic.LETTER);
        matchers.put("digit", CharMatcher.Basic.DIGIT);
        matchers.put("whitespace", CharMatcher.Basic.WHITESPACE);
        matchers.put("punctuation", CharMatcher.Basic.PUNCTUATION);
        matchers.put("symbol", CharMatcher.Basic.SYMBOL);
        // Populate with unicode categories from java.lang.Character
        for (Field field : Character.class.getFields()) {
            if (field.getName().startsWith("DIRECTIONALITY") == false
                && Modifier.isPublic(field.getModifiers())
                && Modifier.isStatic(field.getModifiers())
                && field.getType() == byte.class) {
                try {
                    matchers.put(field.getName().toLowerCase(Locale.ROOT), CharMatcher.ByUnicodeCategory.of(field.getByte(null)));
                } catch (Exception e) {
                    // just ignore
                    continue;
                }
            }
        }
        MATCHERS = unmodifiableMap(matchers);
    }

    static CharMatcher parseTokenChars(Settings settings) {
        List<String> characterClasses = settings.getAsList("token_chars");
        if (characterClasses == null || characterClasses.isEmpty()) {
            return null;
        }
        CharMatcher.Builder builder = new CharMatcher.Builder();
        for (String characterClass : characterClasses) {
            characterClass = characterClass.toLowerCase(Locale.ROOT).trim();
            CharMatcher matcher = MATCHERS.get(characterClass);
            if (matcher == null) {
                if (characterClass.equals("custom") == false) {
                    throw new IllegalArgumentException(
                        "Unknown token type: '"
                            + characterClass
                            + "', must be one of "
                            + Stream.of(MATCHERS.keySet(), Collections.singleton("custom"))
                                .flatMap(x -> x.stream())
                                .collect(Collectors.toSet())
                    );
                }
                String customCharacters = settings.get("custom_token_chars");
                if (customCharacters == null) {
                    throw new IllegalArgumentException("Token type: 'custom' requires setting `custom_token_chars`");
                }
                final Set<Integer> customCharSet = customCharacters.chars().boxed().collect(Collectors.toSet());
                matcher = customCharSet::contains;
            }
            builder.or(matcher);
        }
        return builder.build();
    }

    NGramTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        int maxAllowedNgramDiff = indexSettings.getMaxNgramDiff();
        this.minGram = settings.getAsInt("min_gram", NGramTokenizer.DEFAULT_MIN_NGRAM_SIZE);
        this.maxGram = settings.getAsInt("max_gram", NGramTokenizer.DEFAULT_MAX_NGRAM_SIZE);
        int ngramDiff = maxGram - minGram;
        if (ngramDiff > maxAllowedNgramDiff) {
            throw new IllegalArgumentException(
                "The difference between max_gram and min_gram in NGram Tokenizer must be less than or equal to: ["
                    + maxAllowedNgramDiff
                    + "] but was ["
                    + ngramDiff
                    + "]. This limit can be set by changing the ["
                    + IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey()
                    + "] index level setting."
            );
        }
        this.matcher = parseTokenChars(settings);
    }

    @Override
    public Tokenizer create() {
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
    }

}
