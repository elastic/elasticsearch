/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.ja.JapaneseTokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;

public class AnalysisKuromojiFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisKuromojiFactoryTests() {
        super(new AnalysisKuromojiPlugin());
    }

    @Override
    protected Map<String, FactorySettings> charFilterSettings() {
        return Map.of("kuromoji_iteration_mark", settings().affects("normalize_kanji", "false").affects("normalize_kana", "false"));
    }

    @Override
    protected Map<String, FactorySettings> tokenizerSettings() {
        return Map.of(
            "kuromoji_tokenizer",
            // default mode is SEARCH, so vary to a clearly different mode
            settings().affects("mode", "extended").affects("discard_punctuation", "false").affects("discard_compound_token", "true")
        );
    }

    @Override
    protected Map<String, FactorySettings> tokenFilterSettings() {
        return Map.ofEntries(
            entry("kuromoji_baseform", stateless()),
            entry("kuromoji_number", stateless()),
            entry("hiragana_uppercase", stateless()),
            entry("katakana_uppercase", stateless()),
            entry("kuromoji_part_of_speech", settings().affects("stoptags", List.of("助詞"))),
            entry("kuromoji_readingform", settings().affects("use_romaji", "true")),
            entry("kuromoji_stemmer", settings().affects("minimum_length", "3")),
            entry(
                "ja_stop",
                settings().affects("stopwords", List.of("の")).affects("ignore_case", "true").affects("remove_trailing", "false")
            ),
            entry("kuromoji_completion", settings().affects("mode", "query"))
        );
    }

    @Override
    protected Map<String, FactorySettings> analyzerSettings() {
        return Map.of("kuromoji", identity(), "kuromoji_completion", identity());
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>(super.getTokenizers());
        tokenizers.put("japanese", JapaneseTokenizerFactory.class);
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("japanesebaseform", KuromojiBaseFormFilterFactory.class);
        filters.put("japanesepartofspeechstop", KuromojiPartOfSpeechFilterFactory.class);
        filters.put("japanesereadingform", KuromojiReadingFormFilterFactory.class);
        filters.put("japanesekatakanastem", KuromojiKatakanaStemmerFactory.class);
        filters.put("japanesenumber", KuromojiNumberFilterFactory.class);
        filters.put("japanesehiraganauppercase", HiraganaUppercaseFilterFactory.class);
        filters.put("japanesekatakanauppercase", KatakanaUppercaseFilterFactory.class);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getCharFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getCharFilters());
        filters.put("japaneseiterationmark", KuromojiIterationMarkCharFilterFactory.class);
        return filters;
    }
}
