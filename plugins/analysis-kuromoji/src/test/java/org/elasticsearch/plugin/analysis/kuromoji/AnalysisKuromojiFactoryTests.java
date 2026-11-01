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

public class AnalysisKuromojiFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisKuromojiFactoryTests() {
        super(new AnalysisKuromojiPlugin());
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

    private static final List<String> USER_DICTIONARY_RULES = List.of("東京スカイツリー,東京 スカイツリー,トウキョウ スカイツリー,カスタム名詞");
    private static final List<String> OTHER_USER_DICTIONARY_RULES = List.of("関西国際空港,関西 国際 空港,カンサイ コクサイ クウコウ,カスタム名詞");

    // Base rules so lenient has duplicates to act on; it only changes which are rejected, never the keyed rules.
    private static FactorySettings withUserDictionary() {
        return settings(Map.of("user_dictionary_rules", USER_DICTIONARY_RULES)).affects(
            "user_dictionary_rules",
            OTHER_USER_DICTIONARY_RULES
        ).ignored("lenient", "true", "false");
    }

    @Override
    protected Map<String, FactorySettings> tokenFilterSettings() {
        Map<String, FactorySettings> filters = new HashMap<>();
        for (String stateless : List.of("kuromoji_baseform", "kuromoji_number", "hiragana_uppercase", "katakana_uppercase")) {
            filters.put(stateless, alwaysShares());
        }
        filters.put(
            "kuromoji_part_of_speech",
            settings(Map.of("stoptags", List.of("助詞-格助詞-一般", "助詞-終助詞"))).affects("stoptags", List.of("助詞-終助詞"))
                .ignored("stoptags", List.of("助詞-終助詞", "助詞-格助詞-一般"))
        );
        filters.put("kuromoji_readingform", settings().affects("use_romaji", "true"));
        filters.put("kuromoji_stemmer", settings().affects("minimum_length", "5"));
        filters.put(
            "ja_stop",
            settings().affects("stopwords", List.of("foo")).affects("ignore_case", "true").affects("remove_trailing", "false")
        );
        // An unrecognized mode falls back to index.
        filters.put("kuromoji_completion", settings().affects("mode", "query").ignored("mode", "index", "unknown"));
        return filters;
    }

    @Override
    protected Map<String, FactorySettings> tokenizerSettings() {
        return Map.of(
            "kuromoji_tokenizer",
            withUserDictionary().affects("mode", "normal", "extended")
                .ignored("mode", "search")
                .affects("discard_punctuation", "false")
                .affects("discard_compound_token", "true")
                .affects("nbest_cost", "1000")
                .affects("nbest_examples", "/箱根山-箱根/成田空港-成田/")
        );
    }

    @Override
    protected Map<String, FactorySettings> charFilterSettings() {
        return Map.of("kuromoji_iteration_mark", settings().affects("normalize_kanji", "false").affects("normalize_kana", "false"));
    }

    @Override
    protected Map<String, FactorySettings> analyzerSettings() {
        return Map.of(
            "kuromoji",
            withUserDictionary().affects("mode", "normal", "extended")
                .affects("stopwords", List.of("foo"))
                .affects("stopwords_case", "true"),
            "kuromoji_completion",
            withUserDictionary().affects("mode", "query")
        );
    }
}
