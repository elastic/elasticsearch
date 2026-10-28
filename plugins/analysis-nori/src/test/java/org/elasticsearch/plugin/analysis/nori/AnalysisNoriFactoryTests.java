/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.nori;

import org.apache.lucene.analysis.ko.KoreanTokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalysisNoriFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisNoriFactoryTests() {
        super(new AnalysisNoriPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>(super.getTokenizers());
        tokenizers.put("korean", KoreanTokenizerFactory.class);
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("koreanpartofspeechstop", NoriPartOfSpeechStopFilterFactory.class);
        filters.put("koreanreadingform", NoriReadingFormFilterFactory.class);
        filters.put("koreannumber", NoriNumberFilterFactory.class);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getCharFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getCharFilters());
        // TODO: LUCENE11 (https://github.com/elastic/elasticsearch/issues/163775):
        // expose HangulCompositionCharFilter as an ES char_filter.
        filters.put("hangulcomposition", Void.class);
        return filters;
    }

    @Override
    protected Map<String, FactorySettings> tokenFilterSettings() {
        return Map.of(
            "nori_part_of_speech",
            settings(Map.of("stoptags", List.of("NNG", "NNP"))).affects("stoptags", List.of("NNG"))
                .ignored("stoptags", List.of("NNP", "NNG")),
            "nori_readingform",
            alwaysShares(),
            "nori_number",
            alwaysShares()
        );
    }

    // Base rules so lenient has duplicates to act on; it only changes which are rejected, never the keyed rules.
    private static FactorySettings withUserDictionary() {
        return settings(Map.of("user_dictionary_rules", List.of("세종시 세종 시"))).affects("user_dictionary_rules", List.of("대한민국 대한 민국"))
            .ignored("lenient", "true", "false");
    }

    @Override
    protected Map<String, FactorySettings> tokenizerSettings() {
        return Map.of(
            "nori_tokenizer",
            withUserDictionary().affects("decompound_mode", "none", "mixed")
                .ignored("decompound_mode", "DISCARD")
                .affects("discard_punctuation", "false")
        );
    }

    @Override
    protected Map<String, FactorySettings> analyzerSettings() {
        return Map.of("nori", withUserDictionary().affects("decompound_mode", "none", "mixed").affects("stoptags", List.of("NNG")));
    }
}
