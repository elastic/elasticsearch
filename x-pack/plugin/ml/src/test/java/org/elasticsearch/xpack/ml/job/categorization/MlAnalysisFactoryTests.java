/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.categorization;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MlAnalysisFactoryTests extends AnalysisFactoryTestCase {
    public MlAnalysisFactoryTests() {
        super(new MachineLearning(Settings.EMPTY));
    }

    // ML depends on the Lucene ICU and Japanese analysis modules, whose SPI factories ML does not expose.

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        for (String notExposed : List.of(
            "icufolding",
            "icunormalizer2",
            "icutransform",
            "japanesebaseform",
            "japanesehiraganauppercase",
            "japanesekatakanastem",
            "japanesekatakanauppercase",
            "japanesenumber",
            "japanesepartofspeechstop",
            "japanesereadingform"
        )) {
            filters.put(notExposed, Void.class);
        }
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>(super.getTokenizers());
        tokenizers.put("icu", Void.class);
        tokenizers.put("japanese", Void.class);
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getCharFilters() {
        Map<String, Class<?>> charFilters = new HashMap<>(super.getCharFilters());
        charFilters.put("icunormalizer2", Void.class);
        charFilters.put("japaneseiterationmark", Void.class);
        return charFilters;
    }

    @Override
    protected Map<String, FactorySettings> charFilterSettings() {
        return Map.of(FirstNonBlankLineCharFilter.NAME, alwaysShares(), FirstLineWithLettersCharFilter.NAME, alwaysShares());
    }

    @Override
    protected Map<String, FactorySettings> tokenizerSettings() {
        return Map.of(MlClassicTokenizer.NAME, alwaysShares(), MlStandardTokenizer.NAME, alwaysShares());
    }
}
