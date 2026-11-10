/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;

import java.util.HashMap;
import java.util.Map;

public class AnalysisICUFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisICUFactoryTests() {
        super(new AnalysisICUPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>(super.getTokenizers());
        tokenizers.put("icu", IcuTokenizerFactory.class);
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("icufolding", IcuFoldingTokenFilterFactory.class);
        filters.put("icunormalizer2", IcuNormalizerTokenFilterFactory.class);
        filters.put("icutransform", IcuTransformTokenFilterFactory.class);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getCharFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getCharFilters());
        filters.put("icunormalizer2", IcuNormalizerCharFilterFactory.class);
        return filters;
    }

    private static final String UNICODE_SET_FILTER = "[^åäöÅÄÖ]";

    @Override
    protected Map<String, FactorySettings> tokenFilterSettings() {
        return Map.of(
            "icu_normalizer",
            settings().affects("name", "nfc", "nfkc").affects("unicode_set_filter", UNICODE_SET_FILTER),
            "icu_folding",
            settings().affects("unicode_set_filter", UNICODE_SET_FILTER),
            // No rules file resolves in the contract's config directory, so rules are parsed inline. Country is only read with a
            // language and variant only with a country, so the base has both.
            "icu_collation",
            settings(Map.of("language", "de", "country", "DE")).affects("rules", "&a < b")
                .affects("language", "fr")
                .affects("country", "CH")
                .affects("variant", "PHONEBOOK")
                .affects("strength", "primary")
                .affects("decomposition", "canonical")
                .affects("alternate", "shifted")
                .affects("caseLevel", "true")
                .affects("caseFirst", "upper")
                .affects("numeric", "true")
                .affects("variableTop", " ")
                .affects("hiraganaQuaternaryMode", "true"),
            "icu_transform",
            settings().affects("id", "Any-Latin").affects("ruleset", "a > b;").affects("dir", "reverse")
        );
    }

    @Override
    protected Map<String, FactorySettings> tokenizerSettings() {
        // rule_files are read from the config directory, which the contract does not have; IcuAnalyzerSharingTests covers them.
        return Map.of("icu_tokenizer", settings());
    }

    @Override
    protected Map<String, FactorySettings> charFilterSettings() {
        // An unrecognized mode falls back to compose.
        return Map.of(
            "icu_normalizer",
            settings().affects("name", "nfc")
                .affects("mode", "decompose")
                .ignored("mode", "compose", "unknown")
                .affects("unicode_set_filter", UNICODE_SET_FILTER)
        );
    }

    @Override
    protected Map<String, FactorySettings> analyzerSettings() {
        return Map.of(
            "icu_analyzer",
            settings().affects("method", "nfc").affects("mode", "decompose").affects("unicode_set_filter", UNICODE_SET_FILTER)
        );
    }
}
