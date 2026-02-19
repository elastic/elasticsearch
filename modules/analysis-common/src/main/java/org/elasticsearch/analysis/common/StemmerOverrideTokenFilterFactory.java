/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter.StemmerOverrideMap;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.analysis.common.async.AsyncInitStemmerOverrideFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.dictionary.CustomDictionaryService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public class StemmerOverrideTokenFilterFactory extends AbstractTokenFilterFactory {
    private static final String RULES_KEY = "rules";
    private static final String RULES_PATH_KEY = RULES_KEY + "_path";
    private static final String RULES_DICTIONARY_KEY = RULES_KEY + "_dictionary";

    private final PlainActionFuture<StemmerOverrideMap> overrideMapFuture;

    StemmerOverrideTokenFilterFactory(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings,
        ThreadPool threadPool,
        CustomDictionaryService customDictionaryService
    ) {
        super(name);
        validateSettings(settings);

        this.overrideMapFuture = new PlainActionFuture<>();
        final Supplier<List<String>> rulesSupplier = Analysis.getWordListSupplier(
            customDictionaryService,
            env,
            settings,
            RULES_PATH_KEY,
            RULES_KEY,
            RULES_DICTIONARY_KEY,
            true
        );

        threadPool.executor(ThreadPool.Names.ANALYZE)
            .execute(ActionRunnable.supply(overrideMapFuture, () -> buildStemmerOverrideMap(rulesSupplier.get())));
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new AsyncInitStemmerOverrideFilter(tokenStream, overrideMapFuture, StemmerOverrideFilter::new);
    }

    private static void validateSettings(Settings settings) {
        int sourceCount = 0;
        for (String sourceKey : List.of(RULES_KEY, RULES_PATH_KEY, RULES_DICTIONARY_KEY)) {
            if (settings.hasValue(sourceKey)) {
                sourceCount++;
            }
        }

        if (sourceCount != 1) {
            throw new IllegalArgumentException(
                "stemmer_override requires one of ["
                    + RULES_KEY
                    + "], ["
                    + RULES_PATH_KEY
                    + "], or ["
                    + RULES_DICTIONARY_KEY
                    + "] to be configured"
            );
        }
    }

    private static StemmerOverrideMap buildStemmerOverrideMap(List<String> rules) throws IOException {
        StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(false);
        parseRules(rules, builder, "=>");
        return builder.build();
    }

    static void parseRules(List<String> rules, StemmerOverrideFilter.Builder builder, String mappingSep) {
        for (String rule : rules) {
            String[] sides = rule.split(mappingSep, -1);
            if (sides.length != 2) {
                throw new IllegalArgumentException("Invalid Keyword override Rule:" + rule);
            }

            String[] keys = sides[0].split(",", -1);
            String override = sides[1].trim();
            if (override.isEmpty() || override.indexOf(',') != -1) {
                throw new IllegalArgumentException("Invalid Keyword override Rule:" + rule);
            }

            for (String key : keys) {
                String trimmedKey = key.trim();
                if (trimmedKey.isEmpty()) {
                    throw new IllegalArgumentException("Invalid Keyword override Rule:" + rule);
                }
                builder.add(trimmedKey, override);
            }
        }
    }

}
