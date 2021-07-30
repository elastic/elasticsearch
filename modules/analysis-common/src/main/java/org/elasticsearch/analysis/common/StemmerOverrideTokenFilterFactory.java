/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter.StemmerOverrideMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;

import java.io.IOException;
import java.util.List;

public class StemmerOverrideTokenFilterFactory extends AbstractTokenFilterFactory {

    private final StemmerOverrideMap overrideMap;

    StemmerOverrideTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);

        List<String> rules = Analysis.getWordList(env, settings, "rules");
        if (rules == null) {
            throw new IllegalArgumentException("stemmer override filter requires either `rules` or `rules_path` to be configured");
        }

        StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(false);
        parseRules(rules, builder, "=>");
        overrideMap = builder.build();

    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new StemmerOverrideFilter(tokenStream, overrideMap);
    }

    static void parseRules(List<String> rules, StemmerOverrideFilter.Builder builder, String mappingSep) {
        for (String rule : rules) {
            String[] sides = rule.split(mappingSep, -1);
            if (sides.length != 2) {
                throw new RuntimeException("Invalid Keyword override Rule:" + rule);
            }

            String[] keys = sides[0].split(",", -1);
            String override = sides[1].trim();
            if (override.isEmpty() || override.indexOf(',') != -1) {
                throw new RuntimeException("Invalid Keyword override Rule:" + rule);
            }

            for (String key : keys) {
                String trimmedKey = key.trim();
                if (trimmedKey.isEmpty()) {
                    throw new RuntimeException("Invalid Keyword override Rule:" + rule);
                }
                builder.add(trimmedKey, override);
            }
        }
    }

}
