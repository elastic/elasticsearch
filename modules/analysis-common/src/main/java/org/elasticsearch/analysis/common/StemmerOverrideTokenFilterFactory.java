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
