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
import org.apache.lucene.analysis.minhash.MinHashFilter;
import org.apache.lucene.analysis.minhash.MinHashFilterFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * TokenFilterFactoryAdapter for {@link MinHashFilterFactory}
 *
 */
public class MinHashTokenFilterFactory extends AbstractTokenFilterFactory {

    private final MinHashFilterFactory minHashFilterFactory;

    MinHashTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        int hashCount = settings.getAsInt("hash_count", MinHashFilter.DEFAULT_HASH_COUNT);
        int bucketCount = settings.getAsInt("bucket_count", MinHashFilter.DEFAULT_BUCKET_COUNT);
        int hashSetSize = settings.getAsInt("hash_set_size", MinHashFilter.DEFAULT_HASH_SET_SIZE);
        // Only validate when the user explicitly configured parameters; eager instantiation with
        // default settings (empty Settings) is skipped so that an unrelated low max_token_count
        // on an index that doesn't use min_hash does not cause index creation to fail.
        if (settings.hasValue("hash_count") || settings.hasValue("bucket_count") || settings.hasValue("hash_set_size")) {
            long maxTokenCount = indexSettings.getMaxTokenCount();
            long maxOutputTokens = (long) hashCount * bucketCount * hashSetSize;
            if (maxOutputTokens > maxTokenCount) {
                throw new IllegalArgumentException(
                    "The product of hash_count ["
                        + hashCount
                        + "], bucket_count ["
                        + bucketCount
                        + "], and hash_set_size ["
                        + hashSetSize
                        + "] is ["
                        + maxOutputTokens
                        + "], which exceeds the maximum token count limit ["
                        + maxTokenCount
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_TOKEN_COUNT_SETTING.getKey()
                        + "] index level setting."
                );
            }
        }
        minHashFilterFactory = new MinHashFilterFactory(convertSettings(settings));
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return minHashFilterFactory.create(tokenStream);
    }

    private static Map<String, String> convertSettings(Settings settings) {
        Map<String, String> settingMap = new HashMap<>();
        if (settings.hasValue("hash_count")) {
            settingMap.put("hashCount", settings.get("hash_count"));
        }
        if (settings.hasValue("bucket_count")) {
            settingMap.put("bucketCount", settings.get("bucket_count"));
        }
        if (settings.hasValue("hash_set_size")) {
            settingMap.put("hashSetSize", settings.get("hash_set_size"));
        }
        if (settings.hasValue("with_rotation")) {
            settingMap.put("withRotation", settings.get("with_rotation"));
        }
        return settingMap;
    }
}
