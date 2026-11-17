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
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.MinHashIndexParamBounds;

import java.util.HashMap;
import java.util.Map;

/**
 * TokenFilterFactoryAdapter for {@link MinHashFilterFactory}
 *
 */
public class MinHashTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MinHashTokenFilterFactory.class);

    /**
     * Hard upper bounds for the {@code hash_count}, {@code bucket_count}, and {@code hash_set_size}
     * parameters of filters defined inline in a request (e.g. {@code _analyze}). No legitimate MinHash
     * configuration needs values anywhere near these limits (typical values are in the single digits to
     * low hundreds). The caps prevent integer overflow in the product check and bound the memory an
     * {@code _analyze} request can use.
     */
    static final int MAX_HASH_COUNT = 10_000;
    static final int MAX_BUCKET_COUNT = 10_000;
    static final int MAX_HASH_SET_SIZE = 10_000;

    private final MinHashFilterFactory minHashFilterFactory;
    private final Object sharingKey;

    MinHashTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        int hashCount = settings.getAsInt("hash_count", MinHashFilter.DEFAULT_HASH_COUNT);
        int bucketCount = settings.getAsInt("bucket_count", MinHashFilter.DEFAULT_BUCKET_COUNT);
        int hashSetSize = settings.getAsInt("hash_set_size", MinHashFilter.DEFAULT_HASH_SET_SIZE);
        if (name.startsWith(AnalysisRegistry.ANONYMOUS_COMPONENT_PREFIX)) {
            validateInlineDefinition(indexSettings, settings, hashCount, bucketCount, hashSetSize);
        } else if (indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS)) {
            String violation = MinHashIndexParamBounds.violation(hashCount, bucketCount, hashSetSize);
            if (violation != null) {
                throw new IllegalArgumentException(violation);
            }
        } else {
            // Indices created before the bounds were introduced keep working, but the same configuration
            // can't be used for a new index (e.g. a reindex destination), so warn about it.
            String violation = MinHashIndexParamBounds.violation(hashCount, bucketCount, hashSetSize);
            if (violation != null) {
                deprecationLogger.warn(
                    DeprecationCategory.ANALYSIS,
                    // Include the index and filter in the key so that the rate limiter doesn't hide the warning for other indices.
                    "min_hash_index_param_bounds_" + indexSettings.getIndex().getName() + "_" + name,
                    "[min_hash] filter [{}] on index [{}]: {} This configuration is allowed on this index, but can't be used "
                        + "on new indices. Lower these parameters in the settings of any new index, for example a reindex "
                        + "destination; note that this changes the hashes the filter produces.",
                    name,
                    indexSettings.getIndex().getName(),
                    violation
                );
            }
        }
        minHashFilterFactory = new MinHashFilterFactory(convertSettings(settings));
        // MinHashFilterFactory hides its parsed settings, so derive them here the way it does: with_rotation
        // is parsed leniently (anything but a case-insensitive "true" is false) and defaults to whether there
        // is more than one bucket.
        boolean withRotation = settings.hasValue("with_rotation")
            ? "true".equalsIgnoreCase(settings.get("with_rotation"))
            : bucketCount > 1;
        this.sharingKey = new Key(hashCount, bucketCount, hashSetSize, withRotation);
    }

    /**
     * Checks a filter defined inline in a request, such as {@code _analyze}. These definitions are built per
     * request, so they are bounded by the request's {@code index.analyze.max_token_count}.
     */
    private static void validateInlineDefinition(
        IndexSettings indexSettings,
        Settings settings,
        int hashCount,
        int bucketCount,
        int hashSetSize
    ) {
        validateParamBound("hash_count", hashCount, MAX_HASH_COUNT, settings);
        validateParamBound("bucket_count", bucketCount, MAX_BUCKET_COUNT, settings);
        validateParamBound("hash_set_size", hashSetSize, MAX_HASH_SET_SIZE, settings);
        // Only validate when the user explicitly configured parameters, so that a low max_token_count
        // doesn't reject a min_hash filter that uses the defaults.
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
    }

    private static void validateParamBound(String paramName, int value, int maxValue, Settings settings) {
        if (settings.hasValue(paramName) && value > maxValue) {
            throw new IllegalArgumentException("[" + paramName + "] must not exceed [" + maxValue + "] but was [" + value + "]");
        }
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

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(int hashCount, int bucketCount, int hashSetSize, boolean withRotation) {}
}
