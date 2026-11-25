/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

/**
 * Builds an analyzer for normalization that lowercases terms.
 */
public class LowercaseNormalizerProvider extends AbstractIndexAnalyzerProvider<LowercaseNormalizer> {

    private final LowercaseNormalizer analyzer;

    public LowercaseNormalizerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        this.analyzer = new LowercaseNormalizer();
    }

    @Override
    public LowercaseNormalizer get() {
        return analyzer;
    }

    /**
     * Returns a new normalizer that lowercases the output of {@code tokenizerFactory}. Always a fresh instance, so the
     * keyword and whitespace variants of one index never share (and so never close) the same analyzer.
     */
    public LowercaseNormalizer build(TokenizerFactory tokenizerFactory) {
        return new LowercaseNormalizer(tokenizerFactory::create);
    }

    @Override
    public Object sharingKey() {
        // Stateless normalizer: every instance is interchangeable.
        return SHARING_KEY;
    }

    private static final Object SHARING_KEY = new Object();
}
