/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        super(indexSettings, name, settings);
        this.analyzer = new LowercaseNormalizer();
    }

    @Override
    public LowercaseNormalizer get() {
        return analyzer;
    }
}
