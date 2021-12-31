/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;

public class StableNormalizerFactory extends StableTokenFilterFactory implements NormalizingTokenFilterFactory {
    public StableNormalizerFactory(
        IndexSettings indexSettings,
        Environment environment,
        String name,
        Settings settings,
        AnalysisIteratorFactory factory
    ) {
        super(indexSettings, environment, name, settings, factory);
    }
}
