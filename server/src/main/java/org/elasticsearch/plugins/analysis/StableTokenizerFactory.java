/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;
import org.elasticsearch.index.analysis.PluginIteratorStream;

public class StableTokenizerFactory extends AbstractTokenizerFactory {
    private final AnalysisIteratorFactory factory;

    public StableTokenizerFactory(
        IndexSettings indexSettings,
        Environment environment,
        String name,
        Settings settings,
        AnalysisIteratorFactory factory
    ) {
        super(indexSettings, settings, name);
        this.factory = factory;
    }

    @Override
    public Tokenizer create() {
        PluginIteratorStream tokenizer = new PluginIteratorStream();
        PortableAnalyzeIterator iterator = factory.newInstance(tokenizer);
        tokenizer.setIterator(iterator);

        return tokenizer;
    }
}
