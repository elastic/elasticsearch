/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.PluginIteratorTokenizer;

public class StableTokenFilterFactory extends AbstractTokenFilterFactory {
    private final AnalysisIteratorFactory factory;

    public StableTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings, AnalysisIteratorFactory factory) {
        super(indexSettings, name, settings);
        this.factory = factory;
    }

    @Override
    public TokenStream create(TokenStream input) {
        PortableAnalyzeIterator iterator = factory.newInstance(new ESTokenStream(input));
        return new PluginIteratorTokenizer(iterator);
    }
}
