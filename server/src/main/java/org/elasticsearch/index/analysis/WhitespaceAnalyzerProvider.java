/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

public class WhitespaceAnalyzerProvider extends AbstractIndexAnalyzerProvider<WhitespaceAnalyzer> {

    private final WhitespaceAnalyzer analyzer;

    public WhitespaceAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.analyzer = new WhitespaceAnalyzer();
    }

    @Override
    public WhitespaceAnalyzer get() {
        return this.analyzer;
    }
}
