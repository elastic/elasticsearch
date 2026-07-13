/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;

public class SimpleAnalyzerProvider extends AbstractIndexAnalyzerProvider<SimpleAnalyzer> {

    private final SimpleAnalyzer simpleAnalyzer;

    public SimpleAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        this.simpleAnalyzer = new SimpleAnalyzer();
    }

    @Override
    public SimpleAnalyzer get() {
        return this.simpleAnalyzer;
    }
}
