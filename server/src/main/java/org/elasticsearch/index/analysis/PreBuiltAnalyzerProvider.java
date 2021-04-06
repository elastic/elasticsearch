/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;

public class PreBuiltAnalyzerProvider implements AnalyzerProvider<NamedAnalyzer> {

    private final NamedAnalyzer analyzer;

    public PreBuiltAnalyzerProvider(String name, AnalyzerScope scope, Analyzer analyzer) {
        // we create the named analyzer here so the resources associated with it will be shared
        // and we won't wrap a shared analyzer with named analyzer each time causing the resources
        // to not be shared...
        this.analyzer = new NamedAnalyzer(name, scope, analyzer);
    }

    @Override
    public String name() {
        return analyzer.name();
    }

    @Override
    public AnalyzerScope scope() {
        return analyzer.scope();
    }

    @Override
    public NamedAnalyzer get() {
        return analyzer;
    }
}
