/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.settings.Settings;

public abstract class AbstractIndexAnalyzerProvider<T extends Analyzer> implements AnalyzerProvider<T> {

    private final String name;

    /**
     * Constructs a new analyzer component, with the index name and its settings and the analyzer name.
     *
     * @param name          The analyzer name
     */
    public AbstractIndexAnalyzerProvider(String name, Settings settings) {
        this.name = name;
        Analysis.checkForDeprecatedVersion(name, settings);
    }

    /**
     * Returns the injected name of the analyzer.
     */
    @Override
    public final String name() {
        return this.name;
    }

    @Override
    public final AnalyzerScope scope() {
        return AnalyzerScope.INDEX;
    }
}
