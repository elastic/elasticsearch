/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.mapper.TextFieldMapper;

public class PreBuiltAnalyzerProvider implements AnalyzerProvider<NamedAnalyzer> {

    private final NamedAnalyzer analyzer;

    public PreBuiltAnalyzerProvider(String name, AnalyzerScope scope, Analyzer analyzer) {
        // we create the named analyzer here so the resources associated with it will be shared
        // and we won't wrap a shared analyzer with named analyzer each time causing the resources
        // to not be shared...
        this.analyzer = new NamedAnalyzer(name, scope, analyzer, TextFieldMapper.Defaults.POSITION_INCREMENT_GAP);
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

    @Override
    public Object sharingKey() {
        // Keys on the bound NamedAnalyzer, whose equals()/hashCode() compare only the analyzer name. For
        // prebuilt analyzers that is the intended grain: the name is the node-global registration key (one
        // provider per name), so it uniquely identifies the analyzer. This collapses the per-IndexVersion
        // instances PreBuiltAnalyzers caches into one slot, which is safe because the server prebuilt
        // analyzers are version-invariant (PreBuiltAnalyzers#create ignores the version); a
        // version-sensitive component folds the version into its own factory sharingKey() instead. See
        // AnalysisRegistryTests#testVersionInvariantAnalyzersShareAcrossVersions.
        return analyzer;
    }
}
