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
        // Keys on the bound NamedAnalyzer, whose equals()/hashCode() compare only the analyzer name.
        // For prebuilt analyzers that is exactly the intended grain: the name is the node-global
        // registration key (PreBuiltAnalyzerProviderFactory registers one provider per name), so the
        // name uniquely identifies the analyzer's behavior — two providers that share a name are the
        // same prebuilt analyzer, never two different ones.
        //
        // This deliberately collapses the per-IndexVersion instances PreBuiltAnalyzers caches into one
        // shared cache slot. That is correct because the server prebuilt analyzers are version-invariant
        // by construction (PreBuiltAnalyzers#create ignores the version), so sharing them across
        // mixed-version indices saves the per-analyzer thread-local cost with no behavioral change — see
        // AnalysisRegistryTests#testVersionInvariantAnalyzersShareAcrossVersions. A component whose
        // behavior DOES depend on the index version must fold that version into its own factory
        // sharingKey() (the composition-level key carries no version); FactorySharingKeyTests covers that.
        return analyzer;
    }
}
