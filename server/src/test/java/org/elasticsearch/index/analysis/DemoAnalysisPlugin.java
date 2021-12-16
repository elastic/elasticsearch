/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.classic.ClassicTokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.email.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.analysis.AnalysisIteratorFactory;

import java.util.List;
import java.util.Map;

public class DemoAnalysisPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return Map.of(
            "demo_legacy", DemoTokenFilterFactory::new,
            "demo_legacy_normalizer", DemoNormalizerFactory::new
        );
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<AnalysisIteratorFactory>> getIterators() {
        return Map.of(
            "demo", DemoIteratorFactory::new,
            "demo_normalizer", DemoNormalizerIteratorFactory::new
        );
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<AnalysisIteratorFactory>> getTokenizerIterators() {
        return Map.of(
            "demo_tokenizer", DemoTokenizerIteratorFactory::new
        );
    }

    @Override
    public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
        return List.of(PreConfiguredTokenFilter.singleton("uppercase", true, UpperCaseFilter::new));
    }

    @Override
    public List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
        return List.of(
            PreConfiguredTokenizer.singleton("keyword", KeywordTokenizer::new),
            PreConfiguredTokenizer.singleton("classic", ClassicTokenizer::new),
            PreConfiguredTokenizer.singleton("uax_url_email", UAX29URLEmailTokenizer::new),
            PreConfiguredTokenizer.singleton("path_hierarchy", PathHierarchyTokenizer::new),
            PreConfiguredTokenizer.singleton("letter", LetterTokenizer::new),
            PreConfiguredTokenizer.singleton("whitespace", WhitespaceTokenizer::new)
        );
    }
}
