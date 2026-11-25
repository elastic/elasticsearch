/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A factory for a conditional token filter that only applies child filters if the underlying token
 * matches an {@link AnalysisPredicateScript}
 */
public class ScriptedConditionTokenFilterFactory extends AbstractTokenFilterFactory {

    private final AnalysisPredicateScript.Factory factory;
    private final List<String> filterNames;
    // Older indices keep treating the wrapped filters as usable at index time, so their mappings still validate.
    private final boolean inheritAnalysisMode;

    ScriptedConditionTokenFilterFactory(IndexSettings indexSettings, String name, Settings settings, ScriptService scriptService) {
        super(name);
        this.inheritAnalysisMode = indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.CONDITION_FILTER_INHERITS_ANALYSIS_MODE);

        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        if (script.getType() != ScriptType.INLINE) {
            throw new IllegalArgumentException("Cannot use stored scripts in tokenfilter [" + name + "]");
        }
        this.factory = scriptService.compile(script, AnalysisPredicateScript.CONTEXT);

        this.filterNames = settings.getAsList("filter");
        if (this.filterNames.isEmpty()) {
            throw new IllegalArgumentException("Empty list of filters provided to tokenfilter [" + name + "]");
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new UnsupportedOperationException("getChainAwareTokenFilterFactory should be called first");
    }

    @Override
    public Object sharingKey() {
        // Identity: the filters named in [filter] are resolved against the index's own filter map and need
        // not appear in the analyzer's chain, so the analyzer key does not cover them. Keying on the names
        // would let two indices whose same-named filters differ share one analyzer.
        return this;
    }

    @Override
    public TokenFilterFactory getChainAwareTokenFilterFactory(
        IndexCreationContext context,
        TokenizerFactory tokenizer,
        List<CharFilterFactory> charFilters,
        List<TokenFilterFactory> previousTokenFilters,
        Function<String, TokenFilterFactory> allFilters
    ) {
        // Guard against a filter referring to itself (directly or via a cycle), which would otherwise
        // recurse until the stack overflows and takes the node down.
        allFilters = ReferringFilterResolver.enter(name(), allFilters);
        List<TokenFilterFactory> filters = new ArrayList<>();
        List<TokenFilterFactory> existingChain = new ArrayList<>(previousTokenFilters);
        boolean degraded = false;
        AnalysisMode mode = AnalysisMode.ALL;
        Set<String> resourceNames = new HashSet<>();
        for (String filter : filterNames) {
            TokenFilterFactory tff = allFilters.apply(filter);
            if (tff == null) {
                throw new IllegalArgumentException(
                    "ScriptedConditionTokenFilter [" + name() + "] refers to undefined token filter [" + filter + "]"
                );
            }
            tff = tff.getChainAwareTokenFilterFactory(context, tokenizer, charFilters, existingChain, allFilters);
            filters.add(tff);
            existingChain.add(tff);
            degraded |= tff.isDegraded();
            mode = mode.merge(tff.getAnalysisMode());
            resourceNames.addAll(tff.getResourceNames());
        }
        final boolean anyDegraded = degraded;
        final AnalysisMode analysisMode = inheritAnalysisMode ? mode : AnalysisMode.ALL;
        final Set<String> resources = inheritAnalysisMode ? Set.copyOf(resourceNames) : Set.of();

        return new TokenFilterFactory() {
            @Override
            public String name() {
                return ScriptedConditionTokenFilterFactory.this.name();
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                Function<TokenStream, TokenStream> filter = in -> {
                    for (TokenFilterFactory tff : filters) {
                        in = tff.create(in);
                    }
                    return in;
                };
                return new ScriptedConditionTokenFilter(tokenStream, filter, factory.newInstance());
            }

            @Override
            public boolean isDegraded() {
                return anyDegraded;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }

            @Override
            public Set<String> getResourceNames() {
                return resources;
            }
        };
    }

    private static class ScriptedConditionTokenFilter extends ConditionalTokenFilter {

        private final AnalysisPredicateScript script;
        private final AnalysisPredicateScript.Token token;

        ScriptedConditionTokenFilter(TokenStream input, Function<TokenStream, TokenStream> inputFactory, AnalysisPredicateScript script) {
            super(input, inputFactory);
            this.script = script;
            this.token = new AnalysisPredicateScript.Token(this);
        }

        @Override
        protected boolean shouldFilter() {
            token.updatePosition();
            return script.execute(token);
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            token.reset();
        }
    }

}
